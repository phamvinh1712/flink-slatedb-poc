package io.slatedb.flink.poc;

import io.slatedb.uniffi.*;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * RESCALE TEST — exercises the §6.4 one-DB-per-subtask rescale MERGE algorithm with REAL SlateDB:
 * clone-with-projection (base adoption + logical clip) + scan-copy (slice migration from other sources).
 *
 * This directly runs the code an operator's initializeState would run on a parallelism change. The list of
 * (dbName, checkpointId, keyGroupRange) "handles" is exactly what Flink's UNION list state carries across a
 * rescale (§6.3/§6.4); we build them here rather than driving a full Flink savepoint/rescale (which tests
 * Flink's savepoint machinery, not our merge). Requires JDK 22+ (SlateDB FFM).
 *
 * §6.4 algorithm, per new subtask owning key-group range R:
 *   sources = old handles whose range intersects R
 *   base    = source with the largest overlap with R
 *   clone base@cp → newDb WITH projectionRange = R  (adopt base AND clip to R in one step — §14.2)
 *   for each other source s: scan s@cp over (s.range ∩ R), put into newDb   (scan-copy slice)
 *   ⇒ newDb = (⋃ sources' keys) ∩ R = exactly the keys whose key group ∈ R
 * Correctness rests on: each key group is owned by exactly ONE old subtask ⇒ no dup, no loss (§6.4).
 *
 * Tests BOTH directions:
 *   DOWNSCALE 3→2 — each new range is a UNION of old ranges ⇒ multi-source scan-copy (the expensive path)
 *   UPSCALE   2→4 — each new range is a SUB-range of an old one ⇒ clone+projection CLIP (foreign-key removal)
 */
public final class SlateDbRescaleE2E {

    static final int MAXP = 128;          // key groups (immutable maxParallelism)
    static final int NUM_KEYS = 400;

    private static <T> T await(CompletableFuture<T> f) {
        try { return f.get(60, TimeUnit.SECONDS); } catch (Exception e) { throw new RuntimeException(e); }
    }
    private static byte[] bytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }
    static int kgOf(String key) { return KeyGroupRangeAssignment.assignToKeyGroup(key, MAXP); }

    /** §6.1 key: [kg byte][keybytes] — kg∈[0,127] fits one non-negative byte, so byte order == kg order. */
    static byte[] slateKey(String key) {
        byte[] k = bytes(key);
        byte[] out = new byte[1 + k.length];
        out[0] = (byte) kgOf(key);
        System.arraycopy(k, 0, out, 1, k.length);
        return out;
    }

    /** Byte range covering every key whose key group ∈ [lo,hi]. */
    static KeyRange kgRangeBytes(int lo, int hi) {
        byte[] start = new byte[]{(byte) lo};                 // {lo} ≤ {lo}{...}
        byte[] end = (hi < 127) ? new byte[]{(byte) (hi + 1)} : null;  // exclusive; null = unbounded at top
        return new KeyRange(start, true, end, false);
    }

    /** A pre-rescale subtask's state handle — exactly what Flink union list state would carry. */
    record Handle(String dbName, String cpId, int lo, int hi) {
        int overlap(int rlo, int rhi) { return Math.max(0, Math.min(hi, rhi) - Math.max(lo, rlo) + 1); }
    }

    private static boolean ok = true;
    private static void check(String name, boolean cond, String detail) {
        System.out.println((cond ? "  [PASS] " : "  [FAIL] ") + name + (detail.isEmpty() ? "" : " — " + detail));
        if (!cond) ok = false;
    }

    static ObjectStore store;
    static String workAbs;   // absolute work dir, leading '/' stripped for db-names

    public static void main(String[] args) throws Exception {
        System.out.println("=== RESCALE E2E: §6.4 clone + scan-copy merge with real SlateDB ===");
        System.out.println("Runtime Java: " + System.getProperty("java.version"));

        Path work = Files.createTempDirectory("flink-slatedb-rescale-");
        workAbs = work.toString().substring(1);   // strip leading '/'

        // Ground truth: every key and its expected value.
        Map<String, String> truth = new HashMap<>();
        for (int i = 0; i < NUM_KEYS; i++) truth.put("user-" + i, "v-" + i);

        try (ObjectStore s = ObjectStore.resolve("file:///")) {
            store = s;

            // ---- Build the P1=3 pre-rescale state (one SlateDB per old subtask) ----
            List<Handle> gen3 = buildInitial(3, truth, "g3");
            check("built P1=3 state (3 DBs, key-group partitioned)", gen3.size() == 3,
                    ranges(gen3));

            // ---- DOWNSCALE 3 → 2 (multi-source scan-copy) ----
            System.out.println("\n[DOWNSCALE 3→2] each new range = union of old ranges → multi-source scan-copy");
            List<Handle> down2 = rescale(gen3, 2, "down2");
            verifyPartition("downscale 3→2", down2, 2, truth);

            // ---- UPSCALE 2 → 4 (clone+projection clip) ----
            System.out.println("\n[UPSCALE 2→4] each new range = sub-range of an old one → clone+projection CLIP");
            // Start upscale from the downscaled P2=2 generation (chained rescale, like repeated ops).
            List<Handle> up4 = rescale(down2, 4, "up4");
            verifyPartition("upscale 2→4", up4, 4, truth);

        } catch (UnsatisfiedLinkError | NoClassDefFoundError e) {
            System.out.println("  [SKIP] SlateDB native lib/class load failed — need JDK 22+ + -Djava.library.path. " + e);
            System.exit(2);
        }

        System.out.println();
        System.out.println(ok ? "RESCALE E2E PASSED ✅ (§6.4 merge preserves exact key-group partition, no loss/dup, both directions)"
                              : "RESCALE E2E FAILED ❌");
        if (!ok) System.exit(1);
    }

    /** Create `p` DBs, route each key to its owning subtask, memtable-flush + checkpoint each. */
    static List<Handle> buildInitial(int p, Map<String, String> truth, String tag) throws Exception {
        Db[] dbs = new Db[p];
        Admin[] admins = new Admin[p];
        String[] names = new String[p];
        for (int i = 0; i < p; i++) {
            names[i] = workAbs + "/" + tag + "-sub" + i;
            dbs[i] = await(new DbBuilder(names[i], store).build());
            admins[i] = new AdminBuilder(names[i], store).build();
        }
        for (var e : truth.entrySet()) {
            int sub = KeyGroupRangeAssignment.assignKeyToParallelOperator(e.getKey(), MAXP, p);
            await(dbs[sub].put(slateKey(e.getKey()), bytes(e.getValue())));
        }
        List<Handle> handles = new ArrayList<>();
        for (int i = 0; i < p; i++) {
            await(dbs[i].flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));
            String cp = await(admins[i].createDetachedCheckpoint(new CheckpointOptions(null, null, null))).id();
            KeyGroupRange r = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(MAXP, p, i);
            handles.add(new Handle(names[i], cp, r.getStartKeyGroup(), r.getEndKeyGroup()));
            await(dbs[i].shutdown()); dbs[i].close(); admins[i].close();
        }
        return handles;
    }

    /** §6.4 merge: produce `newP` new DBs from the old handles. Returns new handles. */
    static List<Handle> rescale(List<Handle> old, int newP, String tag) throws Exception {
        List<Handle> out = new ArrayList<>();
        for (int idx = 0; idx < newP; idx++) {
            KeyGroupRange nr = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(MAXP, newP, idx);
            int lo = nr.getStartKeyGroup(), hi = nr.getEndKeyGroup();
            String newDb = workAbs + "/" + tag + "-sub" + idx;

            // sources intersecting the new range; base = largest overlap.
            List<Handle> sources = new ArrayList<>();
            for (Handle h : old) if (h.overlap(lo, hi) > 0) sources.add(h);
            Handle base = sources.stream().max(Comparator.comparingInt(h -> h.overlap(lo, hi))).orElseThrow();

            // 1. Adopt base AND clip to new range via clone projection (§14.2 / §6.4).
            try (Admin a = new AdminBuilder(base.dbName, store).build()) {
                CloneSourceSpec src = new CloneSourceSpec(base.dbName, base.cpId, kgRangeBytes(lo, hi));
                CloneBuilder cb = a.createCloneBuilderFromSource(src);
                cb.withClonePath(newDb);
                cb.withObjectStore(store);
                await(cb.build());
            }
            Db nd = await(new DbBuilder(newDb, store).build());
            Admin na = new AdminBuilder(newDb, store).build();

            // 2. Scan-copy the (source ∩ newRange) slice from every non-base source.
            int copied = 0;
            for (Handle s : sources) {
                if (s == base) continue;
                int slo = Math.max(s.lo, lo), shi = Math.min(s.hi, hi);
                try (DbReaderBuilder rb = new DbReaderBuilder(s.dbName, store)) {
                    rb.withCheckpointId(s.cpId);
                    DbReader rdr = await(rb.build());
                    DbIterator it = await(rdr.scan(kgRangeBytes(slo, shi)));
                    for (KeyValue kv = await(it.next()); kv != null; kv = await(it.next())) {
                        await(nd.put(kv.key(), kv.value()));
                        copied++;
                    }
                    it.close();
                    await(rdr.shutdown());
                }
            }
            await(nd.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));
            String cp = await(na.createDetachedCheckpoint(new CheckpointOptions(null, null, null))).id();
            out.add(new Handle(newDb, cp, lo, hi));
            System.out.println("  sub" + idx + " range=[" + lo + "," + hi + "] base=" + shortName(base.dbName)
                    + "(ovlp " + base.overlap(lo, hi) + ") +scan-copied " + copied + " from " + (sources.size() - 1) + " src");
            await(nd.shutdown()); nd.close(); na.close();
        }
        return out;
    }

    /** Assert the new generation is an exact partition of ground truth: no loss, no dup, correct ownership, values intact. */
    static void verifyPartition(String label, List<Handle> gen, int p, Map<String, String> truth) throws Exception {
        Map<String, Integer> keyToDb = new HashMap<>();   // key → which new subtask holds it
        Map<String, String> seen = new HashMap<>();
        boolean noForeign = true;
        for (int i = 0; i < gen.size(); i++) {
            Handle h = gen.get(i);
            try (DbBuilder b = new DbBuilder(h.dbName, store)) {
                Db db = await(b.build());
                try (db) {
                    DbIterator it = await(db.scan(new KeyRange(null, false, null, false)));
                    for (KeyValue kv = await(it.next()); kv != null; kv = await(it.next())) {
                        int kg = kv.key()[0] & 0xFF;
                        String key = new String(kv.key(), 1, kv.key().length - 1, StandardCharsets.UTF_8);
                        if (kg < h.lo || kg > h.hi) noForeign = false;   // clip failed → foreign key present
                        if (seen.containsKey(key)) keyToDb.put(key, -999);  // duplicate across DBs
                        seen.put(key, new String(kv.value(), StandardCharsets.UTF_8));
                        keyToDb.putIfAbsent(key, i);
                    }
                    it.close();
                    await(db.shutdown());
                }
            }
        }
        boolean noDup = keyToDb.values().stream().noneMatch(v -> v == -999);
        boolean noLoss = seen.keySet().equals(truth.keySet());
        boolean valuesIntact = truth.entrySet().stream().allMatch(e -> e.getValue().equals(seen.get(e.getKey())));
        // Ownership: each key resides in the subtask whose range owns its key group.
        boolean ownership = true;
        for (String key : truth.keySet()) {
            int expectedSub = KeyGroupRangeAssignment.assignKeyToParallelOperator(key, MAXP, p);
            if (!Integer.valueOf(expectedSub).equals(keyToDb.get(key))) ownership = false;
        }
        check(label + ": no foreign keys (clip works, §6.4)", noForeign, "");
        check(label + ": no duplicates across new DBs", noDup, "");
        check(label + ": no loss (all " + truth.size() + " keys present)", noLoss,
                "have " + seen.size());
        check(label + ": values intact after merge", valuesIntact, "");
        check(label + ": each key in the subtask that owns its key group", ownership, "");
    }

    static String ranges(List<Handle> hs) {
        StringBuilder sb = new StringBuilder();
        for (Handle h : hs) sb.append("[").append(h.lo).append(",").append(h.hi).append("]");
        return sb.toString();
    }
    static String shortName(String p) { int i = p.lastIndexOf('/'); return i < 0 ? p : p.substring(i + 1); }
}
