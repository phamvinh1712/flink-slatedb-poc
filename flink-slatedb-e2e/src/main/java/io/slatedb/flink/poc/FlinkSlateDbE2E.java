package io.slatedb.flink.poc;

import io.slatedb.uniffi.*;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * END-TO-END: a REAL Flink KeyedProcessFunction backed by SlateDB as its per-key state store, in ONE JVM,
 * with exactly-once restore via clone-from-pinned-checkpoint. This is the composition the rest of the PoC
 * did NOT exercise (§16.5). Requires JDK 22+ (SlateDB FFM); Flink 1.20 runs fine on JDK 25 (§16.3).
 *
 * The stream: [a,a,b, __BARRIER__, a,c]
 *   - Before __BARRIER__: SlateDB counts become a=2, b=1.
 *   - On __BARRIER__ the operator performs the §6.3 checkpoint: MEMTABLE flush + createDetachedCheckpoint,
 *     and records the checkpoint id to a file (the "Flink barrier" analog, taken from inside the operator
 *     that owns the Db — single writer).
 *   - After __BARRIER__: a=3, c=1 are written durably (memtable-flushed) — these are "post-barrier" writes.
 *
 * Then restore (§6.6): CLONE from the pinned checkpoint (NOT a writer reopen) and assert the clone has
 * EXACTLY the pre-barrier state {a=2, b=1} — post-barrier writes {a=3, c=1} discarded → exactly-once.
 *
 * Design points exercised together:
 *   §6.1 key-group-prefixed keys in one SlateDB keyspace
 *   §6.2 blocking get→increment→put per key inside a real keyed operator (single-threaded-per-key ⇒ no races)
 *   §6.3 checkpoint = MEMTABLE flush + createDetachedCheckpoint (the §16.2 fix — plain flush() would lose state)
 *   §6.6 restore = clone-from-checkpoint; post-barrier writes excluded
 *
 * Test simplification (noted honestly): parallelism = 1 (one SlateDB instance, all key groups) so the
 * expected counts are deterministic. The design supports one-Db-per-subtask at higher parallelism (§4);
 * that path (rescale merge) is NOT exercised here.
 */
public final class FlinkSlateDbE2E {

    static final String BARRIER = "__BARRIER__";
    static final int MAX_PARALLELISM = 128;

    private static <T> T await(CompletableFuture<T> f) {
        try { return f.get(60, TimeUnit.SECONDS); } catch (Exception e) { throw new RuntimeException(e); }
    }
    private static byte[] bytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    /** §6.1 key encoding: [keyGroup prefix (1 byte, maxP<=128)][ key ]. */
    static byte[] slateKey(String key) {
        int kg = KeyGroupRangeAssignment.assignToKeyGroup(key, MAX_PARALLELISM);
        byte[] k = bytes(key);
        byte[] out = new byte[1 + k.length];
        out[0] = (byte) kg;                    // 1 prefix byte since maxParallelism <= 128 (§6.1)
        System.arraycopy(k, 0, out, 1, k.length);
        return out;
    }

    private static boolean ok = true;
    private static void check(String name, boolean cond, String detail) {
        System.out.println((cond ? "  [PASS] " : "  [FAIL] ") + name + (detail.isEmpty() ? "" : " — " + detail));
        if (!cond) ok = false;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== END-TO-END: Flink KeyedProcessFunction + SlateDB + clone-restore exactly-once ===");
        System.out.println("Runtime Java: " + System.getProperty("java.version")
                + " | Flink: " + org.apache.flink.runtime.util.EnvironmentInformation.getVersion());

        Path work = Files.createTempDirectory("flink-slatedb-e2e-");
        // VERIFIED API pattern (§16.4): ObjectStore.resolve requires an EMPTY path — the store is the
        // filesystem ROOT ("file:///"), and the DB path (absolute, minus leading '/') is the builder db-name.
        // ObjectStore.resolve("file:///tmp/x") FAILS ("provide path to builder instead"); "file:///" is OK.
        String parentDbName = work.resolve("parent-db").toString().substring(1);  // strip leading '/'
        String cloneDbName  = work.resolve("clone-db").toString().substring(1);
        Path cpIdFile = work.resolve("checkpoint-id.txt");

        try {
            // ---- Phase 1: REAL Flink job writes SlateDB state through a keyed operator ----
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            env.setMaxParallelism(MAX_PARALLELISM);

            List<String> stream = List.of("a", "a", "b", BARRIER, "a", "c");
            env.fromData(stream)
               .keyBy(s -> s.equals(BARRIER) ? "a" : s)   // route BARRIER with a real key so it lands in the operator
               .process(new SlateDbCountingFn(parentDbName, cpIdFile.toString()))
               .print();
            env.execute("flink-slatedb-e2e-phase1");

            // ---- Read the checkpoint id the operator recorded at the barrier ----
            String cpId = Files.readString(cpIdFile).trim();
            check("operator recorded a checkpoint id at the barrier (§6.3)", !cpId.isEmpty(), "cpId=" + cpId);

            // ---- Phase 2: restore via CLONE from the pinned checkpoint (§6.6), assert exactly-once ----
            Map<String, Long> cloneCounts = cloneAndReadCounts(parentDbName, cloneDbName, cpId,
                    List.of("a", "b", "c"));

            System.out.println("  clone counts = " + cloneCounts);
            check("§6.6 exactly-once: clone has PRE-barrier a=2", cloneCounts.getOrDefault("a", -1L) == 2L,
                    "expected 2, got " + cloneCounts.get("a") + " (3 would mean post-barrier write leaked)");
            check("§6.6 exactly-once: clone has PRE-barrier b=1", cloneCounts.getOrDefault("b", -1L) == 1L,
                    "got " + cloneCounts.get("b"));
            check("§6.6 exactly-once: clone EXCLUDES post-barrier key c", !cloneCounts.containsKey("c"),
                    "c present would mean post-barrier write leaked");

        } catch (UnsatisfiedLinkError | NoClassDefFoundError e) {
            System.out.println("  [SKIP] SlateDB native lib/class load failed — need JDK 22+ and -Djava.library.path. " + e);
            System.exit(2);
        }

        System.out.println();
        System.out.println(ok ? "E2E PASSED ✅  (real Flink keyed operator + SlateDB + exactly-once clone-restore)"
                              : "E2E FAILED ❌");
        if (!ok) System.exit(1);
    }

    /** Clone parent@cpId → clonePath, open the clone, read the counter for each key. */
    private static Map<String, Long> cloneAndReadCounts(String parentDbName, String cloneDbName,
                                                         String cpId, List<String> keys) throws Exception {
        // Store = filesystem ROOT ("file:///"); db names are absolute paths (leading '/' stripped). §16.4
        try (ObjectStore store = ObjectStore.resolve("file:///")) {
            // Clone parent@cpId → clone db.
            try (Admin admin = new AdminBuilder(parentDbName, store).build()) {
                CloneSourceSpec src = new CloneSourceSpec(parentDbName, cpId, null);
                CloneBuilder cb = admin.createCloneBuilderFromSource(src);
                cb.withClonePath(cloneDbName);
                cb.withObjectStore(store);
                await(cb.build());
            }
            // Open the clone and read the counters.
            Map<String, Long> counts = new TreeMap<>();
            try (DbBuilder cbb = new DbBuilder(cloneDbName, store)) {
                Db clone = await(cbb.build());
                try (clone) {
                    for (String key : keys) {
                        byte[] v = await(clone.get(slateKey(key)));
                        if (v != null) counts.put(key, Long.parseLong(new String(v, StandardCharsets.UTF_8)));
                    }
                    await(clone.shutdown());
                }
            }
            return counts;
        }
    }

    /**
     * Real Flink keyed operator. Per element: RMW-increment a per-key counter in SlateDB (§6.2).
     * On the BARRIER sentinel: perform the §6.3 checkpoint (MEMTABLE flush + createDetachedCheckpoint)
     * and record the checkpoint id to a file. Opens SlateDB rooted at the work dir; db name "parent-db".
     */
    public static final class SlateDbCountingFn extends KeyedProcessFunction<String, String, String> {
        private final String dbName;         // absolute path (leading '/' stripped), e.g. var/folders/.../parent-db
        private final String cpIdFile;
        private transient ObjectStore store;
        private transient Db db;
        private transient Admin admin;

        SlateDbCountingFn(String dbName, String cpIdFile) {
            this.dbName = dbName;
            this.cpIdFile = cpIdFile;
        }

        @Override
        public void open(OpenContext ctx) throws Exception {
            // Store = filesystem ROOT; db-name is the absolute path. §16.4 verified pattern.
            this.store = ObjectStore.resolve("file:///");
            this.db = await(new DbBuilder(dbName, store).build());
            this.admin = new AdminBuilder(dbName, store).build();
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            if (BARRIER.equals(value)) {
                // §6.3: the checkpoint barrier — MEMTABLE flush (not plain flush(); see §16.2) then detached checkpoint.
                await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));
                CheckpointCreateResult cp = await(admin.createDetachedCheckpoint(new CheckpointOptions(null, null, null)));
                Files.writeString(Path.of(cpIdFile), cp.id());
                out.collect("BARRIER→checkpoint " + cp.id());
                return;
            }
            // §6.2 RMW per key — safe because Flink runs one element at a time per key (single-threaded-per-key).
            byte[] k = slateKey(value);
            byte[] cur = await(db.get(k));
            long n = (cur == null) ? 0L : Long.parseLong(new String(cur, StandardCharsets.UTF_8));
            n++;
            await(db.put(k, bytes(Long.toString(n))));
            out.collect(value + "=" + n);
        }

        @Override
        public void close() throws Exception {
            if (db != null) {
                // Make post-barrier writes durable in the manifest too (so we can PROVE the clone excludes them).
                try { await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE))); } catch (Throwable ignored) {}
                try { await(db.shutdown()); } catch (Throwable ignored) {}
                try { db.close(); } catch (Throwable ignored) {}
            }
            if (admin != null) try { admin.close(); } catch (Throwable ignored) {}
            if (store != null) try { store.close(); } catch (Throwable ignored) {}
        }
    }
}
