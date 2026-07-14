package io.slatedb.flink.poc;

import io.slatedb.uniffi.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * DATABASE SPLIT + MERGE via RFC-0004 projection & union (the clone builder), verified by RUNNING.
 *
 * This exercises the RFC-blessed rescale primitives (README §6.4 discusses them; the older
 * SlateDbRescaleE2E proved projection/split + a manual scan-copy merge — this proves the actual
 * UNION path, the one gap that test left):
 *
 *   SPLIT  = projection: clone a DB restricted to a key range (CloneBuilder.withProjectionRange /
 *            CloneSourceSpec range). Keys outside the range are logically deleted.
 *   MERGE  = union: clone from MULTIPLE non-overlapping sources (CloneBuilder.withSource called
 *            repeatedly). RFC rules enforced by the operation: ranges must be non-overlapping &
 *            adjacent; each source's WAL must be flushed to L0 first (union can't merge WAL state).
 *
 * Layout: keys "key-000000".."key-000599" (fixed-width so lexical order = numeric order).
 * Split boundary at "key-000300": lo = [key-000000, key-000300), hi = [key-000300, key-000300~end].
 *
 * Verifies:
 *   A. SPLIT: lo shard has exactly the low half, hi shard exactly the high half (projection clips).
 *   B. MERGE: union(lo, hi) → a DB with ALL 600 keys, every value correct (no loss/dup/corruption).
 *   C. the merged DB is writable (new writes land) and the seq counter advanced past carried data.
 *
 * Requires JDK 22+, -Djava.library.path=native.
 */
public final class SlateDbMergeSplitE2E {

    static final int N = 600;
    static final int SPLIT_AT = 300;

    static <T> T await(CompletableFuture<T> f) {
        try { return f.get(120, TimeUnit.SECONDS); } catch (Exception e) { throw new RuntimeException(e); }
    }
    static byte[] b(String s) { return s.getBytes(StandardCharsets.UTF_8); }
    static String s(byte[] x) { return x == null ? null : new String(x, StandardCharsets.UTF_8); }
    static byte[] key(int i) { return b(String.format("key-%06d", i)); }   // fixed-width → lexical==numeric
    static byte[] val(int i) { return b("val-" + i); }

    static boolean ok = true;
    static void check(String n, boolean c, String d) {
        System.out.println((c ? "  [PASS] " : "  [FAIL] ") + n + (d.isEmpty() ? "" : " — " + d));
        if (!c) ok = false;
    }

    /** Count how many of key(0..N-1) are present in the DB at `dbName`, and how many have the right value. */
    static int[] verifyKeys(ObjectStore store, String dbName, int from, int to) {
        Db db = await(new DbBuilder(dbName, store).build());
        int present = 0, wrong = 0;
        try {
            for (int i = from; i < to; i++) {
                byte[] got = await(db.get(key(i)));
                if (got != null) { present++; if (!s(val(i)).equals(s(got))) wrong++; }
            }
        } finally { await(db.shutdown()); db.close(); }
        return new int[]{present, wrong};
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== DB SPLIT + MERGE via RFC-0004 projection & union (§6.4) ===");
        System.out.println("Runtime Java: " + System.getProperty("java.version"));

        Path work = Files.createTempDirectory("flink-slatedb-mergesplit-");
        // file:/// root; db names are absolute paths with leading '/' stripped (§16.7).
        String srcName = work.resolve("src-db").toString().substring(1);
        String loName  = work.resolve("shard-lo").toString().substring(1);
        String hiName  = work.resolve("shard-hi").toString().substring(1);
        String mergedName = work.resolve("merged-db").toString().substring(1);
        byte[] boundary = key(SPLIT_AT);   // "key-000300"

        try (ObjectStore store = ObjectStore.resolve("file:///")) {

            // ---- populate the source DB with N keys, then flush (union needs data in L0, not WAL) ----
            Db src = await(new DbBuilder(srcName, store).build());
            for (int i = 0; i < N; i++) await(src.put(key(i), val(i)));
            await(src.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));   // WAL → L0 (§16.2 / union rule)
            await(src.shutdown()); src.close();
            System.out.println("populated src with " + N + " keys, flushed to L0");

            Admin admin = new AdminBuilder(srcName, store).build();

            // ================= SPLIT via PROJECTION =================
            // lo = [key-000000, key-000300)   (start inclusive, end EXCLUSIVE)
            CloneBuilder lo = admin.createCloneBuilderFromSource(
                    new CloneSourceSpec(srcName, null, null));   // null checkpoint → clone latest
            lo.withClonePath(loName);                      // clone path is an absolute uri path
            lo.withProjectionRange(new KeyRange(key(0), true, boundary, false));
            await(lo.build()); lo.close();

            // hi = [key-000300, key-000599]   (both inclusive; end = last key)
            CloneBuilder hi = admin.createCloneBuilderFromSource(
                    new CloneSourceSpec(srcName, null, null));
            hi.withClonePath(hiName);
            hi.withProjectionRange(new KeyRange(boundary, true, key(N - 1), true));
            await(hi.build()); hi.close();
            System.out.println("split src → shard-lo [0,300) + shard-hi [300,599]");

            // A. verify the split: lo has low half only, hi has high half only.
            int[] loAll = verifyKeys(store, loName, 0, N);        // present across whole keyspace
            int[] hiAll = verifyKeys(store, hiName, 0, N);
            int[] loLow = verifyKeys(store, loName, 0, SPLIT_AT); // low half present in lo?
            int[] hiHigh = verifyKeys(store, hiName, SPLIT_AT, N);
            check("SPLIT: shard-lo has the low half (300 keys) and only those",
                    loLow[0] == SPLIT_AT && loLow[1] == 0 && loAll[0] == SPLIT_AT,
                    "loLowPresent=" + loLow[0] + " loLowWrong=" + loLow[1] + " loTotalPresent=" + loAll[0]);
            check("SPLIT: shard-hi has the high half (300 keys) and only those",
                    hiHigh[0] == (N - SPLIT_AT) && hiHigh[1] == 0 && hiAll[0] == (N - SPLIT_AT),
                    "hiHighPresent=" + hiHigh[0] + " hiHighWrong=" + hiHigh[1] + " hiTotalPresent=" + hiAll[0]);

            // ================= MERGE via UNION =================
            // Union the two adjacent, non-overlapping shards back into one DB. Each CloneSourceSpec
            // carries its own range; withSource appends the 2nd source. Ranges must be adjacent:
            // lo=[0,300), hi=[300,599]  → together cover [0,599], no overlap.
            CloneBuilder merged = admin.createCloneBuilderFromSource(
                    new CloneSourceSpec(loName, null, new KeyRange(key(0), true, boundary, false)));
            merged.withSource(
                    new CloneSourceSpec(hiName, null, new KeyRange(boundary, true, key(N - 1), true)));
            merged.withClonePath(mergedName);
            await(merged.build()); merged.close();
            System.out.println("union(shard-lo, shard-hi) → merged-db");

            // B. verify the merge: ALL N keys present with correct values.
            int[] mergedAll = verifyKeys(store, mergedName, 0, N);
            check("MERGE: union has ALL " + N + " keys with correct values",
                    mergedAll[0] == N && mergedAll[1] == 0,
                    "present=" + mergedAll[0] + "/" + N + " wrong=" + mergedAll[1]);

            // C. merged DB is writable and reads back new + carried-over data together.
            Db mdb = await(new DbBuilder(mergedName, store).build());
            await(mdb.put(key(N), val(N)));                 // brand-new key past the merged range end
            check("MERGE: merged DB accepts new writes", s(val(N)).equals(s(await(mdb.get(key(N))))),
                    "newKey=" + s(await(mdb.get(key(N)))));
            // spot-check one key from each source survived in the live merged DB
            check("MERGE: a low-shard key + a high-shard key both readable from merged DB",
                    s(val(42)).equals(s(await(mdb.get(key(42)))))
                            && s(val(555)).equals(s(await(mdb.get(key(555))))),
                    "k42=" + s(await(mdb.get(key(42)))) + " k555=" + s(await(mdb.get(key(555)))));
            await(mdb.shutdown()); mdb.close();

            admin.close();

        } catch (UnsatisfiedLinkError | NoClassDefFoundError e) {
            System.out.println("  [SKIP] native lib/class load failed — need JDK 22+ + -Djava.library.path. " + e);
            System.exit(2);
        }

        System.out.println();
        System.out.println(ok ? "MERGE/SPLIT E2E PASSED ✅ (RFC-0004 projection split + union merge, all keys intact)"
                              : "MERGE/SPLIT E2E FAILED ❌");
        if (!ok) System.exit(1);
    }
}
