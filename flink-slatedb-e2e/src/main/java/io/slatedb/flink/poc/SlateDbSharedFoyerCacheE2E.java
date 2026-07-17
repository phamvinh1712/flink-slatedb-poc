package io.slatedb.flink.poc;

import io.slatedb.uniffi.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * SHARED FOYER CACHE across SlateDB instances (§9.1a / §12.7) — verify one FoyerCache can back many
 * DBs safely. This is the primary mitigation for the §4.1 N/P memory trap: instead of N/P × 640 MiB
 * of per-DB cache, all shards on a subtask draw from ONE bounded pool.
 *
 * FlinkShardPerBucketParallelE2E already proved this for MOKA at P=4; this proves it for FOYER
 * specifically (the DEFAULT backend), and adds a direct collision-safety probe: SAME key bytes,
 * DIFFERENT values, in DIFFERENT DBs sharing the cache — each DB must read back ITS OWN value
 * (scope_id namespacing: CachedKey{scope_id, sst_id, block_id}).
 *
 * Verifies:
 *   A. one FoyerCache backs N DBs (build once, withDbCache to each) — opens + works.
 *   B. COLLISION-SAFE: same keys with per-DB values → each DB reads its own (no cross-contamination).
 *   C. reads are served correctly through the shared cache after flush (cache actually used, no mixups).
 * Requires JDK 22+, -Djava.library.path=native.
 */
public final class SlateDbSharedFoyerCacheE2E {

    static final int NUM_DBS = 4;      // shards sharing one cache
    static final int KEYS = 150;

    static <T> T await(CompletableFuture<T> f) {
        try { return f.get(60, TimeUnit.SECONDS); } catch (Exception e) { throw new RuntimeException(e); }
    }
    static byte[] b(String s) { return s.getBytes(StandardCharsets.UTF_8); }
    static String s(byte[] x) { return x == null ? null : new String(x, StandardCharsets.UTF_8); }
    static boolean ok = true;
    static void check(String n, boolean c, String d) {
        System.out.println((c ? "  [PASS] " : "  [FAIL] ") + n + (d.isEmpty() ? "" : " — " + d));
        if (!c) ok = false;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== SHARED FOYER CACHE across SlateDB instances (§9.1a/§12.7) ===");
        System.out.println("Runtime Java: " + System.getProperty("java.version"));

        Path work = Files.createTempDirectory("flink-slatedb-foyer-");

        try (ObjectStore store = ObjectStore.resolve("file:///")) {
            // ONE foyer cache: 64 MiB, 4 shards — shared by ALL DBs below.
            DbCache shared = DbCache.newFoyerCache(new FoyerCacheOptions(64L * 1024 * 1024, 4));

            Db[] dbs = new Db[NUM_DBS];
            String[] names = new String[NUM_DBS];
            for (int d = 0; d < NUM_DBS; d++) {
                names[d] = work.resolve("db-" + d).toString().substring(1);
                DbBuilder builder = new DbBuilder(names[d], store);
                builder.withDbCache(shared);                 // ← SAME cache instance to every builder
                dbs[d] = await(builder.build());
            }
            check("A. " + NUM_DBS + " DBs opened against ONE shared FoyerCache", true, "");

            // Write the SAME key set into every DB, but with DB-specific values:
            //   db d, key k  →  value "d<d>-k<k>".  If the shared cache mixed entries across DBs,
            //   a read from db i would return db j's value.
            for (int d = 0; d < NUM_DBS; d++) {
                for (int k = 0; k < KEYS; k++) await(dbs[d].put(b("key-" + k), b("d" + d + "-k" + k)));
                await(dbs[d].flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));   // → SSTs, cache populated
            }
            System.out.println("  wrote " + KEYS + " keys × " + NUM_DBS + " DBs (same keys, per-DB values)");
            System.out.flush();

            // B+C. Read every key from every DB (twice: cold then warm through the shared cache);
            // each must return ITS OWN value, never another DB's.
            int wrong = 0, missing = 0, crossContam = 0;
            for (int pass = 0; pass < 2; pass++) {           // pass 0 warms the shared cache, pass 1 hits it
                for (int d = 0; d < NUM_DBS; d++) {
                    for (int k = 0; k < KEYS; k++) {
                        String got = s(await(dbs[d].get(b("key-" + k))));
                        String mine = "d" + d + "-k" + k;
                        if (got == null) missing++;
                        else if (!got.equals(mine)) {
                            wrong++;
                            // is it another DB's value? (the collision-failure signature)
                            if (got.matches("d\\d+-k" + k)) crossContam++;
                        }
                    }
                }
            }
            check("B. NO cross-contamination: same keys in " + NUM_DBS + " DBs sharing the cache → each reads its own",
                    crossContam == 0, "crossContaminated=" + crossContam);
            check("C. all reads correct through the shared cache (cold + warm passes)",
                    wrong == 0 && missing == 0, "wrong=" + wrong + " missing=" + missing);

            for (Db d : dbs) { await(d.shutdown()); d.close(); }
            shared.close();

        } catch (UnsatisfiedLinkError | NoClassDefFoundError e) {
            System.out.println("  [SKIP] native/class load failed — need JDK 22+ + -Djava.library.path. " + e);
            System.exit(2);
        }

        System.out.println();
        System.out.println(ok ? "SHARED-FOYER-CACHE PASSED ✅ (one FoyerCache backs many DBs; scope_id keeps them collision-safe)"
                              : "SHARED-FOYER-CACHE FAILED ❌");
        if (!ok) System.exit(1);
    }
}
