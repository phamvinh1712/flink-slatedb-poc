package io.slatedb.flink.poc;

import io.slatedb.uniffi.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * LONG GC TEST (§7) — the companion to SlateDbGcE2E that actually WAITS OUT the compactor's 900s
 * (15-minute) internal-checkpoint expiry and observes the orphaned SST files get PHYSICALLY DELETED.
 *
 * Why this is separate and slow: post-compaction orphans are pinned by an auto-created compactor
 * checkpoint with a hardcoded 900s lifetime (slatedb compactor_state_protocols.rs:250 — "so that
 * it's extremely unlikely for the gc to delete ssts out from underneath the writer"). Only after
 * that checkpoint expires does the compacted-SST GC delete the orphans. So this test runs ~17 min.
 *
 * It is NOT part of the default quick suite. Run it explicitly when you want to see reclamation:
 *   mvn -q compile
 *   java ... io.slatedb.flink.poc.SlateDbGcLongE2E
 *
 * Verifies:
 *   - orphans exist after compaction (on-disk files > latest-manifest referenced),
 *   - after ~900s the on-disk compacted file count DROPS to essentially the live (manifest-referenced) set,
 *   - all data still intact.
 *
 * Requires JDK 22+, -Djava.library.path=native. Honors env GC_LONG_MAX_MINUTES (default 17).
 */
public final class SlateDbGcLongE2E {

    static final int NUM_KEYS = 2000;
    static final int BATCHES  = 20;

    private static <T> T await(CompletableFuture<T> f) {
        try { return f.get(120, TimeUnit.SECONDS); } catch (Exception e) { throw new RuntimeException(e); }
    }
    private static byte[] bytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }
    private static boolean ok = true;
    private static void check(String name, boolean cond, String detail) {
        System.out.println((cond ? "  [PASS] " : "  [FAIL] ") + name + (detail.isEmpty() ? "" : " — " + detail));
        if (!cond) ok = false;
    }
    private static int countCompactedSstFiles(Path dbRoot) {
        Path c = dbRoot.resolve("compacted");
        if (!Files.isDirectory(c)) return 0;
        try (Stream<Path> s = Files.list(c)) {
            return (int) s.filter(p -> p.getFileName().toString().endsWith(".sst")).count();
        } catch (IOException e) { throw new RuntimeException(e); }
    }
    private static int referenced(Admin admin) {
        VersionedManifest m = await(admin.readManifest(null));
        return m.l0().size() + m.compacted().stream().mapToInt(sr -> sr.sstViews().size()).sum();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== LONG GC E2E: wait out the 900s compactor checkpoint, observe orphan SSTs physically deleted (§7) ===");
        System.out.println("Runtime Java: " + System.getProperty("java.version"));
        int maxMinutes = Integer.parseInt(System.getenv().getOrDefault("GC_LONG_MAX_MINUTES", "17"));

        Path work = Files.createTempDirectory("flink-slatedb-gclong-");
        String dbName = work.resolve("gc-db").toString().substring(1);
        Path dbRoot = Paths.get("/" + dbName);

        try (ObjectStore store = ObjectStore.resolve("file:///")) {
            Settings s = Settings._default();
            s.set("l0_sst_size_bytes", "4096");
            s.set("compactor_options.poll_interval", "\"200ms\"");
            s.set("garbage_collector_options.compacted_options.min_age", "\"2s\"");
            s.set("garbage_collector_options.compacted_options.interval", "\"5s\"");

            DbBuilder db = new DbBuilder(dbName, store);
            db.withSettings(s);
            Db d = await(db.build());
            Admin admin = new AdminBuilder(dbName, store).build();

            Map<String, String> truth = new HashMap<>();
            for (int b = 0; b < BATCHES; b++) {
                for (int i = 0; i < NUM_KEYS / BATCHES; i++) {
                    int id = b * (NUM_KEYS / BATCHES) + i;
                    String k = "key-" + id, v = "val-" + id + "-b" + b;
                    await(d.put(bytes(k), bytes(v)));
                    truth.put(k, v);
                }
                await(d.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));
            }
            Thread.sleep(8000);   // let round-1 compaction settle

            int afterCompactionFiles = countCompactedSstFiles(dbRoot);
            int liveReferenced = referenced(admin);
            System.out.println("  after compaction: onDiskFiles=" + afterCompactionFiles
                    + " manifestReferenced=" + liveReferenced);
            check("§7 orphans exist after compaction", afterCompactionFiles > liveReferenced,
                    "files=" + afterCompactionFiles + " referenced=" + liveReferenced);

            // Poll every 30s until files collapse to ~live set, or timeout.
            int minFilesSeen = afterCompactionFiles;
            long start = System.currentTimeMillis();
            boolean deleted = false;
            while ((System.currentTimeMillis() - start) < maxMinutes * 60_000L) {
                int files = countCompactedSstFiles(dbRoot);
                int ref = referenced(admin);
                minFilesSeen = Math.min(minFilesSeen, files);
                long elapsed = (System.currentTimeMillis() - start) / 1000;
                System.out.println("  t+" + elapsed + "s: onDiskFiles=" + files + " referenced=" + ref);
                if (files < afterCompactionFiles && (files - ref) <= 3) { deleted = true; break; }
                Thread.sleep(30_000);
            }

            check("§7 GC PHYSICALLY DELETED orphans after the 900s checkpoint expiry",
                    deleted, "minFilesSeen=" + minFilesSeen + " afterCompactionFiles=" + afterCompactionFiles);

            int wrong = 0, missing = 0;
            for (var e : truth.entrySet()) {
                byte[] got = await(d.get(bytes(e.getKey())));
                if (got == null) missing++;
                else if (!e.getValue().equals(new String(got, StandardCharsets.UTF_8))) wrong++;
            }
            check("§7 NO DATA LOSS after physical GC (all " + truth.size() + " keys intact)",
                    missing == 0 && wrong == 0, "missing=" + missing + " wrong=" + wrong);

            await(d.shutdown()); d.close(); admin.close(); db.close(); s.close();

        } catch (UnsatisfiedLinkError | NoClassDefFoundError e) {
            System.out.println("  [SKIP] SlateDB native lib/class load failed — need JDK 22+ + -Djava.library.path. " + e);
            System.exit(2);
        }

        System.out.println();
        System.out.println(ok ? "LONG GC E2E PASSED ✅ (orphans physically deleted after the 900s compactor-checkpoint expiry; zero data loss)"
                              : "LONG GC E2E FAILED ❌ (or timed out before the 900s expiry elapsed)");
        if (!ok) System.exit(1);
    }
}
