package io.slatedb.flink.poc;

import io.slatedb.uniffi.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * COMPACTION TEST (§7/§8) — observe SlateDB compaction actually running, and verify data survives it.
 *
 * Design claims exercised:
 *   §7  compaction runs (embedded compactor merges L0 SSTs into sorted runs) — OBSERVED via the manifest:
 *       many L0 SSTs accumulate, then L0 count DROPS while `compacted` (sorted runs) grows.
 *   §7  compaction does NOT lose/corrupt data — every key still readable & correct after compaction.
 *   §8  the knobs are real — we tune l0_sst_size_bytes small + many memtable flushes to force many L0 SSTs,
 *       and poll_interval small so the compactor reacts; then watch it drain L0.
 *
 * Method: force MANY small L0 SSTs (small l0_sst_size + a memtable flush per batch), poll the manifest over
 * time, and assert (a) L0 rose above the compaction threshold at some point, and (b) the compactor then
 * reduced L0 by merging into sorted runs (manifest.compacted grew), and (c) all data intact throughout.
 * Requires JDK 22+.
 */
public final class SlateDbCompactionE2E {

    static final int NUM_KEYS = 2000;
    static final int BATCHES = 20;           // 20 memtable flushes → up to ~20 L0 SSTs before compaction

    private static <T> T await(CompletableFuture<T> f) {
        try { return f.get(120, TimeUnit.SECONDS); } catch (Exception e) { throw new RuntimeException(e); }
    }
    private static byte[] bytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    private static boolean ok = true;
    private static void check(String name, boolean cond, String detail) {
        System.out.println((cond ? "  [PASS] " : "  [FAIL] ") + name + (detail.isEmpty() ? "" : " — " + detail));
        if (!cond) ok = false;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== COMPACTION E2E: force L0 SSTs, observe compactor merge them, verify no data loss (§7/§8) ===");
        System.out.println("Runtime Java: " + System.getProperty("java.version"));

        Path work = Files.createTempDirectory("flink-slatedb-compaction-");
        String dbName = work.resolve("compact-db").toString().substring(1);   // §16.4 file:/// root

        try (ObjectStore store = ObjectStore.resolve("file:///")) {

            // §8 tuning: tiny L0 SSTs so each flush makes a new one; aggressive compactor polling.
            Settings s = Settings._default();
            // set() takes JSON values: numbers bare, strings JSON-quoted, durations as quoted strings.
            s.set("l0_sst_size_bytes", "4096");                       // 4KB → many small L0 SSTs
            s.set("compactor_options.poll_interval", "\"200ms\"");    // quoted duration string
            // (leave l0_max_ssts_per_key default so compaction triggers well before any backpressure stall)

            DbBuilder db = new DbBuilder(dbName, store);
            db.withSettings(s);
            Db d = await(db.build());
            Admin admin = new AdminBuilder(dbName, store).build();

            // Write in batches, memtable-flushing each batch → forces a fresh L0 SST per batch.
            Map<String, String> truth = new HashMap<>();
            for (int b = 0; b < BATCHES; b++) {
                for (int i = 0; i < NUM_KEYS / BATCHES; i++) {
                    int id = b * (NUM_KEYS / BATCHES) + i;
                    String k = "key-" + id, v = "val-" + id + "-b" + b;
                    await(d.put(bytes(k), bytes(v)));
                    truth.put(k, v);
                }
                await(d.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));   // → new L0 SST
            }

            // Poll the manifest over ~6s: track peak L0 count and whether sorted runs (compacted) appear.
            int peakL0 = 0, minL0After = Integer.MAX_VALUE, maxCompactedRuns = 0;
            long maxCompactedSize = 0;
            for (int poll = 0; poll < 30; poll++) {
                VersionedManifest m = await(admin.readManifest(null));   // null → latest
                int l0 = m.l0().size();
                int runs = m.compacted().size();
                long compactedBytes = m.compacted().stream().mapToLong(SortedRun::estimatedSizeBytes).sum();
                peakL0 = Math.max(peakL0, l0);
                maxCompactedRuns = Math.max(maxCompactedRuns, runs);
                maxCompactedSize = Math.max(maxCompactedSize, compactedBytes);
                if (runs > 0) minL0After = Math.min(minL0After, l0);   // L0 count once compaction has produced runs
                if (poll % 5 == 0)
                    System.out.println("  poll " + poll + ": L0=" + l0 + " sortedRuns=" + runs
                            + " compactedBytes=" + compactedBytes);
                Thread.sleep(200);
            }
            System.out.println("  → peakL0=" + peakL0 + " maxSortedRuns=" + maxCompactedRuns
                    + " minL0AfterCompaction=" + (minL0After == Integer.MAX_VALUE ? "n/a" : minL0After)
                    + " maxCompactedBytes=" + maxCompactedSize);

            // §7 assertion 1: compaction actually ran — sorted runs were produced from L0.
            check("§7 compactor produced sorted runs (compaction ran)", maxCompactedRuns > 0,
                    "maxSortedRuns=" + maxCompactedRuns);
            // §7 assertion 2: L0 was drained — after compaction, L0 count is below the peak (SSTs got merged away).
            check("§7 compaction DRAINED L0 (post-compaction L0 < peak L0)",
                    minL0After != Integer.MAX_VALUE && minL0After < peakL0,
                    "peakL0=" + peakL0 + " minL0After=" + (minL0After == Integer.MAX_VALUE ? "n/a" : minL0After));
            // §7 assertion 3: sorted runs hold real data.
            check("§7 sorted runs contain data (compactedBytes > 0)", maxCompactedSize > 0,
                    "bytes=" + maxCompactedSize);

            // §7 assertion 4 (the one that matters): NO DATA LOSS across compaction — every key intact.
            int wrong = 0, missing = 0;
            for (var e : truth.entrySet()) {
                byte[] got = await(d.get(bytes(e.getKey())));
                if (got == null) missing++;
                else if (!e.getValue().equals(new String(got, StandardCharsets.UTF_8))) wrong++;
            }
            check("§7 NO DATA LOSS/CORRUPTION across compaction (all " + truth.size() + " keys intact)",
                    missing == 0 && wrong == 0, "missing=" + missing + " wrong=" + wrong);

            await(d.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));
            await(d.shutdown()); d.close(); admin.close(); db.close(); s.close();

        } catch (UnsatisfiedLinkError | NoClassDefFoundError e) {
            System.out.println("  [SKIP] SlateDB native lib/class load failed — need JDK 22+ + -Djava.library.path. " + e);
            System.exit(2);
        }

        System.out.println();
        System.out.println(ok ? "COMPACTION E2E PASSED ✅ (compactor merged L0→sorted-runs, drained L0, zero data loss)"
                              : "COMPACTION E2E FAILED ❌");
        if (!ok) System.exit(1);
    }
}
