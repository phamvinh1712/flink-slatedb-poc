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
 * GARBAGE-COLLECTION TEST (§7 follow-up) — how SlateDB reclaims space after compaction, and the
 * NON-OBVIOUS reason orphaned SST files linger. This test documents a mechanism that only surfaced
 * by RUNNING it (the design-doc assumption "small min_age ⇒ prompt deletion" is WRONG).
 *
 * Verified mechanism (slatedb v0.14.1 source + this test):
 *   1. Compaction does NOT delete files. It merges input SSTs into new sorted-run SSTs and rewrites
 *      the MANIFEST to drop the old ones. The old SST FILES remain on storage as orphans.
 *   2. A background GARBAGE COLLECTOR (garbage_collector/compacted_gc.rs, ON by default) deletes an
 *      orphan only when ALL of these hold:
 *        (a) it is older than `min_age` (compacted dir default 300s),
 *        (b) it is older than the compaction low-watermark,
 *        (c) it is NOT referenced by the latest manifest OR ANY LIVE CHECKPOINT.
 *   3. THE CATCH (compactor_state_protocols.rs:250) — on every manifest commit the compactor first
 *      writes an INTERNAL CHECKPOINT with a hardcoded 900s (15-min) expiry, explicitly "so that it's
 *      extremely unlikely for the gc to delete ssts out from underneath the writer." That checkpoint
 *      pins the manifest that still references the just-compacted L0s. So condition (c) stays FALSE
 *      for ~15 minutes REGARDLESS of how small you set min_age.
 *
 * ⇒ Post-compaction orphans are physically deleted only AFTER the 900s compactor-checkpoint expiry.
 *   This test therefore does NOT wait for deletion (that needs a >15-min run). Instead it verifies,
 *   in seconds, the STATE that explains the delay, all observed live:
 *     A. compaction ran (manifest sorted runs appear, L0 drains).
 *     B. compaction ORPHANED files: on-disk *.sst count in compacted/ exceeds what the LATEST
 *        manifest references.
 *     C. the compactor auto-created CHECKPOINT(s) with a ~900s expiry — the thing pinning the orphans.
 *     D. GC is running yet correctly RETAINS the orphans (count does not collapse to the latest-
 *        manifest live set within the window) — i.e. GC is conservative, not broken.
 *     E. no data loss throughout.
 *
 * See SlateDbGcLongE2E for the >15-min companion that observes the actual physical deletion.
 * Requires JDK 22+ (FFM binding), -Djava.library.path=native.
 */
public final class SlateDbGcE2E {

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
        Path compactedDir = dbRoot.resolve("compacted");
        if (!Files.isDirectory(compactedDir)) return 0;
        try (Stream<Path> s = Files.list(compactedDir)) {
            return (int) s.filter(p -> p.getFileName().toString().endsWith(".sst")).count();
        } catch (IOException e) { throw new RuntimeException(e); }
    }

    /** Distinct compacted SST ids the LATEST manifest references (L0 views + sorted-run views). */
    private static int manifestReferencedSstCount(Admin admin) {
        VersionedManifest m = await(admin.readManifest(null));
        int l0 = m.l0().size();
        int inRuns = m.compacted().stream().mapToInt(sr -> sr.sstViews().size()).sum();
        return l0 + inRuns;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== GC E2E: compaction orphans SST files; GC retains them behind a 900s compactor checkpoint (§7) ===");
        System.out.println("Runtime Java: " + System.getProperty("java.version"));

        Path work = Files.createTempDirectory("flink-slatedb-gc-");
        String dbName = work.resolve("gc-db").toString().substring(1);   // §16.4 file:/// root
        Path dbRoot = Paths.get("/" + dbName);

        try (ObjectStore store = ObjectStore.resolve("file:///")) {

            Settings s = Settings._default();
            s.set("l0_sst_size_bytes", "4096");
            s.set("compactor_options.poll_interval", "\"200ms\"");
            // Aggressively small GC knobs. These are NOT enough to trigger deletion, because the
            // compactor's 900s internal checkpoint pins the orphans — which is exactly the finding.
            s.set("garbage_collector_options.compacted_options.min_age", "\"1s\"");
            s.set("garbage_collector_options.compacted_options.interval", "\"1s\"");

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

            // Observe ~15s: track on-disk file count, manifest-referenced count, sorted runs, checkpoints.
            int peakFiles = 0, peakReferenced = 0, maxSortedRuns = 0, finalFiles = 0, finalReferenced = 0;
            int maxCheckpoints = 0;
            long compactorCkptLifetimeSecs = -1;
            boolean sawCompaction = false;
            System.out.println("  poll: onDiskSstFiles | manifestReferenced | sortedRuns | checkpoints");
            for (int poll = 0; poll < 20; poll++) {
                int files = countCompactedSstFiles(dbRoot);
                VersionedManifest m = await(admin.readManifest(null));
                int referenced = m.l0().size() + m.compacted().stream().mapToInt(sr -> sr.sstViews().size()).sum();
                int runs = m.compacted().size();
                int ckpts = m.checkpoints().size();
                // Capture the compactor checkpoint's lifetime (expire - create) once we see one.
                for (Checkpoint c : m.checkpoints()) {
                    if (c.expireTimeSecs() != null) {
                        compactorCkptLifetimeSecs = c.expireTimeSecs() - c.createTimeSecs();
                    }
                }
                peakFiles = Math.max(peakFiles, files);
                peakReferenced = Math.max(peakReferenced, referenced);
                maxSortedRuns = Math.max(maxSortedRuns, runs);
                maxCheckpoints = Math.max(maxCheckpoints, ckpts);
                if (runs > 0) sawCompaction = true;
                finalFiles = files; finalReferenced = referenced;
                if (poll % 4 == 0)
                    System.out.println("  " + poll + ": " + files + " | " + referenced + " | " + runs + " | " + ckpts);
                Thread.sleep(750);
            }
            System.out.println("  → peakFiles=" + peakFiles + " peakReferenced=" + peakReferenced
                    + " maxSortedRuns=" + maxSortedRuns + " maxCheckpoints=" + maxCheckpoints
                    + " compactorCkptLifetimeSecs=" + compactorCkptLifetimeSecs
                    + " finalFiles=" + finalFiles + " finalReferenced=" + finalReferenced);

            // A. compaction ran.
            check("§7 compaction ran (sorted runs produced)", maxSortedRuns > 0, "maxSortedRuns=" + maxSortedRuns);

            // B. compaction ORPHANED files — more .sst files on disk than the latest manifest references.
            check("§7 compaction ORPHANED files (peak on-disk files > latest-manifest referenced)",
                    peakFiles > peakReferenced, "peakFiles=" + peakFiles + " peakReferenced=" + peakReferenced);

            // C. the compactor auto-created a checkpoint with a ~900s (15-min) expiry — the pin.
            check("§7 compactor auto-created a pinning checkpoint", maxCheckpoints > 0,
                    "checkpoints=" + maxCheckpoints);
            check("§7 that checkpoint has a ~900s lifetime (the GC-retention window)",
                    compactorCkptLifetimeSecs >= 600 && compactorCkptLifetimeSecs <= 1200,
                    "lifetimeSecs=" + compactorCkptLifetimeSecs);

            // D. GC is running but conservatively RETAINS the orphans within our window (not a bug).
            check("§7 GC RETAINS orphans within the window (files stay above live set; deletion is deferred)",
                    finalFiles > finalReferenced,
                    "finalFiles=" + finalFiles + " finalReferenced=" + finalReferenced);

            // E. no data loss.
            int wrong = 0, missing = 0;
            for (var e : truth.entrySet()) {
                byte[] got = await(d.get(bytes(e.getKey())));
                if (got == null) missing++;
                else if (!e.getValue().equals(new String(got, StandardCharsets.UTF_8))) wrong++;
            }
            check("§7 NO DATA LOSS (all " + truth.size() + " keys intact)",
                    missing == 0 && wrong == 0, "missing=" + missing + " wrong=" + wrong);

            System.out.println();
            System.out.println("  NOTE: physical deletion of these orphans happens only after the compactor");
            System.out.println("        checkpoint's ~900s expiry — NOT governed by min_age. See SlateDbGcLongE2E");
            System.out.println("        for the >15-min run that observes the files actually disappear.");

            await(d.shutdown()); d.close(); admin.close(); db.close(); s.close();

        } catch (UnsatisfiedLinkError | NoClassDefFoundError e) {
            System.out.println("  [SKIP] SlateDB native lib/class load failed — need JDK 22+ + -Djava.library.path. " + e);
            System.exit(2);
        }

        System.out.println();
        System.out.println(ok ? "GC E2E PASSED ✅ (compaction orphans files; compactor 900s checkpoint pins them; GC conservatively retains; zero data loss)"
                              : "GC E2E FAILED ❌");
        if (!ok) System.exit(1);
    }
}
