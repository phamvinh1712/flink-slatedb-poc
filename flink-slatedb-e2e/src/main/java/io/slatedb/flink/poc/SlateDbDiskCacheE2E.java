package io.slatedb.flink.poc;

import io.slatedb.uniffi.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * LOCAL-DISK CACHE directory config (§9.1a/§9.2) — answer "how do I put SlateDB's cache on local SSD?"
 *
 * IMPORTANT distinction discovered from slatedb-uniffi 0.14.1 source:
 *   - The foyer BLOCK cache exposed to Java (DbCache.newFoyerCache) is MEMORY-ONLY —
 *     FoyerCacheOptions is {maxCapacity, shards}, no disk dir. Foyer's disk/hybrid tier
 *     (FoyerHybridCache, FsDeviceBuilder("/path")) is NOT bound by uniffi → unreachable from Java.
 *   - The reachable on-disk cache is the OBJECT-STORE cache (CachedObjectStore): a local-SSD
 *     mirror of SST/part files, configured via Settings key `object_store_cache_options.root_folder`.
 *     This is the one to point at local SSD under Flink (§9.2 tiering).
 *
 * This test sets root_folder to a temp dir, does read-heavy work, and asserts SlateDB actually
 * MATERIALIZES cache files under that directory (proving the setting is wired end-to-end from Java).
 * Requires JDK 22+, -Djava.library.path=native.
 */
public final class SlateDbDiskCacheE2E {

    static <T> T await(CompletableFuture<T> f) {
        try { return f.get(60, TimeUnit.SECONDS); } catch (Exception e) { throw new RuntimeException(e); }
    }
    static byte[] b(String s) { return s.getBytes(StandardCharsets.UTF_8); }
    static boolean ok = true;
    static void check(String n, boolean c, String d) {
        System.out.println((c ? "  [PASS] " : "  [FAIL] ") + n + (d.isEmpty() ? "" : " — " + d));
        if (!c) ok = false;
    }
    static long countFiles(Path root) throws Exception {
        if (!Files.exists(root)) return 0;
        AtomicLong n = new AtomicLong();
        Files.walk(root).filter(Files::isRegularFile).forEach(p -> n.incrementAndGet());
        return n.get();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== SlateDB LOCAL-DISK (object-store) CACHE directory config (§9.1a/§9.2) ===");
        System.out.println("Runtime Java: " + System.getProperty("java.version"));

        Path work = Files.createTempDirectory("flink-slatedb-diskcache-");
        Path cacheDir = work.resolve("ssd-cache");           // ← where we tell SlateDB to mirror SSTs
        String dbName = work.resolve("db").toString().substring(1);
        String cachePath = cacheDir.toString();

        try (ObjectStore store = ObjectStore.resolve("file:///")) {
            Settings s = Settings._default();
            // point the local-disk object-store cache at cacheDir; also cache PUTs so writes populate it,
            // and use a small L0 so data flushes to SSTs promptly.
            s.set("object_store_cache_options.root_folder", "\"" + cachePath + "\"");
            s.set("object_store_cache_options.cache_puts", "true");
            s.set("object_store_cache_options.part_size_bytes", "1024");
            s.set("l0_sst_size_bytes", "4096");
            System.out.println("  configured object_store_cache_options.root_folder = " + cachePath);
            System.out.println("  settings JSON = " + s.toJsonString());

            DbBuilder builder = new DbBuilder(dbName, store);
            builder.withSettings(s);
            Db db = await(builder.build());

            long before = countFiles(cacheDir);

            // write + flush several rounds → SSTs; then read them back → populates the disk cache on read
            for (int round = 0; round < 6; round++) {
                for (int i = 0; i < 120; i++) await(db.put(b("k" + (round * 120 + i)), b("val-" + round + "-" + i)));
                await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));
            }
            for (int i = 0; i < 720; i++) await(db.get(b("k" + i)));    // reads pull SST parts through the cache

            Thread.sleep(1500);   // let async cache writes settle
            long after = countFiles(cacheDir);

            check("A. root_folder accepted + DB opened with disk cache configured", true, "");
            check("B. cache directory created on disk", Files.isDirectory(cacheDir), cachePath);
            check("C. SlateDB materialized cache files under root_folder",
                    after > before && after > 0, "files before=" + before + " after=" + after);

            // sanity: data still correct through the cached path
            String v = new String(await(db.get(b("k0"))), StandardCharsets.UTF_8);
            check("D. reads correct through the disk-cached path", v.equals("val-0-0"), "k0=" + v);

            await(db.shutdown()); db.close(); s.close();

        } catch (UnsatisfiedLinkError | NoClassDefFoundError e) {
            System.out.println("  [SKIP] native/class load failed — need JDK 22+ + -Djava.library.path. " + e);
            System.exit(2);
        }

        System.out.println();
        System.out.println(ok ? "DISK-CACHE E2E PASSED ✅ (object_store_cache_options.root_folder puts SlateDB's cache on local SSD from Java)"
                              : "DISK-CACHE E2E FAILED ❌");
        if (!ok) System.exit(1);
    }
}
