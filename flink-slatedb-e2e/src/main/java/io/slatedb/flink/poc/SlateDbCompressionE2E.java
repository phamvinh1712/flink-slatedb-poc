package io.slatedb.flink.poc;

import io.slatedb.uniffi.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SST COMPRESSION on the object store (§9) — answers two questions:
 *   Q1: "Does SlateDB compress data on S3?"
 *       Default is compression_codec=null (OFF). But the slatedb-uniffi 0.14.1 JAR is built with
 *       features=["all"], so snappy/zlib/lz4/zstd are all COMPILED IN and selectable from Java via
 *       the generic Settings JSON setter. Compression is applied per data/index/filter BLOCK inside
 *       each SST before the SST bytes are PUT to the object store (slatedb/src/format/sst.rs:
 *       compress_and_transform). So yes: with a codec set, the bytes landing on S3 are compressed.
 *
 *       Subtlety this test pins down: the settable JSON path goes through serde on the
 *       CompressionCodec enum (PascalCase variant names → "Snappy"/"Zstd"), NOT the TOML/FromStr path
 *       (lowercase "snappy"). We probe which literal Settings.set() accepts.
 *
 *   Q2: "If I change the compression codec, does it break existing files?"
 *       No. Each SST footer stores its OWN compression_codec (SsTableInfo.compression_codec), and the
 *       read path decompresses with the codec baked into that SST (sst.rs: `let compression_codec =
 *       info.compression_codec; ... decompress(bytes, c)`). It never consults the live Settings on read.
 *       So SSTs written under codec A remain readable after you switch the writer to codec B (or null),
 *       and new SSTs use B. This test proves it: write under Zstd, reopen under Snappy, read A's data
 *       back correctly, write more, reopen under null (off), read everything back.
 *
 * Requires JDK 22+, -Djava.library.path=native.
 */
public final class SlateDbCompressionE2E {

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

    /** Total bytes of *.sst files under a DB dir on the (file://) object store — the on-"S3" footprint. */
    static long sstBytes(Path dbDir) throws Exception {
        if (!Files.exists(dbDir)) return 0;
        AtomicLong n = new AtomicLong();
        Files.walk(dbDir)
                .filter(Files::isRegularFile)
                .filter(p -> p.getFileName().toString().endsWith(".sst"))
                .forEach(p -> { try { n.addAndGet(Files.size(p)); } catch (Exception ignore) {} });
        return n.get();
    }

    /** Try to set compression_codec via the JSON setter; returns the literal that was accepted, or null. */
    static String probeCodecLiteral(String... candidates) {
        for (String lit : candidates) {
            try (Settings s = Settings._default()) {
                s.set("compression_codec", "\"" + lit + "\"");
                return lit;   // accepted
            } catch (Exception e) { /* try next */ }
        }
        return null;
    }

    /** Build a DB with a given codec literal (null → leave default/off). Highly compressible values. */
    static void writeRun(ObjectStore store, String dbName, String codecLit, int from, int to) throws Exception {
        Settings s = Settings._default();
        if (codecLit != null) s.set("compression_codec", "\"" + codecLit + "\"");
        s.set("l0_sst_size_bytes", "8192");     // small L0 so writes flush to SSTs promptly
        DbBuilder builder = new DbBuilder(dbName, store);
        builder.withSettings(s);
        Db db = await(builder.build());
        // very repetitive payload → compresses hugely if a codec is active
        String payload = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".repeat(16);
        for (int i = from; i < to; i++) await(db.put(b("k" + i), b(payload + i)));
        await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));
        await(db.shutdown()); db.close(); s.close();
    }

    static int readCheck(ObjectStore store, String dbName, String codecLit, int from, int to, String tag) throws Exception {
        Settings s = Settings._default();
        if (codecLit != null) s.set("compression_codec", "\"" + codecLit + "\"");
        DbBuilder builder = new DbBuilder(dbName, store);
        builder.withSettings(s);
        Db db = await(builder.build());
        int bad = 0;
        String payload = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".repeat(16);
        for (int i = from; i < to; i++) {
            String got = s(await(db.get(b("k" + i))));
            if (!(payload + i).equals(got)) bad++;
        }
        await(db.shutdown()); db.close(); s.close();
        return bad;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== SlateDB SST COMPRESSION on the object store (§9) ===");
        System.out.println("Runtime Java: " + System.getProperty("java.version"));

        Path work = Files.createTempDirectory("flink-slatedb-compress-");

        try (ObjectStore store = ObjectStore.resolve("file:///")) {
            // ---- Q1a: which JSON literal does the setter accept? (serde PascalCase vs FromStr lowercase) ----
            String zstdLit   = probeCodecLiteral("Zstd", "zstd", "ZSTD");
            String snappyLit = probeCodecLiteral("Snappy", "snappy", "SNAPPY");
            check("codec literal accepted by Settings.set(\"compression_codec\", ...)",
                    zstdLit != null && snappyLit != null,
                    "zstd=" + zstdLit + " snappy=" + snappyLit);

            // default: is compression on or off out of the box?
            String defaultJson = Settings._default().toJsonString();
            boolean defaultOff = defaultJson.contains("\"compression_codec\":null")
                    || defaultJson.contains("\"compression_codec\": null");
            check("Q1: default compression_codec is null (OFF unless you set it)", defaultOff,
                    "settings JSON contains compression_codec:null = " + defaultOff);

            // ---- Q1b: does a codec actually shrink the bytes written to the store? ----
            String dbOff  = work.resolve("db-off").toString().substring(1);
            String dbComp = work.resolve("db-comp").toString().substring(1);
            writeRun(store, dbOff,  null,     0, 150);   // compression OFF
            writeRun(store, dbComp, zstdLit,  0, 150);   // compression ON (zstd)
            long offBytes  = sstBytes(Path.of("/" + dbOff));
            long compBytes = sstBytes(Path.of("/" + dbComp));
            check("Q1: SST bytes on the store shrink with a codec set (compressed on 'S3')",
                    compBytes > 0 && compBytes < offBytes,
                    "uncompressed=" + offBytes + "B  zstd=" + compBytes + "B  ratio="
                            + (offBytes == 0 ? "n/a" : String.format("%.2fx", (double) offBytes / compBytes)));

            // ---- Q2: does switching the codec break already-written SSTs? ----
            String dbMix = work.resolve("db-mix").toString().substring(1);
            writeRun(store, dbMix, zstdLit,   0,   100);   // batch A written under Zstd
            int badAfterSnappy = readCheck(store, dbMix, snappyLit, 0, 100, "reopen-as-snappy");
            check("Q2: SSTs written under Zstd still readable after switching writer to Snappy",
                    badAfterSnappy == 0, "mismatches=" + badAfterSnappy);

            writeRun(store, dbMix, snappyLit, 100, 200);   // batch B appended under Snappy
            int badMixedOff = readCheck(store, dbMix, null, 0, 200, "reopen-as-off");
            check("Q2: mixed-codec SSTs (Zstd batch + Snappy batch) all readable after switching to OFF",
                    badMixedOff == 0, "mismatches across 200 keys=" + badMixedOff);

        } catch (UnsatisfiedLinkError | NoClassDefFoundError e) {
            System.out.println("  [SKIP] native/class load failed — need JDK 22+ + -Djava.library.path. " + e);
            System.exit(2);
        }

        System.out.println();
        System.out.println(ok ? "COMPRESSION E2E PASSED ✅ (codec is per-SST + self-describing; switching it never breaks old files; default is OFF)"
                              : "COMPRESSION E2E FAILED ❌");
        if (!ok) System.exit(1);
    }
}
