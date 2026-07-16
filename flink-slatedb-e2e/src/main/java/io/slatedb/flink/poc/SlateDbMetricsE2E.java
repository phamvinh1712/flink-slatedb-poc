package io.slatedb.flink.poc;

import io.slatedb.uniffi.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * OBSERVABILITY (§18.5 / RFC 0021) — verify SlateDB exposes metrics via a bound MetricsRecorder,
 * and discover the REAL metric names (the doc otherwise quotes RFC 0021, unverified).
 *
 * Installs a DefaultMetricsRecorder on the DB (withMetricsRecorder), does representative work
 * (puts, gets/hits+misses, memtable flush, checkpoint, some compaction), then dumps the recorder's
 * snapshot() — the full list of metrics actually captured, with names, labels, and values.
 *
 * Verifies:
 *   A. metrics ARE collected once a recorder is wired (snapshot non-empty) — default is no-op.
 *   B. the risk-relevant metric families exist (some form of: L0 count, cache hit/miss, mem size,
 *      compaction, request latency). Asserted loosely by name-substring since exact names are the
 *      thing we're discovering — the PRINTED snapshot is the authoritative catalog.
 * Requires JDK 22+, -Djava.library.path=native.
 */
public final class SlateDbMetricsE2E {

    static <T> T await(CompletableFuture<T> f) {
        try { return f.get(60, TimeUnit.SECONDS); } catch (Exception e) { throw new RuntimeException(e); }
    }
    static byte[] b(String s) { return s.getBytes(StandardCharsets.UTF_8); }
    static boolean ok = true;
    static void check(String n, boolean c, String d) {
        System.out.println((c ? "  [PASS] " : "  [FAIL] ") + n + (d.isEmpty() ? "" : " — " + d));
        if (!c) ok = false;
    }
    static String valStr(MetricValue v) {   // UniFFI names the tuple field v1()
        if (v instanceof MetricValue.Counter c) return "counter=" + c.v1();
        if (v instanceof MetricValue.Gauge g) return "gauge=" + g.v1();
        if (v instanceof MetricValue.UpDownCounter u) return "updown=" + u.v1();
        if (v instanceof MetricValue.Histogram h) {
            HistogramMetricValue hv = h.v1();
            return "hist(count=" + hv.count() + " sum=" + hv.sum() + " min=" + hv.min() + " max=" + hv.max() + ")";
        }
        return "?";
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== SlateDB METRICS via DefaultMetricsRecorder (§18.5 / RFC 0021) ===");
        System.out.println("Runtime Java: " + System.getProperty("java.version"));

        Path work = Files.createTempDirectory("flink-slatedb-metrics-");
        String dbName = work.resolve("db").toString().substring(1);

        try (ObjectStore store = ObjectStore.resolve("file:///")) {
            DefaultMetricsRecorder rec = new DefaultMetricsRecorder();

            Settings s = Settings._default();
            s.set("l0_sst_size_bytes", "4096");                    // small L0 → force flushes/compaction
            s.set("compactor_options.poll_interval", "\"200ms\"");
            DbBuilder builder = new DbBuilder(dbName, store);
            builder.withSettings(s);
            builder.withMetricsRecorder(rec);                      // ← wire the recorder (default is no-op)
            Db db = await(builder.build());
            Admin admin = new AdminBuilder(dbName, store).build();

            // representative work: writes, flushes (→ L0 + compaction), gets (hits + a miss), checkpoint
            for (int round = 0; round < 8; round++) {
                for (int i = 0; i < 200; i++) await(db.put(b("k" + (round * 200 + i)), b("v" + i)));
                await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));
            }
            for (int i = 0; i < 500; i++) await(db.get(b("k" + i)));          // hits
            for (int i = 0; i < 100; i++) await(db.get(b("absent-" + i)));     // misses
            await(admin.createDetachedCheckpoint(new CheckpointOptions(null, null, null)));
            Thread.sleep(3000);   // let compaction + metrics settle

            // dump the full snapshot — the authoritative metric catalog
            List<Metric> snap = new ArrayList<>(rec.snapshot());   // snapshot() is immutable → copy to sort
            snap.sort(Comparator.comparing(Metric::name));
            // group by name (many share a name with different labels) → print each unique name once,
            // with its instrument type, label keys, and one sample value.
            LinkedHashMap<String, List<Metric>> byName = new LinkedHashMap<>();
            for (Metric m : snap) byName.computeIfAbsent(m.name(), k -> new ArrayList<>()).add(m);
            System.out.println("\n  ---- SlateDB metric catalog (" + byName.size() + " distinct names, "
                    + snap.size() + " series) ----");
            for (var e : byName.entrySet()) {
                Metric m0 = e.getValue().get(0);
                Set<String> lkeys = new TreeSet<>();
                for (Metric m : e.getValue()) for (MetricLabel l : m.labels()) lkeys.add(l.key());
                String labelInfo = lkeys.isEmpty() ? "" : "  labels=" + lkeys + " (" + e.getValue().size() + " series)";
                System.out.println("    " + e.getKey() + "  [" + valStr(m0.value()) + "]" + labelInfo);
            }
            System.out.println("  --------------------------------------------\n");

            // A. metrics collected at all
            check("A. recorder captured metrics (snapshot non-empty; default recorder is no-op)",
                    !snap.isEmpty(), "count=" + snap.size());

            // B. the risk-relevant families exist (loose substring match on the discovered names)
            Set<String> names = new HashSet<>();
            for (Metric m : snap) names.add(m.name().toLowerCase());
            java.util.function.Predicate<String> any = sub ->
                    names.stream().anyMatch(nm -> nm.contains(sub));
            check("B. L0 / SST-count metric present", any.test("l0") || any.test("sst"), "");
            check("B. cache metric present", any.test("cache"), "");
            check("B. memory/size metric present", any.test("mem") || any.test("size") || any.test("bytes"), "");
            check("B. compaction metric present", any.test("compact"), "");

            await(db.shutdown()); db.close(); admin.close(); s.close(); rec.close();

        } catch (UnsatisfiedLinkError | NoClassDefFoundError e) {
            System.out.println("  [SKIP] native/class load failed — need JDK 22+ + -Djava.library.path. " + e);
            System.exit(2);
        }

        System.out.println(ok ? "METRICS E2E PASSED ✅ (DefaultMetricsRecorder captures SlateDB metrics; catalog printed above)"
                              : "METRICS E2E FAILED ❌");
        if (!ok) System.exit(1);
    }
}
