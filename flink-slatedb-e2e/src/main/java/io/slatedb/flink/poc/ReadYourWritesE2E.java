package io.slatedb.flink.poc;

import io.slatedb.uniffi.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Verifies READ-YOUR-WRITES consistency in a single Db instance BEFORE any flush.
 * No flushWithOptions / no checkpoint anywhere — everything stays in the mutable memtable (and WAL buffer).
 * Store = memory:/// so there is no disk/S3 that could "hide" unflushed data; the only way a read can
 * see the write is if the memtable read path works.
 *
 * Cases:
 *   A. put then get (never flushed)                → sees the value
 *   B. overwrite then get                          → sees the LATEST value (MVCC newest-seq wins)
 *   C. delete then get                             → sees null (tombstone honored pre-flush)
 *   D. interleaved RMW loop (get→+1→put) x N       → final count exact (no lost/stale reads)
 *   E. many keys written then all read back        → every key consistent, no flush
 *   F. scan over unflushed range                   → returns the written rows in order
 *   G. cross-check: is await_durable even needed for RYW? do it with await_durable=false too
 */
public final class ReadYourWritesE2E {
    static <T> T await(CompletableFuture<T> f) {
        try { return f.get(60, TimeUnit.SECONDS); } catch (Exception e) { throw new RuntimeException(e); }
    }
    static byte[] b(String s) { return s.getBytes(StandardCharsets.UTF_8); }
    static String s(byte[] x) { return x == null ? null : new String(x, StandardCharsets.UTF_8); }

    static boolean ok = true;
    static void check(String name, boolean cond, String detail) {
        System.out.println((cond ? "  [PASS] " : "  [FAIL] ") + name + (detail.isEmpty() ? "" : " — " + detail));
        if (!cond) ok = false;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== READ-YOUR-WRITES before flush (single Db, memory:///, NO flush anywhere) ===");
        System.out.println("Runtime Java: " + System.getProperty("java.version"));

        try (ObjectStore store = ObjectStore.resolve("memory:///")) {
            Db db = await(new DbBuilder("ryw", store).build());

            // A. put then get, never flushed
            await(db.put(b("a"), b("1")));
            check("A. put→get (unflushed) sees value", "1".equals(s(await(db.get(b("a"))))),
                    "got=" + s(await(db.get(b("a")))));

            // B. overwrite then get → latest wins
            await(db.put(b("a"), b("2")));
            await(db.put(b("a"), b("3")));
            check("B. overwrite→get sees LATEST", "3".equals(s(await(db.get(b("a"))))),
                    "got=" + s(await(db.get(b("a")))));

            // C. delete then get → null (tombstone visible pre-flush)
            await(db.put(b("d"), b("x")));
            check("C0. pre-delete visible", "x".equals(s(await(db.get(b("d"))))), "");
            await(db.delete(b("d")));
            check("C. delete→get sees null (tombstone pre-flush)", await(db.get(b("d"))) == null,
                    "got=" + s(await(db.get(b("d")))));

            // D. interleaved read-modify-write loop, never flushed
            await(db.put(b("cnt"), b("0")));
            int N = 500;
            for (int i = 0; i < N; i++) {
                int cur = Integer.parseInt(s(await(db.get(b("cnt")))));  // read own write each iteration
                await(db.put(b("cnt"), b(Integer.toString(cur + 1))));
            }
            int finalCnt = Integer.parseInt(s(await(db.get(b("cnt")))));
            check("D. RMW loop x" + N + " reads own writes (final=" + N + ")", finalCnt == N,
                    "final=" + finalCnt);

            // E. many keys, write all then read all — no flush
            int M = 2000;
            for (int i = 0; i < M; i++) await(db.put(b("k" + i), b("v" + i)));
            int wrong = 0, missing = 0;
            for (int i = 0; i < M; i++) {
                String got = s(await(db.get(b("k" + i))));
                if (got == null) missing++;
                else if (!("v" + i).equals(got)) wrong++;
            }
            check("E. " + M + " unflushed keys all consistent", missing == 0 && wrong == 0,
                    "missing=" + missing + " wrong=" + wrong);

            // F. scan over unflushed range k100..k110
            await(db.put(b("range-1"), b("A")));
            await(db.put(b("range-2"), b("B")));
            await(db.put(b("range-3"), b("C")));
            DbIterator it = await(db.scan(new KeyRange(b("range-1"), true, b("range-3"), true)));
            List<String> scanned = new ArrayList<>();
            for (KeyValue kv = await(it.next()); kv != null; kv = await(it.next()))
                scanned.add(s(kv.key()) + "=" + s(kv.value()));
            check("F. scan over unflushed range returns rows in order",
                    scanned.equals(List.of("range-1=A", "range-2=B", "range-3=C")),
                    "scanned=" + scanned);

            await(db.shutdown()); db.close();

            // G. same, but writes with await_durable=false (write returns before WAL durable) — RYW still hold?
            Db db2 = await(new DbBuilder("ryw2", store).build());
            WriteOptions noDurable = new WriteOptions(false);
            PutOptions defPut = new PutOptions(new Ttl.Default());   // use DB default TTL
            await(db2.putWithOptions(b("nd"), b("42"), defPut, noDurable));
            check("G. await_durable=false: read-your-write still consistent", "42".equals(s(await(db2.get(b("nd"))))),
                    "got=" + s(await(db2.get(b("nd")))));
            await(db2.shutdown()); db2.close();
        }

        System.out.println();
        System.out.println(ok ? "READ-YOUR-WRITES PASSED ✅ (consistent reads before any flush)"
                              : "READ-YOUR-WRITES FAILED ❌");
        if (!ok) System.exit(1);
    }
}
