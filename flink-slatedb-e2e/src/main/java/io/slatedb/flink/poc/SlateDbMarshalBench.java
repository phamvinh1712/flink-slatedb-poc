package io.slatedb.flink.poc;

import io.slatedb.uniffi.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Microbenchmark: separate the TWO components of SlateDB Rust↔Java per-op cost:
 *   (A) async round-trip LATENCY  — blocking await(get) one at a time (serial)
 *   (B) marshalling THROUGHPUT    — fire N gets concurrently, await all (pipelined)
 * If (B) ≫ (A), the bottleneck is round-trip latency (Tokio hop + callback), NOT the byte copies.
 *
 * Store = memory:/// (no S3/disk); pre-loaded keys → all reads are block-cache HITS. So any cost
 * here is FFI + RustBuffer copies + async plumbing, never storage.
 */
public final class SlateDbMarshalBench {
    static <T> T await(CompletableFuture<T> f) {
        try { return f.get(120, TimeUnit.SECONDS); } catch (Exception e) { throw new RuntimeException(e); }
    }
    static byte[] key(int i) { return ("key-" + i).getBytes(StandardCharsets.UTF_8); }
    static byte[] val(int size) { byte[] v = new byte[size]; Arrays.fill(v, (byte) 'x'); return v; }

    static final int KEYSPACE = 2000;
    static final int WARMUP   = 2_000;
    static final int MEASURE  = 20_000;
    static final int PIPELINE = 256;   // concurrent in-flight gets for the throughput test

    public static void main(String[] args) throws Exception {
        System.out.println("Runtime Java: " + System.getProperty("java.version"));
        System.out.println("Store: memory:/// (isolates FFI+copy+async, no S3/disk)\n");

        int[] valueSizes = { 16, 256, 1024, 4096, 16384 };

        try (ObjectStore store = ObjectStore.resolve("memory:///")) {
            Db db = await(new DbBuilder("bench", store).build());

            System.out.printf("%-9s | %-26s | %-30s | %-22s%n",
                    "valSize", "serial GET (latency)", "pipelined GET (throughput)", "batched PUT (100)");
            System.out.println("-".repeat(96));

            for (int vsize : valueSizes) {
                byte[] value = val(vsize);
                for (int i = 0; i < KEYSPACE; i++) await(db.put(key(i), value));
                await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));
                for (int i = 0; i < KEYSPACE; i++) await(db.get(key(i)));   // warm cache

                // ---- (A) serial GET: one await at a time ----
                for (int i = 0; i < WARMUP; i++) await(db.get(key(i % KEYSPACE)));
                long t0 = System.nanoTime();
                long checksum = 0;
                for (int i = 0; i < MEASURE; i++) checksum += await(db.get(key(i % KEYSPACE))).length;
                long serialNs = System.nanoTime() - t0;

                // ---- (B) pipelined GET: PIPELINE in flight, await batch ----
                for (int w = 0; w < WARMUP / PIPELINE; w++) {
                    List<CompletableFuture<byte[]>> fs = new ArrayList<>(PIPELINE);
                    for (int j = 0; j < PIPELINE; j++) fs.add(db.get(key(j % KEYSPACE)));
                    for (var f : fs) await(f);
                }
                t0 = System.nanoTime();
                int rounds = MEASURE / PIPELINE;
                for (int r = 0; r < rounds; r++) {
                    List<CompletableFuture<byte[]>> fs = new ArrayList<>(PIPELINE);
                    for (int j = 0; j < PIPELINE; j++) fs.add(db.get(key((r * PIPELINE + j) % KEYSPACE)));
                    for (var f : fs) checksum += await(f).length;
                }
                long pipeNs = System.nanoTime() - t0;
                long pipeOps = (long) rounds * PIPELINE;

                // ---- batched PUT ----
                int batchRows = 100, batches = MEASURE / batchRows;
                for (int w = 0; w < WARMUP / batchRows; w++) {
                    WriteBatch wb = new WriteBatch();
                    for (int j = 0; j < batchRows; j++) wb.put(key(j), value);
                    await(db.write(wb)); wb.close();
                }
                t0 = System.nanoTime();
                for (int b2 = 0; b2 < batches; b2++) {
                    WriteBatch wb = new WriteBatch();
                    for (int j = 0; j < batchRows; j++) wb.put(key(j), value);
                    await(db.write(wb)); wb.close();
                }
                long batchNs = System.nanoTime() - t0;

                double serialUs = serialNs / (double) MEASURE / 1000.0;
                double serialOps = MEASURE / (serialNs / 1e9);
                double pipeUs = pipeNs / (double) pipeOps / 1000.0;
                double pipeOpsS = pipeOps / (pipeNs / 1e9);
                double batchUs = batchNs / (double) (batches * batchRows) / 1000.0;
                double batchOpsS = (batches * batchRows) / (batchNs / 1e9);

                System.out.printf("%-9d | %6.2f µs (%,8.0f/s) | %6.2f µs/op (%,10.0f/s) | %6.2f µs (%,8.0f/s)%n",
                        vsize, serialUs, serialOps, pipeUs, pipeOpsS, batchUs, batchOpsS);
                if (checksum < 0) System.out.println(checksum);
            }
            await(db.shutdown()); db.close();
        }
        System.out.println("\nAll on memory:/// → FFI + marshalling + async only, NOT storage latency.");
        System.out.println("Serial = per-call round-trip cost; Pipelined = marshalling throughput ceiling.");
        System.out.println("On real S3 a cache-MISS get is ~1-10 ms, dwarfing all of the above.");
    }
}
