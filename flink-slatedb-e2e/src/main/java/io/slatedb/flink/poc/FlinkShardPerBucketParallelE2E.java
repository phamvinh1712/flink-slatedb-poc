package io.slatedb.flink.poc;

import io.slatedb.uniffi.*;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * GENUINELY PARALLEL + SHARD-PER-BUCKET (§4 / §4.1 / §12.7) — the big untested gap.
 *
 * A real Flink job at setParallelism(PARALLELISM) where the keyed operator uses the SHARD-PER-BUCKET layout:
 *   - A fixed number of SHARDS (SHARDS ≥ maxParallelism-ish) each = a bucket of key groups, one SlateDB per shard.
 *   - Each SUBTASK opens the shards whose key groups it owns (its KeyGroupRange) → N/P SlateDB instances per subtask.
 *   - All shards on a subtask SHARE ONE DbCache (§12.7 — verified collision-safe by scope_id; here exercised live).
 *   - Multiple subtasks run CONCURRENTLY in one JVM (one shared UniFFI Tokio runtime, §4.1).
 *
 * Verifies together (all previously untested):
 *   parallelism > 1 with embedded SlateDB (concurrent Db instances in one process)
 *   §4/§4.1 shard-per-bucket layout: subtask owns N/P shards; a key routes to the shard for its key group
 *   §12.7 one shared DbCache across many Db instances — no cross-instance collision (values stay correct)
 *   §13.5 cross-subtask ownership: every key handled by the subtask whose KeyGroupRange owns its key group
 * Assertion: after the job, the union of all shard DBs contains every key exactly once with the correct count.
 *
 * Requires JDK 22+ (SlateDB FFM); Flink 1.20 on JDK 25 (§16.3).
 */
public final class FlinkShardPerBucketParallelE2E {

    static final int PARALLELISM = 4;
    static final int MAXP = 128;          // key groups
    static final int SHARDS = 16;         // fixed physical shards; each = MAXP/SHARDS = 8 key groups
    static final int KG_PER_SHARD = MAXP / SHARDS;
    static final int NUM_KEYS = 800;
    static final int REPEATS = 3;         // each key appears 3× → expected count 3

    static String workAbs;

    private static <T> T await(CompletableFuture<T> f) {
        try { return f.get(60, TimeUnit.SECONDS); } catch (Exception e) { throw new RuntimeException(e); }
    }
    private static byte[] bytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }
    static int kgOf(String key) { return KeyGroupRangeAssignment.assignToKeyGroup(key, MAXP); }
    static int shardOf(String key) { return kgOf(key) / KG_PER_SHARD; }        // key → physical shard
    static String shardDbName(int shard) { return workAbs + "/shard-" + shard; }
    static byte[] shardKey(String key) {   // key group prefix within the shard's keyspace
        byte[] k = bytes(key);
        byte[] out = new byte[1 + k.length];
        out[0] = (byte) kgOf(key);
        System.arraycopy(k, 0, out, 1, k.length);
        return out;
    }

    private static boolean ok = true;
    private static void check(String name, boolean cond, String detail) {
        System.out.println((cond ? "  [PASS] " : "  [FAIL] ") + name + (detail.isEmpty() ? "" : " — " + detail));
        if (!cond) ok = false;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== PARALLEL SHARD-PER-BUCKET E2E: P=" + PARALLELISM + ", " + SHARDS
                + " shards, embedded SlateDB per subtask, shared cache (§4/§4.1/§12.7/§13.5) ===");
        System.out.println("Runtime Java: " + System.getProperty("java.version")
                + " | Flink: " + org.apache.flink.runtime.util.EnvironmentInformation.getVersion());

        Path work = Files.createTempDirectory("flink-shard-parallel-");
        workAbs = work.toString().substring(1);

        // Build input: NUM_KEYS keys, each REPEATS times, shuffled deterministically.
        List<String> input = new ArrayList<>();
        for (int r = 0; r < REPEATS; r++)
            for (int i = 0; i < NUM_KEYS; i++) input.add("user-" + i);
        Collections.shuffle(input, new Random(42));

        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(PARALLELISM);
            env.setMaxParallelism(MAXP);

            env.fromData(input)
               .keyBy(s -> s)
               .process(new ShardedCountingFn())
               .print();
            env.execute("flink-shard-per-bucket-parallel");

            // ---- Verify: read ALL shard DBs, reassemble state, assert exact counts + no dup + ownership ----
            Map<String, Long> merged = new HashMap<>();
            Map<String, Integer> keyShardCount = new HashMap<>();
            try (ObjectStore store = ObjectStore.resolve("file:///")) {
                for (int shard = 0; shard < SHARDS; shard++) {
                    String name = shardDbName(shard);
                    if (!Files.exists(Path.of("/" + name))) continue;   // shard never created (no keys) — ok
                    try (DbBuilder b = new DbBuilder(name, store)) {
                        Db db = await(b.build());
                        try (db) {
                            DbIterator it = await(db.scan(new KeyRange(null, false, null, false)));
                            for (KeyValue kv = await(it.next()); kv != null; kv = await(it.next())) {
                                String key = new String(kv.key(), 1, kv.key().length - 1, StandardCharsets.UTF_8);
                                long v = Long.parseLong(new String(kv.value(), StandardCharsets.UTF_8));
                                merged.merge(key, v, Long::sum);
                                keyShardCount.merge(key, 1, Integer::sum);
                                // ownership: this key's shard must equal the shard file we're reading
                                if (shardOf(key) != shard) ok = false;
                            }
                            it.close();
                            await(db.shutdown());
                        }
                    }
                }
            }

            boolean noDup = keyShardCount.values().stream().allMatch(c -> c == 1);
            boolean noLoss = merged.size() == NUM_KEYS;
            boolean exactCounts = merged.values().stream().allMatch(v -> v == REPEATS);

            check("parallel job ran at parallelism " + PARALLELISM + " with embedded SlateDB", true, "");
            check("§4 shard-per-bucket: no key in >1 shard (clean partition)", noDup, "");
            check("§13.5 ownership: every key in the shard owning its key group", ok, "");
            check("no loss: all " + NUM_KEYS + " keys present across shards", noLoss, "have " + merged.size());
            check("exact counts: every key counted exactly " + REPEATS + " (concurrent RMW correct)",
                    exactCounts, exactCounts ? "" : "bad=" + merged.entrySet().stream()
                            .filter(e -> e.getValue() != REPEATS).limit(5).toList());
            System.out.println("  (subtasks that ran: " + ShardedCountingFn.subtasksSeen
                    + " ; peak concurrent open Db instances across JVM: " + ShardedCountingFn.peakOpenDbs + ")");
            check("§4.1 multiple subtasks each opened N/P shards concurrently (>1 subtask, >1 Db)",
                    ShardedCountingFn.subtasksSeen.size() > 1 && ShardedCountingFn.peakOpenDbs > 1,
                    "subtasks=" + ShardedCountingFn.subtasksSeen + " peakDbs=" + ShardedCountingFn.peakOpenDbs);

        } catch (UnsatisfiedLinkError | NoClassDefFoundError e) {
            System.out.println("  [SKIP] SlateDB native lib/class load failed — need JDK 22+ + -Djava.library.path. " + e);
            System.exit(2);
        }

        System.out.println();
        System.out.println(ok ? "PARALLEL SHARD-PER-BUCKET E2E PASSED ✅ (P>1 embedded SlateDB, shard-per-bucket, shared cache, exact)"
                              : "PARALLEL SHARD-PER-BUCKET E2E FAILED ❌");
        if (!ok) System.exit(1);
    }

    /**
     * Keyed operator using shard-per-bucket: opens the shards this subtask owns (by KeyGroupRange), all
     * sharing ONE DbCache (§12.7). Routes each key to its shard's Db and does per-key RMW.
     */
    public static final class ShardedCountingFn extends KeyedProcessFunction<String, String, String> {
        // JVM-wide diagnostics (MiniCluster shares statics across subtask threads).
        static final Set<Integer> subtasksSeen = java.util.concurrent.ConcurrentHashMap.newKeySet();
        static final java.util.concurrent.atomic.AtomicInteger openDbs = new java.util.concurrent.atomic.AtomicInteger();
        static volatile int peakOpenDbs = 0;

        private transient ObjectStore store;
        private transient DbCache sharedCache;       // §12.7 ONE cache shared by all shards on this subtask
        private transient Admin[] admins;
        private transient Map<Integer, Db> shardDbs; // shard id → Db (only the shards this subtask owns)

        @Override
        public void open(OpenContext ctx) throws Exception {
            var ti = getRuntimeContext().getTaskInfo();
            int subtask = ti.getIndexOfThisSubtask();
            subtasksSeen.add(subtask);
            KeyGroupRange myRange = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                    MAXP, ti.getNumberOfParallelSubtasks(), subtask);

            store = ObjectStore.resolve("file:///");
            // §12.7: one shared in-memory cache for ALL shards this subtask opens (moka, 64MB).
            sharedCache = DbCache.newMokaCache(new MokaCacheOptions(64L * 1024 * 1024, null, null));

            shardDbs = new HashMap<>();
            // Determine which shards this subtask owns: a shard is owned if ANY of its key groups ∈ myRange.
            List<Integer> ownedShards = new ArrayList<>();
            for (int shard = 0; shard < SHARDS; shard++) {
                int shardKgLo = shard * KG_PER_SHARD, shardKgHi = shardKgLo + KG_PER_SHARD - 1;
                boolean owned = shardKgHi >= myRange.getStartKeyGroup() && shardKgLo <= myRange.getEndKeyGroup();
                if (owned) ownedShards.add(shard);
            }
            for (int shard : ownedShards) {
                DbBuilder b = new DbBuilder(shardDbName(shard), store);
                b.withDbCache(sharedCache);          // SHARE the cache across instances
                Db d = await(b.build());
                shardDbs.put(shard, d);
                int now = openDbs.incrementAndGet();
                synchronized (ShardedCountingFn.class) { peakOpenDbs = Math.max(peakOpenDbs, now); }
            }
            System.out.println("  [open] subtask " + subtask + " range=[" + myRange.getStartKeyGroup() + ","
                    + myRange.getEndKeyGroup() + "] owns shards " + ownedShards + " (shared cache)");
        }

        @Override
        public void processElement(String key, Context ctx, Collector<String> out) throws Exception {
            Db d = shardDbs.get(shardOf(key));
            if (d == null) { ok = false; out.collect("ERR no shard for " + key); return; }
            byte[] k = shardKey(key);
            byte[] cur = await(d.get(k));
            long n = (cur == null ? 0L : Long.parseLong(new String(cur, StandardCharsets.UTF_8))) + 1;
            await(d.put(k, bytes(Long.toString(n))));
        }

        @Override
        public void close() throws Exception {
            if (shardDbs != null) for (Db d : shardDbs.values()) {
                try { await(d.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE))); } catch (Throwable ignored) {}
                try { await(d.shutdown()); } catch (Throwable ignored) {}
                try { d.close(); } catch (Throwable ignored) {}
                openDbs.decrementAndGet();
            }
            if (sharedCache != null) try { sharedCache.close(); } catch (Throwable ignored) {}
            if (store != null) try { store.close(); } catch (Throwable ignored) {}
        }
    }
}
