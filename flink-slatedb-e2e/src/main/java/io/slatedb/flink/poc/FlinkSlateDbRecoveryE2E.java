package io.slatedb.flink.poc;

import io.slatedb.uniffi.*;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * THE HIGH-FIDELITY TEST — REAL Flink checkpointing + an INDUCED failure + genuine recovery, asserting
 * exactly-once SlateDB keyed state across a Flink restart. Closes the last gap noted in §16.5.
 *
 * Unlike FlinkSlateDbE2E (which triggered a checkpoint from a sentinel event), this uses Flink's actual
 * checkpoint-coordinator lifecycle:
 *   - env.enableCheckpointing(): Flink drives periodic global checkpoints.
 *   - CheckpointedFunction.snapshotState(): §6.3 — MEMTABLE flush + createDetachedCheckpoint, and store the
 *     SlateDB checkpoint id in Flink OPERATOR STATE (so it rides inside the Flink checkpoint).
 *   - An induced RuntimeException mid-stream (attempt 0 only) forces Flink to restart the job.
 *   - CheckpointedFunction.initializeState() with isRestored()==true: §6.6 — CLONE the SlateDB db from the
 *     stored checkpoint id (NOT a writer reopen), into a new generation path, and write there. This discards
 *     post-checkpoint writes; Flink replays the post-checkpoint records from the (checkpointed) source.
 *
 * EXACTLY-ONCE ASSERTION (timing-independent): each input key appears a known number of times; after
 * recovery the final SlateDB count per key MUST equal its input frequency. Too high = double-count
 * (at-least-once, i.e. clone-restore failed to roll back). Too low = data loss.
 *
 * Requires JDK 22+ (SlateDB FFM). Flink 1.20.1 runs on JDK 25 (§16.3). Parallelism=1 (one SlateDB, no rescale).
 */
public final class FlinkSlateDbRecoveryE2E {

    static final int MAX_PARALLELISM = 128;
    static final int KEYS = 6;
    static final int PER_KEY = 10;                 // each key appears 10× → expected final count = 10
    static final int FAIL_AT = 30;                 // induce failure after this many records (attempt 0)

    // Shared across the in-JVM restart (MiniCluster keeps statics):
    static final AtomicBoolean induced = new AtomicBoolean(false);   // fail exactly once
    static volatile boolean aFlinkCheckpointCompleted = false;       // only fail after a checkpoint completed
    static volatile boolean sawRestore = false;                      // proof recovery actually happened

    private static <T> T await(CompletableFuture<T> f) {
        try { return f.get(60, TimeUnit.SECONDS); } catch (Exception e) { throw new RuntimeException(e); }
    }
    private static byte[] bytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    static byte[] slateKey(String key) {   // §6.1 key-group prefix (1 byte since maxP<=128)
        int kg = KeyGroupRangeAssignment.assignToKeyGroup(key, MAX_PARALLELISM);
        byte[] k = bytes(key);
        byte[] out = new byte[1 + k.length];
        out[0] = (byte) kg;
        System.arraycopy(k, 0, out, 1, k.length);
        return out;
    }

    private static boolean ok = true;
    private static void check(String name, boolean cond, String detail) {
        System.out.println((cond ? "  [PASS] " : "  [FAIL] ") + name + (detail.isEmpty() ? "" : " — " + detail));
        if (!cond) ok = false;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== RECOVERY E2E: real Flink checkpoint + induced failure + clone-restore exactly-once ===");
        System.out.println("Runtime Java: " + System.getProperty("java.version")
                + " | Flink: " + org.apache.flink.runtime.util.EnvironmentInformation.getVersion());

        Path work = Files.createTempDirectory("flink-slatedb-recovery-");
        Path currentDbFile = work.resolve("current-db.txt");   // operator records its live db path here

        // Build the replayable input: KEYS keys, each PER_KEY times, interleaved so a checkpoint lands mid-stream.
        List<String> input = new ArrayList<>();
        for (int r = 0; r < PER_KEY; r++)
            for (int k = 0; k < KEYS; k++)
                input.add("key-" + k);   // 60 records total

        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            env.setMaxParallelism(MAX_PARALLELISM);
            env.enableCheckpointing(250);   // Flink drives real periodic checkpoints
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.milliseconds(300)));

            env.addSource(new ThrottledReplayableSource(input))
               .returns(Types.STRING)
               .keyBy(s -> s)
               .process(new SlateDbCountingRmwFn(work.toString(), currentDbFile.toString()))
               .print();

            env.execute("flink-slatedb-recovery-e2e");   // runs, fails once, RECOVERS, completes

            // ---- After recovery+completion: read the final live SlateDB state and assert exactly-once ----
            check("recovery actually happened (initializeState saw isRestored=true)", sawRestore, "");

            String finalDbName = Files.readString(currentDbFile).trim();
            Map<String, Long> counts = readCounts(finalDbName);
            System.out.println("  final counts = " + counts);

            boolean allExact = true;
            for (int k = 0; k < KEYS; k++) {
                long c = counts.getOrDefault("key-" + k, -1L);
                if (c != PER_KEY) allExact = false;
            }
            check("EXACTLY-ONCE: every key counted exactly " + PER_KEY + " across the failure (§6.6)",
                    allExact, "counts=" + counts + " (any >"+PER_KEY+" = double-count/at-least-once; <"+PER_KEY+" = loss)");

        } catch (UnsatisfiedLinkError | NoClassDefFoundError e) {
            System.out.println("  [SKIP] SlateDB native lib/class load failed — need JDK 22+ + -Djava.library.path. " + e);
            System.exit(2);
        }

        System.out.println();
        System.out.println(ok ? "RECOVERY E2E PASSED ✅ (real Flink recovery preserved exactly-once SlateDB state)"
                              : "RECOVERY E2E FAILED ❌");
        if (!ok) System.exit(1);
    }

    /** Open a db read-only-ish (as writer, no new writes) and read the KEYS counters. */
    private static Map<String, Long> readCounts(String dbName) throws Exception {
        Map<String, Long> counts = new TreeMap<>();
        try (ObjectStore store = ObjectStore.resolve("file:///");
             DbBuilder b = new DbBuilder(dbName, store)) {
            Db db = await(b.build());
            try (db) {
                for (int k = 0; k < KEYS; k++) {
                    byte[] v = await(db.get(slateKey("key-" + k)));
                    if (v != null) counts.put("key-" + k, Long.parseLong(new String(v, StandardCharsets.UTF_8)));
                }
                await(db.shutdown());
            }
        }
        return counts;
    }

    /**
     * A throttled, REPLAYABLE source: emits records slowly (so Flink checkpoint barriers interleave with
     * processing) and checkpoints its offset. On recovery it resumes from the last checkpointed offset —
     * this is what lets the post-checkpoint records be re-processed exactly once. (A plain fromData source
     * emits everything instantly, so no checkpoint captures mid-stream state — that was the first bug.)
     */
    public static final class ThrottledReplayableSource
            implements org.apache.flink.streaming.api.functions.source.SourceFunction<String>,
                       CheckpointedFunction {
        private final List<String> data;
        private volatile boolean running = true;
        private int offset = 0;                          // next index to emit
        private transient ListState<Integer> offsetState;

        ThrottledReplayableSource(List<String> data) { this.data = data; }

        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            offsetState = ctx.getOperatorStateStore()
                    .getListState(new ListStateDescriptor<>("src-offset", Types.INT));
            if (ctx.isRestored()) {
                for (Integer o : offsetState.get()) offset = o;   // resume from checkpointed offset
                System.out.println("  [source] restored → resuming at offset " + offset);
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
            offsetState.clear();
            offsetState.add(offset);
        }

        @Override
        public void run(SourceContext<String> out) throws Exception {
            while (running && offset < data.size()) {
                synchronized (out.getCheckpointLock()) {     // emit + advance offset atomically wrt checkpoints
                    if (offset >= data.size()) break;
                    out.collect(data.get(offset));
                    offset++;
                }
                Thread.sleep(50);                             // throttle so barriers interleave
            }
        }

        @Override
        public void cancel() { running = false; }
    }

    /**
     * Real keyed operator: per-key RMW counter in SlateDB. Implements CheckpointedFunction to (a) create a
     * SlateDB detached checkpoint in snapshotState and store its id in Flink operator state, and (b) clone
     * from that id on restore in initializeState. SlateDB db path is generational per task attempt.
     */
    public static final class SlateDbCountingRmwFn extends KeyedProcessFunction<String, String, String>
            implements CheckpointedFunction, org.apache.flink.api.common.state.CheckpointListener {

        private final String workDir;       // absolute work dir
        private final String currentDbFile;
        private transient ObjectStore store;
        private transient Db db;
        private transient Admin admin;
        private transient String dbName;     // absolute path (no leading '/'), current generation
        private transient ListState<String> handleState;  // stores "dbName|slateCpId" of the last checkpoint
        private transient int processed;

        SlateDbCountingRmwFn(String workDir, String currentDbFile) {
            this.workDir = workDir;
            this.currentDbFile = currentDbFile;
        }

        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            int attempt = getRuntimeContext().getTaskInfo().getAttemptNumber();
            // Union list state so the (single) subtask gets the full stored handle list.
            handleState = ctx.getOperatorStateStore()
                    .getUnionListState(new ListStateDescriptor<>("slatedb-handle", Types.STRING));

            String genPath = (workDir + "/db-gen-" + attempt).substring(1);   // strip leading '/'
            store = ObjectStore.resolve("file:///");

            if (ctx.isRestored()) {
                // §6.6 RESTORE: clone from the pinned checkpoint stored at the last completed Flink checkpoint.
                String handle = handleState.get().iterator().next();  // "parentDbName|slateCpId"
                String parentDbName = handle.substring(0, handle.lastIndexOf('|'));
                String slateCpId = handle.substring(handle.lastIndexOf('|') + 1);
                sawRestore = true;
                System.out.println("  [restore] attempt=" + attempt + " cloning parent=" + shortName(parentDbName)
                        + " @cp=" + slateCpId + " → " + shortName(genPath));
                try (Admin a = new AdminBuilder(parentDbName, store).build()) {
                    CloneSourceSpec src = new CloneSourceSpec(parentDbName, slateCpId, null);
                    CloneBuilder cb = a.createCloneBuilderFromSource(src);
                    cb.withClonePath(genPath);
                    cb.withObjectStore(store);
                    await(cb.build());
                }
            } else {
                System.out.println("  [fresh] attempt=" + attempt + " new db " + shortName(genPath));
            }

            dbName = genPath;
            db = await(new DbBuilder(dbName, store).build());
            admin = new AdminBuilder(dbName, store).build();
            Files.writeString(Path.of(currentDbFile), dbName);   // record live db for post-job read
        }

        @Override
        public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
            // §6.3: MEMTABLE flush (NOT plain flush(); §16.2) so the manifest captures data, then checkpoint.
            await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));
            CheckpointCreateResult cp = await(admin.createDetachedCheckpoint(new CheckpointOptions(null, null, null)));
            handleState.clear();
            handleState.add(dbName + "|" + cp.id());   // rides inside the Flink checkpoint
            System.out.println("  [snapshot] flinkCp=" + ctx.getCheckpointId() + " processed=" + processed
                    + " slateCp=" + cp.id());
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            aFlinkCheckpointCompleted = true;   // gate the induced failure on a real completed checkpoint
            System.out.println("  [cp-complete] flinkCp=" + checkpointId + " → failure gate armed");
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            int attempt = getRuntimeContext().getTaskInfo().getAttemptNumber();

            // Induce ONE failure, only on attempt 0, only after a Flink checkpoint has completed.
            if (attempt == 0 && aFlinkCheckpointCompleted && processed >= FAIL_AT && induced.compareAndSet(false, true)) {
                System.out.println("  [induce] throwing after " + processed + " records (attempt 0) to force recovery");
                throw new RuntimeException("INDUCED FAILURE for recovery test");
            }

            // §6.2 per-key RMW — safe: Flink runs one element at a time per key.
            byte[] k = slateKey(value);
            byte[] cur = await(db.get(k));
            long n = (cur == null ? 0L : Long.parseLong(new String(cur, StandardCharsets.UTF_8))) + 1;
            await(db.put(k, bytes(Long.toString(n))));
            processed++;
            out.collect(value + "=" + n);
        }

        @Override
        public void close() throws Exception {
            if (db != null) {
                try { await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE))); } catch (Throwable ignored) {}
                try { await(db.shutdown()); } catch (Throwable ignored) {}
                try { db.close(); } catch (Throwable ignored) {}
            }
            if (admin != null) try { admin.close(); } catch (Throwable ignored) {}
            if (store != null) try { store.close(); } catch (Throwable ignored) {}
        }

        private static String shortName(String p) { int i = p.lastIndexOf('/'); return i < 0 ? p : p.substring(i + 1); }
    }
}
