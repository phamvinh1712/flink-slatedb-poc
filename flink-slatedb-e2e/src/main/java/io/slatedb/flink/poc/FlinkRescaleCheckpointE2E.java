package io.slatedb.flink.poc;

import io.slatedb.uniffi.*;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * CHECKPOINT (not savepoint) → RESCALE → RESTORE — answer: "can I reshard SlateDB during a
 * checkpoint upgrade, or does it strictly need a savepoint?"
 *
 * Two independent layers:
 *   (1) SlateDB's reshard is RFC-0004 projection/union off SlateDB's OWN checkpoint IDs — it has no
 *       idea whether Flink used a savepoint or a checkpoint. So the question is really about layer (2).
 *   (2) Flink's rescale-on-restore. Historically savepoints were THE rescale primitive, but Flink
 *       also supports restoring from a RETAINED (externalized) checkpoint — and, since the state is
 *       redistributed the same way, at a DIFFERENT parallelism. This test proves that.
 *
 * Method: NO savepoint anywhere. Enable RETAIN_ON_CANCELLATION externalized checkpoints to a dir.
 * Run1 @ P=2 → let a checkpoint complete → cancel → find the retained _metadata → restore Run2 @ P=4
 * from that CHECKPOINT (upscale) → repeat to Run3 @ P=1 (downscale). SlateDB shards union/project on
 * each restore, driven by the union-list operator state carried in the checkpoint. Assert exactly-once.
 *
 * Requires JDK 22+ (--add-opens for Flink on JDK 25), -Djava.library.path=native.
 */
public final class FlinkRescaleCheckpointE2E {

    static final int MAXP = 128;
    static final int KEYS = 24;
    static final int ROUNDS_PER_RUN = 3;
    static String WORK;
    static String CP_DIR;               // externalized-checkpoint storage dir (file://)
    // MiniCluster runs in-process, so subtasks can signal progress via shared counters.
    static final java.util.concurrent.atomic.AtomicInteger DRAINED = new java.util.concurrent.atomic.AtomicInteger(0);
    static final java.util.concurrent.atomic.AtomicInteger PROCESSED = new java.util.concurrent.atomic.AtomicInteger(0);

    static <T> T await(CompletableFuture<T> f) {
        try { return f.get(120, TimeUnit.SECONDS); } catch (Exception e) { throw new RuntimeException(e); }
    }
    static byte[] bytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }
    static int kgOf(String key) { return KeyGroupRangeAssignment.assignToKeyGroup(key, MAXP); }
    static byte[] slateKey(String key) {                 // §6.1: [kg hi][kg lo][key] big-endian (§18.3)
        int kg = kgOf(key);
        byte[] kb = bytes(key);
        byte[] out = new byte[2 + kb.length];
        out[0] = (byte) ((kg >>> 8) & 0xFF);
        out[1] = (byte) (kg & 0xFF);
        System.arraycopy(kb, 0, out, 2, kb.length);
        return out;
    }
    static KeyRange kgRange(int lo, int hi) {
        byte[] start = new byte[]{ (byte) ((lo >>> 8) & 0xFF), (byte) (lo & 0xFF) };
        int end = hi + 1;
        byte[] endB = new byte[]{ (byte) ((end >>> 8) & 0xFF), (byte) (end & 0xFF) };
        return new KeyRange(start, true, endB, false);
    }
    static boolean ok = true;
    static void check(String n, boolean c, String d) {
        System.out.println((c ? "  [PASS] " : "  [FAIL] ") + n + (d.isEmpty() ? "" : " — " + d));
        if (!c) ok = false;
    }
    static String shortName(String p) { int i = p.lastIndexOf('/'); return i < 0 ? p : p.substring(i + 1); }

    public static void main(String[] args) throws Exception {
        System.out.println("=== FLINK RETAINED-CHECKPOINT → RESCALE → RESTORE fused with SlateDB union/projection ===");
        System.out.println("Runtime Java: " + System.getProperty("java.version"));

        WORK = Files.createTempDirectory("flink-rescale-cp-").toString();
        CP_DIR = "file://" + Files.createDirectories(Path.of(WORK, "checkpoints"));

        MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
                .setNumTaskManagers(1)
                .setNumSlotsPerTaskManager(4)
                .build();

        try (MiniCluster mc = new MiniCluster(cfg)) {
            mc.start();

            // Run1 @ P=2 (fresh) → retained checkpoint
            String cp1 = runToRetainedCheckpoint(mc, 2, null, "run1(P=2, fresh)");
            // Run2 @ P=4 restored from cp1 (UPSCALE from a CHECKPOINT, not a savepoint)
            String cp2 = runToRetainedCheckpoint(mc, 4, cp1, "run2(P=4, upscale from checkpoint)");
            // Run3 @ P=1 restored from cp2 (DOWNSCALE from a CHECKPOINT)
            runToRetainedCheckpoint(mc, 1, cp2, "run3(P=1, downscale from checkpoint)");

            int expectedPerKey = 3 * ROUNDS_PER_RUN;   // 9
            Map<String, Long> finalCounts = readFinalCounts();
            int wrong = 0, missing = 0;
            for (int i = 0; i < KEYS; i++) {
                Long c = finalCounts.get("k" + i);
                if (c == null) missing++;
                else if (c != expectedPerKey) wrong++;
            }
            System.out.println("  final counts sample: k0=" + finalCounts.get("k0") + " k7=" + finalCounts.get("k7")
                    + " k23=" + finalCounts.get("k23") + " (expected " + expectedPerKey + " each)");
            check("RESHARD ON CHECKPOINT (no savepoint): exactly-once across upscale+downscale, every key=" + expectedPerKey,
                    missing == 0 && wrong == 0, "missing=" + missing + " wrong=" + wrong);

            mc.close();
        } catch (UnsatisfiedLinkError | NoClassDefFoundError e) {
            System.out.println("  [SKIP] native/class load failed — need JDK 22+ + -Djava.library.path. " + e);
            System.exit(2);
        }

        System.out.println();
        System.out.println(ok ? "RESCALE-CHECKPOINT E2E PASSED ✅ (Flink reshards from a RETAINED CHECKPOINT — savepoint NOT required; SlateDB union/projection drives it)"
                              : "RESCALE-CHECKPOINT E2E FAILED ❌");
        if (!ok) System.exit(1);
    }

    /** Run at parallelism P (optionally restored from a retained checkpoint dir), let ≥1 checkpoint
     *  complete, cancel the job (retaining the checkpoint), then locate + return the newest retained
     *  checkpoint's _metadata path. */
    static String runToRetainedCheckpoint(MiniCluster mc, int parallelism, String restoreFrom, String label)
            throws Exception {
        System.out.println("---- " + label + " ----");
        DRAINED.set(0);   // reset per run; source subtasks increment when their emit loop finishes
        PROCESSED.set(0); // reset per run; counting operator increments per element processed
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.setMaxParallelism(MAXP);
        env.enableCheckpointing(300);
        // fail fast: if restore/clone throws, surface it instead of silently retrying forever.
        env.setRestartStrategy(RestartStrategies.noRestart());
        // RETAIN externalized checkpoints on cancellation → they survive to be restored from.
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage(CP_DIR);

        DataStream<String> src = env.addSource(new BoundedKeySource(KEYS, ROUNDS_PER_RUN));
        src.keyBy(v -> v)
           .process(new RescalableCountingFn(WORK))
           .name("slatedb-counter").uid("slatedb-counter")
           .addSink(new org.apache.flink.streaming.api.functions.sink.DiscardingSink<>());

        JobGraph jg = env.getStreamGraph().getJobGraph();
        if (restoreFrom != null) {
            // restore from a CHECKPOINT dir exactly like a savepoint path; allowNonRestoredState=false
            jg.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(restoreFrom, false));
        }
        JobID jobId = mc.submitJob(jg).get(60, TimeUnit.SECONDS).getJobID();

        Path cpRoot = Path.of(CP_DIR.substring("file://".length()));
        // 1. wait until ALL source subtasks drained AND the counting operator has PROCESSED every element
        //    of this run (KEYS×ROUNDS). This is deterministic — no wall-clock guess — so the state we then
        //    capture reflects a COMPLETE run, not a mid-drain / async-in-flight partial.
        int expectProcessed = KEYS * ROUNDS_PER_RUN;
        long drainDeadline = System.nanoTime() + 90_000L * 1_000_000L;
        while ((DRAINED.get() < parallelism || PROCESSED.get() < expectProcessed)
                && System.nanoTime() < drainDeadline) {
            assertRunning(mc, jobId);
            Thread.sleep(50);
        }
        // 2. all data processed; the NEXT checkpoint barrier is injected strictly after → its state has
        //    every increment. Record the current newest cp, then wait for a strictly-newer one.
        int drainCp = newestCheckpointNum(cpRoot, jobId);
        String meta = awaitNewCheckpoint(mc, cpRoot, jobId, drainCp, 60_000);

        // Cancel — RETAIN_ON_CANCELLATION keeps the checkpoint on disk.
        mc.cancelJob(jobId).get(60, TimeUnit.SECONDS);
        // wait until it's actually gone so the next run's slots are free
        for (int i = 0; i < 100 && mc.getJobStatus(jobId).get(10, TimeUnit.SECONDS) != JobStatus.CANCELED; i++)
            Thread.sleep(100);
        System.out.println("  retained checkpoint @ " + shortName(shortName(meta)));
        return meta;
    }

    /** Fail fast with the real cause if the job died (e.g. restore/clone threw). */
    static void assertRunning(MiniCluster mc, JobID jobId) throws Exception {
        JobStatus st = mc.getJobStatus(jobId).get(10, TimeUnit.SECONDS);
        if (st == JobStatus.FAILED || st == JobStatus.FAILING) {
            mc.requestJobResult(jobId).get(30, TimeUnit.SECONDS).getSerializedThrowable()
                    .ifPresent(t -> { throw new RuntimeException("job FAILED during restore/run",
                            t.deserializeError(FlinkRescaleCheckpointE2E.class.getClassLoader())); });
            throw new IllegalStateException("job entered " + st + " but no throwable available");
        }
    }

    /** Newest chk-N with a committed _metadata for this job (-1 if none yet). */
    static int newestCheckpointNum(Path cpRoot, JobID jobId) throws Exception {
        Path jobDir = cpRoot.resolve(jobId.toString());
        if (!Files.isDirectory(jobDir)) return -1;
        int bestN = -1;
        try (Stream<Path> s = Files.list(jobDir)) {
            for (Path p : (Iterable<Path>) s::iterator) {
                String n = p.getFileName().toString();
                if (n.startsWith("chk-") && Files.exists(p.resolve("_metadata")))
                    bestN = Math.max(bestN, Integer.parseInt(n.substring(4)));
            }
        }
        return bestN;
    }

    /** Wait for a committed checkpoint STRICTLY NEWER than minExclusive (→ taken after the drain). */
    static String awaitNewCheckpoint(MiniCluster mc, Path cpRoot, JobID jobId, int minExclusive, long timeoutMs)
            throws Exception {
        Path jobDir = cpRoot.resolve(jobId.toString());
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        while (System.nanoTime() < deadline) {
            assertRunning(mc, jobId);
            int n = newestCheckpointNum(cpRoot, jobId);
            if (n > minExclusive && n >= 2)
                return jobDir.resolve("chk-" + n).resolve("_metadata").toString();
            Thread.sleep(150);
        }
        throw new IllegalStateException("no checkpoint > chk-" + minExclusive + " appeared for job " + jobId);
    }

    static Map<String, Long> readFinalCounts() throws Exception {
        Map<String, Long> counts = new HashMap<>();
        Path listFile = Path.of(WORK, "live-dbs.txt");
        if (!Files.exists(listFile)) return counts;
        List<String> dbs = new ArrayList<>(new LinkedHashSet<>(Files.readAllLines(listFile)));
        try (ObjectStore store = ObjectStore.resolve("file:///")) {
            for (String dbName : dbs) {
                if (dbName.isBlank()) continue;
                Db db = await(new DbBuilder(dbName, store).build());
                try {
                    for (int i = 0; i < KEYS; i++) {
                        byte[] v = await(db.get(slateKey("k" + i)));
                        if (v != null) counts.merge("k" + i, Long.parseLong(new String(v, StandardCharsets.UTF_8)), Math::max);
                    }
                } finally { await(db.shutdown()); db.close(); }
            }
        }
        return counts;
    }

    public static final class BoundedKeySource implements SourceFunction<String> {
        private final int keys, rounds;
        private volatile boolean running = true;
        BoundedKeySource(int keys, int rounds) { this.keys = keys; this.rounds = rounds; }
        @Override public void run(SourceContext<String> ctx) throws Exception {
            for (int r = 0; r < rounds && running; r++)
                for (int i = 0; i < keys && running; i++) {
                    synchronized (ctx.getCheckpointLock()) { ctx.collect("k" + i); }
                    Thread.sleep(8);
                }
            DRAINED.incrementAndGet();           // signal: this subtask emitted its FULL key set
            while (running) Thread.sleep(50);   // stay alive so a post-drain checkpoint completes before cancel
        }
        @Override public void cancel() { running = false; }
    }

    public static final class RescalableCountingFn extends KeyedProcessFunction<String, String, String>
            implements CheckpointedFunction {
        private final String workDir;
        private transient ObjectStore store;
        private transient Db db;
        private transient Admin admin;
        private transient String dbName;
        private transient int kgLo, kgHi;
        private transient ListState<String> handleState;

        RescalableCountingFn(String workDir) { this.workDir = workDir; }

        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            int idx = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            int p = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
            KeyGroupRange myRange = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(MAXP, p, idx);
            kgLo = myRange.getStartKeyGroup();
            kgHi = myRange.getEndKeyGroup();

            handleState = ctx.getOperatorStateStore()
                    .getUnionListState(new ListStateDescriptor<>("slatedb-handles", Types.STRING));

            store = ObjectStore.resolve("file:///");
            long stamp = System.nanoTime();
            dbName = (workDir + "/db-p" + p + "-s" + idx + "-" + stamp).substring(1);

            if (ctx.isRestored()) {
                List<CloneSourceSpec> sources = new ArrayList<>();
                List<String> chosen = new ArrayList<>();
                for (String h : handleState.get()) {
                    String[] parts = h.split("\\|");
                    String pDb = parts[0], pCp = parts[1];
                    int pLo = Integer.parseInt(parts[2]), pHi = Integer.parseInt(parts[3]);
                    int lo = Math.max(kgLo, pLo), hi = Math.min(kgHi, pHi);
                    if (lo <= hi) {
                        sources.add(new CloneSourceSpec(pDb, pCp, kgRange(lo, hi)));
                        chosen.add(shortName(pDb) + "[" + pLo + "-" + pHi + "]∩[" + lo + "-" + hi + "]");
                    }
                }
                System.out.println("  [restore] p=" + p + " s=" + idx + " kg[" + kgLo + "-" + kgHi + "] ← union " + chosen);
                if (sources.isEmpty()) {
                    db = await(new DbBuilder(dbName, store).build());
                } else {
                    Admin a = new AdminBuilder(sources.get(0).path(), store).build();
                    CloneBuilder cb = a.createCloneBuilderFromSource(sources.get(0));
                    for (int i = 1; i < sources.size(); i++) cb.withSource(sources.get(i));
                    cb.withClonePath(dbName);
                    cb.withObjectStore(store);
                    await(cb.build());
                    a.close();
                    db = await(new DbBuilder(dbName, store).build());
                }
            } else {
                System.out.println("  [fresh] p=" + p + " s=" + idx + " kg[" + kgLo + "-" + kgHi + "]");
                db = await(new DbBuilder(dbName, store).build());
            }
            admin = new AdminBuilder(dbName, store).build();
        }

        @Override
        public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
            await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));
            CheckpointCreateResult cp = await(admin.createDetachedCheckpoint(new CheckpointOptions(null, null, null)));
            handleState.clear();
            handleState.add(dbName + "|" + cp.id() + "|" + kgLo + "|" + kgHi);
            synchronized (RescalableCountingFn.class) {
                Files.write(Path.of(workDir, "live-dbs.txt"),
                        (dbName + System.lineSeparator()).getBytes(StandardCharsets.UTF_8),
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            byte[] k = slateKey(value);
            byte[] cur = await(db.get(k));
            long n = (cur == null ? 0L : Long.parseLong(new String(cur, StandardCharsets.UTF_8))) + 1;
            await(db.put(k, bytes(Long.toString(n))));
            PROCESSED.incrementAndGet();   // signal: this element fully applied (async RMW done)
        }
    }
}
