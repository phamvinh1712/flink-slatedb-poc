package io.slatedb.flink.poc;

import io.slatedb.uniffi.*;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.DataStream;
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

/**
 * THE fusion test I'd flagged as inferred-not-verified: a REAL Flink savepoint → change parallelism →
 * restore cycle, where the SlateDB per-subtask state is rescaled via RFC-0004 projection/union DRIVEN
 * BY the savepoint's redistributed operator state. Runs on a real MiniCluster (Flink 1.20.1, JDK 25).
 *
 * Design (one-DB-per-subtask, SlateDB external to Flink keyed state):
 *   - Keyed counting operator: per-key RMW counter in SlateDB, keyed by §6.1 key-group prefix.
 *   - Each subtask owns a KeyGroupRange (computed from maxParallelism, currentParallelism, subtaskIndex).
 *   - CheckpointedFunction stores, in UNION list state, a handle per subtask: "dbPath|slateCpId|kgLo|kgHi".
 *     Union list state is the primitive Flink redistributes on rescale (every subtask gets the FULL list).
 *   - On restore at a NEW parallelism, each new subtask: reads all old handles, computes its own new
 *     KeyGroupRange, and UNIONS the old DBs whose ranges intersect it (projecting each to the overlap) —
 *     RFC-0004 union+projection, keyed off the savepoint state. That is the fusion.
 *
 * Cycle: Run1 @ P=2 → stopWithSavepoint → Run2 @ P=4 restored from savepoint (upscale) →
 *        stopWithSavepoint → Run3 @ P=1 (downscale) → assert every key counted exactly-once end-to-end.
 *
 * Requires JDK 22+ (--add-opens for Flink on JDK 25), -Djava.library.path=native.
 */
public final class FlinkRescaleSavepointE2E {

    static final int MAXP = 128;                 // maxParallelism (immutable key-group count)
    static final int KEYS = 24;                  // distinct keys "k0".."k23"
    static final int ROUNDS_PER_RUN = 3;         // each run replays the full key set this many times
    static String WORK;                          // absolute work dir (shared across in-JVM runs)

    static <T> T await(CompletableFuture<T> f) {
        try { return f.get(120, TimeUnit.SECONDS); } catch (Exception e) { throw new RuntimeException(e); }
    }
    static byte[] bytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }
    static int kgOf(String key) { return KeyGroupRangeAssignment.assignToKeyGroup(key, MAXP); }
    static byte[] slateKey(String key) {                 // §6.1: [keyGroup hi][keyGroup lo][key], big-endian (§18.3)
        int kg = kgOf(key);
        byte[] kb = bytes(key);
        byte[] out = new byte[2 + kb.length];
        out[0] = (byte) ((kg >>> 8) & 0xFF);             // BIG-ENDIAN key-group prefix (§18.3)
        out[1] = (byte) (kg & 0xFF);
        System.arraycopy(kb, 0, out, 2, kb.length);
        return out;
    }
    // KeyRange over a contiguous key-group span [lo, hi] (inclusive), matching the big-endian 2-byte prefix.
    static KeyRange kgRange(int lo, int hi) {
        byte[] start = new byte[]{ (byte) ((lo >>> 8) & 0xFF), (byte) (lo & 0xFF) };
        // end-exclusive at (hi+1)<<0 prefix; hi+1 may be up to MAXP (128) which fits in 2 bytes.
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
        System.out.println("=== FLINK SAVEPOINT → RESCALE → RESTORE fused with SlateDB projection/union (§6.4/§16.17) ===");
        System.out.println("Runtime Java: " + System.getProperty("java.version"));

        WORK = Files.createTempDirectory("flink-rescale-sp-").toString();
        Path spDir = Files.createDirectories(Path.of(WORK, "savepoints"));

        MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
                .setNumTaskManagers(1)
                .setNumSlotsPerTaskManager(4)     // enough slots for P=4
                .build();

        try (MiniCluster mc = new MiniCluster(cfg)) {
            mc.start();

            // ---- Run 1 @ P=2 (fresh) ----
            String sp1 = runAndSavepoint(mc, 2, null, spDir.toString(), "run1(P=2, fresh)");
            // ---- Run 2 @ P=4 restored from sp1 (UPSCALE via projection/union) ----
            String sp2 = runAndSavepoint(mc, 4, sp1, spDir.toString(), "run2(P=4, upscale)");
            // ---- Run 3 @ P=1 restored from sp2 (DOWNSCALE via union) ----
            String sp3 = runAndSavepoint(mc, 1, sp2, spDir.toString(), "run3(P=1, downscale)");

            // ---- Verify: total observed increments per key == 3 runs × 3 rounds == 9 ----
            // Each run replays k0..k23 ROUNDS_PER_RUN times; state carries across savepoint+rescale.
            // Read final counts from the live DBs recorded by run3.
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
            check("EXACTLY-ONCE across savepoint→upscale→downscale: every key counted " + expectedPerKey,
                    missing == 0 && wrong == 0, "missing=" + missing + " wrong=" + wrong);

            mc.close();
        } catch (UnsatisfiedLinkError | NoClassDefFoundError e) {
            System.out.println("  [SKIP] native/class load failed — need JDK 22+ + -Djava.library.path. " + e);
            System.exit(2);
        }

        System.out.println();
        System.out.println(ok ? "RESCALE-SAVEPOINT E2E PASSED ✅ (real Flink savepoint+rescale fused with SlateDB union/projection; exactly-once)"
                              : "RESCALE-SAVEPOINT E2E FAILED ❌");
        if (!ok) System.exit(1);
    }

    /** Build+submit a job at parallelism P (optionally restored from savepointPath), run it to completion
     *  of the bounded source, then stop-with-savepoint and return the new savepoint path. */
    static String runAndSavepoint(MiniCluster mc, int parallelism, String restoreFrom,
                                  String spDir, String label) throws Exception {
        System.out.println("---- " + label + " ----");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.setMaxParallelism(MAXP);
        env.enableCheckpointing(300);

        DataStream<String> src = env.addSource(new BoundedKeySource(KEYS, ROUNDS_PER_RUN));
        src.keyBy(v -> v)
           .process(new RescalableCountingFn(WORK))
           .name("slatedb-counter").uid("slatedb-counter")   // stable uid so state maps across rescale
           .addSink(new org.apache.flink.streaming.api.functions.sink.DiscardingSink<>());

        JobGraph jg = env.getStreamGraph().getJobGraph();
        if (restoreFrom != null) {
            jg.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(restoreFrom, false));
        }
        JobID jobId = mc.submitJob(jg).get(60, TimeUnit.SECONDS).getJobID();

        // Wait until the bounded source has drained (job transitions toward FINISHED) — but we stop it
        // with a savepoint while RUNNING to capture state, since a FINISHED job can't be savepointed.
        // Poll until all subtasks have processed their input, then stop-with-savepoint.
        Thread.sleep(4000);   // let the bounded source fully emit + checkpoint at least once
        String sp = mc.stopWithSavepoint(jobId, spDir, true, SavepointFormatType.CANONICAL)
                .get(90, TimeUnit.SECONDS);
        System.out.println("  savepoint @ " + shortName(sp));
        return sp;
    }

    /** Read the counts from whatever DBs run3 left live (recorded in WORK/live-dbs.txt). */
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
                        if (v != null) {
                            long c = Long.parseLong(new String(v, StandardCharsets.UTF_8));
                            // MAX per key across all recorded generations: counts are monotonic (RMW increments),
                            // and a key's true final count lives in whichever DB currently owns it (run-3's DB);
                            // stale generations hold only lower/equal counts for that key.
                            counts.merge("k" + i, c, Math::max);
                        }
                    }
                } finally { await(db.shutdown()); db.close(); }
            }
        }
        return counts;
    }

    // ---- bounded source: emits k0..k(KEYS-1), ROUNDS times, throttled so a checkpoint lands mid-run ----
    public static final class BoundedKeySource implements SourceFunction<String> {
        private final int keys, rounds;
        private volatile boolean running = true;
        BoundedKeySource(int keys, int rounds) { this.keys = keys; this.rounds = rounds; }
        @Override public void run(SourceContext<String> ctx) throws Exception {
            for (int r = 0; r < rounds && running; r++) {
                for (int i = 0; i < keys && running; i++) {
                    synchronized (ctx.getCheckpointLock()) { ctx.collect("k" + i); }
                    Thread.sleep(8);   // throttle → checkpoints land during the run
                }
            }
            // keep the source alive briefly so stop-with-savepoint catches it RUNNING (not FINISHED)
            while (running) Thread.sleep(50);
        }
        @Override public void cancel() { running = false; }
    }

    // ---- the rescalable keyed counter: SlateDB per subtask, union/projection on restore ----
    public static final class RescalableCountingFn extends KeyedProcessFunction<String, String, String>
            implements CheckpointedFunction {

        private final String workDir;
        private transient ObjectStore store;
        private transient Db db;
        private transient Admin admin;
        private transient String dbName;                 // this subtask's live DB (abs path, no leading '/')
        private transient int kgLo, kgHi;                 // this subtask's key-group range
        private transient ListState<String> handleState; // UNION list: "dbPath|cpId|kgLo|kgHi" per subtask
        private transient List<String> restoredHandles;

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
                restoredHandles = new ArrayList<>();
                for (String h : handleState.get()) restoredHandles.add(h);
                // Find the old shards whose key-group range intersects MY new range → union+project them.
                List<CloneSourceSpec> sources = new ArrayList<>();
                List<String> chosen = new ArrayList<>();
                for (String h : restoredHandles) {
                    String[] parts = h.split("\\|");
                    String pDb = parts[0], pCp = parts[1];
                    int pLo = Integer.parseInt(parts[2]), pHi = Integer.parseInt(parts[3]);
                    int lo = Math.max(kgLo, pLo), hi = Math.min(kgHi, pHi);
                    if (lo <= hi) {   // intersects
                        // project each source to the OVERLAP with my range (per-source KeyRange on the spec)
                        sources.add(new CloneSourceSpec(pDb, pCp, kgRange(lo, hi)));
                        chosen.add(shortName(pDb) + "[" + pLo + "-" + pHi + "]∩[" + lo + "-" + hi + "]");
                    }
                }
                System.out.println("  [restore] p=" + p + " s=" + idx + " kg[" + kgLo + "-" + kgHi + "] "
                        + "← union " + chosen);
                if (sources.isEmpty()) {
                    // no overlap (shouldn't happen given ranges cover [0,MAXP)) → fresh db
                    db = await(new DbBuilder(dbName, store).build());
                } else {
                    Admin a = new AdminBuilder(sources.get(0).path(), store).build();
                    CloneBuilder cb = a.createCloneBuilderFromSource(sources.get(0));
                    for (int i = 1; i < sources.size(); i++) cb.withSource(sources.get(i));   // UNION (appends)
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
            // §6.3 + union rule: MEMTABLE flush so all data is in L0/manifest (union can't merge WAL), then cp.
            await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));
            CheckpointCreateResult cp = await(admin.createDetachedCheckpoint(new CheckpointOptions(null, null, null)));
            // Each subtask contributes ITS handle; union list state gathers all of them for the next restore.
            handleState.clear();
            handleState.add(dbName + "|" + cp.id() + "|" + kgLo + "|" + kgHi);
            // record live db (append) so the post-job reader can find the final generation
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
        }
    }
}
