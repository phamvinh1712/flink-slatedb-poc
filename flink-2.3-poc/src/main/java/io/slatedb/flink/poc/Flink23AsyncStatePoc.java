package io.slatedb.flink.poc;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verifies the FLINK 2.3 -side claims of the SlateDB-in-Flink design doc, on an embedded
 * MiniCluster. Prints PASS/FAIL and exits non-zero on any failure.
 *
 * Design points verified:
 *   §3    KeyGroupRange reconstruction via getTaskInfo() (same routing foundation as 1.20).
 *   §0    Flink 2.3: open(OpenContext) is the signature (NOT open(Configuration) as in 1.20).
 *   §0    Flink 2.3: parallelism getters are REMOVED from RuntimeContext — getTaskInfo() is the ONLY path.
 *         (We verify this by using getTaskInfo(); the fact the deprecated getters are gone is a compile-time
 *          property — see the comment in the mapper.)
 *   §0    Async state API EXISTS in 2.3: KeyedStream.enableAsyncState() compiles & runs. This is the
 *         capability absent in 1.20 (§15.2) and the reason ForSt is 2.3's preferred disaggregation path.
 */
public final class Flink23AsyncStatePoc {

    private static final List<String> FAILURES = new ArrayList<>();

    private static void check(String name, boolean cond, String detail) {
        System.out.println((cond ? "  [PASS] " : "  [FAIL] ") + name + (detail.isEmpty() ? "" : " — " + detail));
        if (!cond) FAILURES.add(name);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== Flink 2.3 design-verification PoC ===");
        System.out.println("Flink version: " + org.apache.flink.runtime.util.EnvironmentInformation.getVersion());

        checkKeyGroupAssignment();                 // §3/§13.5 — pure
        checkOpenContextAndRangeReconstruction();  // §3/§0 — MiniCluster, open(OpenContext)
        checkEnableAsyncStateRuns();               // §0/§15.2 — the 2.x-only async capability

        System.out.println();
        if (FAILURES.isEmpty()) {
            System.out.println("ALL CHECKS PASSED ✅");
        } else {
            System.out.println("FAILURES ❌: " + FAILURES);
            System.exit(1);
        }
    }

    /** §3/§13.5 — same deterministic-partition invariant as 1.20; must hold identically on 2.3. */
    private static void checkKeyGroupAssignment() {
        System.out.println("\n[Group A] Key-group assignment (pure, §3/§13.5)");
        final int maxP = 128, par = 4;

        boolean ownership = true;
        for (int i = 0; i < 5000; i++) {
            String key = "k-" + i;
            int kg = KeyGroupRangeAssignment.assignToKeyGroup(key, maxP);
            int subtask = KeyGroupRangeAssignment.assignKeyToParallelOperator(key, maxP, par);
            KeyGroupRange owned = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(maxP, par, subtask);
            if (!owned.contains(kg)) ownership = false;
        }
        check("every key's keyGroup ∈ its routed subtask's range (§13.5)", ownership, "");

        int covered = 0, prevEnd = -1; boolean contiguous = true;
        for (int s = 0; s < par; s++) {
            KeyGroupRange r = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(maxP, par, s);
            if (r.getStartKeyGroup() != prevEnd + 1) contiguous = false;
            prevEnd = r.getEndKeyGroup();
            covered += r.getEndKeyGroup() - r.getStartKeyGroup() + 1;
        }
        check("subtask ranges tile [0,maxP) contiguously", contiguous && covered == maxP, "covered=" + covered);
    }

    /** §3/§0 — open(OpenContext) signature (2.3) + KeyGroupRange reconstruction via getTaskInfo(). */
    private static void checkOpenContextAndRangeReconstruction() throws Exception {
        System.out.println("\n[Group B] open(OpenContext) + KeyGroupRange reconstruction (§3/§0)");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.setMaxParallelism(64);

        List<String> collected = new ArrayList<>();
        try (var it = env.fromSequence(0, 999).map(Long::intValue).returns(Integer.class).setParallelism(1)
                .keyBy(i -> i % 50)
                .map(new RangeReportingMapper())
                .executeAndCollect()) {
            int guard = 0;
            while (guard++ < 2000) {
                try { if (!it.hasNext()) break; collected.add(it.next()); }
                catch (IllegalStateException endOfJob) { break; }  // MiniCluster torn down after delivery
            }
        }

        check("open(OpenContext) invoked (2.3 signature)",
                collected.stream().anyMatch(s -> s.contains("open(OpenContext)=OK")), "");
        check("KeyGroupRange reconstructed via getTaskInfo() (§3)",
                collected.stream().anyMatch(s -> s.contains("taskInfoRange=OK")), "");
        check("all reconstructed ranges non-empty & valid",
                collected.stream().noneMatch(s -> s.contains("EMPTY_RANGE")), "");
    }

    /**
     * §0/§15.2 — The capability that DOES NOT EXIST in 1.20: enableAsyncState().
     *
     * Two verified facts (both impossible to demonstrate on 1.20):
     *   (1) KeyedStream.enableAsyncState() COMPILES — the API is present in 2.3 (this file won't
     *       compile against flink-streaming-java 1.20).
     *   (2) The runtime ENFORCES async-state-awareness: chaining a plain (non-async) operator after
     *       enableAsyncState() throws UnsupportedOperationException("...does not support async state...").
     *       This proves the async-state subsystem is real and wired — you cannot use async state with an
     *       arbitrary operator; you need an async-state-aware operator (or, in practice, ForSt). This is
     *       exactly the §13.6 / §15.2 point: the async path is not free-form, which is why the DIY SlateDB
     *       cold-read path (a plain KeyedProcessFunction) cannot simply "use async state" on 2.3 either.
     */
    private static void checkEnableAsyncStateRuns() throws Exception {
        System.out.println("\n[Group C] Async state opt-in exists & is enforced (§0/§15.2 — 2.x only)");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.setMaxParallelism(32);

        // (1) THE 2.x-ONLY CALL. This line does not compile against Flink 1.20 → API presence proven by compilation.
        KeyedStream<Integer, Integer> asyncKeyed =
                env.fromSequence(0, 199).map(Long::intValue).returns(Integer.class).setParallelism(1)
                   .keyBy(i -> i % 10)
                   .enableAsyncState();
        check("KeyedStream.enableAsyncState() exists (compiles; absent in 1.20)", true, "compile-time proof");

        // (2) Runtime enforces async-state-awareness: a plain map after enableAsyncState() must be rejected.
        boolean enforced = false;
        String msg = "";
        try {
            try (var it = asyncKeyed.map(i -> i).executeAndCollect()) {
                int guard = 0;
                while (guard++ < 500) {
                    try { if (!it.hasNext()) break; it.next(); }
                    catch (IllegalStateException endOfJob) { break; }
                }
            }
        } catch (UnsupportedOperationException expected) {
            enforced = true;
            msg = expected.getMessage() == null ? "" : expected.getMessage().replaceAll("\\s+", " ").trim();
            if (msg.length() > 80) msg = msg.substring(0, 80) + "…";
        }
        check("runtime enforces async-state-aware operators (rejects plain map) (§13.6/§15.2)", enforced, msg);
    }

    /** Keyed mapper reconstructing its KeyGroupRange in open(OpenContext) — the 2.3 signature. */
    public static final class RangeReportingMapper extends RichMapFunction<Integer, String> {
        private transient String report;
        private final AtomicInteger seen = new AtomicInteger();

        @Override
        public void open(OpenContext openContext) {  // §0: 2.3 uses open(OpenContext), NOT open(Configuration)
            var ti = getRuntimeContext().getTaskInfo();   // §0: getTaskInfo() is the ONLY parallelism path in 2.x
            int maxP = ti.getMaxNumberOfParallelSubtasks();
            int par  = ti.getNumberOfParallelSubtasks();
            int idx  = ti.getIndexOfThisSubtask();
            // NOTE: getRuntimeContext().getNumberOfParallelSubtasks() would NOT COMPILE on 2.3 —
            // the direct getters were removed (§0). That's a compile-time proof; we rely on getTaskInfo() here.
            KeyGroupRange r = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(maxP, par, idx);
            String state = r.getNumberOfKeyGroups() > 0 ? "taskInfoRange=OK" : "EMPTY_RANGE";
            this.report = String.format("subtask=%d range=[%d,%d] open(OpenContext)=OK %s",
                    idx, r.getStartKeyGroup(), r.getEndKeyGroup(), state);
        }

        @Override
        public String map(Integer value) {
            return seen.getAndIncrement() == 0 ? report : "";
        }
    }
}
