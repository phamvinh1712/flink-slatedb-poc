package io.slatedb.flink.poc;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verifies the FLINK 1.20 -side claims of the SlateDB-in-Flink design doc, on an embedded
 * MiniCluster (no cluster, no Docker). Each check prints PASS/FAIL and the program exits
 * non-zero if any fails, so `mvn exec:java` is a real gate.
 *
 * Design points verified here:
 *   §3   KeyGroupRange is reconstructable from RuntimeContext (deterministic key routing) — the
 *        foundation of the shard/rescale design. Also confirms assignToKeyGroup is deterministic.
 *   §0   Flink 1.20: open(Configuration) is the signature; RuntimeContext parallelism getters
 *        are PRESENT (deprecated) AND getTaskInfo() works — both compile & run.
 *   §3   Every key routes into exactly the KeyGroupRange its subtask owns (no cross-subtask leak) —
 *        this is what guarantees hot/cold tiers agree on ownership (§13.5).
 */
public final class FlinkKeyedStatePoc {

    // Collects failures across checks.
    private static final List<String> FAILURES = new ArrayList<>();

    private static void check(String name, boolean cond, String detail) {
        System.out.println((cond ? "  [PASS] " : "  [FAIL] ") + name + (detail.isEmpty() ? "" : " — " + detail));
        if (!cond) FAILURES.add(name);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== Flink 1.20 design-verification PoC ===");
        System.out.println("Flink version: " + org.apache.flink.runtime.util.EnvironmentInformation.getVersion());

        checkKeyGroupAssignmentIsDeterministicAndPartitioned();  // §3, §13.5 — pure, no cluster
        checkKeyGroupRangeReconstructionInOperator();            // §3, §0 — runs on MiniCluster

        System.out.println();
        if (FAILURES.isEmpty()) {
            System.out.println("ALL CHECKS PASSED ✅");
        } else {
            System.out.println("FAILURES ❌: " + FAILURES);
            System.exit(1);
        }
    }

    /**
     * §3 / §13.5 — The key→keyGroup→subtask assignment must be (a) deterministic and
     * (b) a clean partition: every key group maps to exactly one subtask, and the subtask's
     * KeyGroupRange contains exactly the key groups assigned to it. This is the invariant the
     * whole shard-per-bucket / hybrid-tier ownership story depends on.
     */
    private static void checkKeyGroupAssignmentIsDeterministicAndPartitioned() {
        System.out.println("\n[Group A] Key-group assignment (pure, §3/§13.5)");

        final int maxParallelism = 128;   // == number of key groups
        final int parallelism = 4;

        // (a) deterministic: same key → same key group, twice.
        boolean deterministic = true;
        for (String k : new String[]{"user-1", "user-2", "abc", "", "🔑", "a-very-long-key-value-1234567890"}) {
            int g1 = KeyGroupRangeAssignment.assignToKeyGroup(k, maxParallelism);
            int g2 = KeyGroupRangeAssignment.assignToKeyGroup(k, maxParallelism);
            if (g1 != g2 || g1 < 0 || g1 >= maxParallelism) deterministic = false;
        }
        check("assignToKeyGroup is deterministic & in [0,maxP)", deterministic, "");

        // (b) the P subtask ranges tile [0, maxParallelism) with no gaps or overlaps.
        int covered = 0;
        int prevEnd = -1;
        boolean contiguous = true;
        for (int subtask = 0; subtask < parallelism; subtask++) {
            KeyGroupRange r = KeyGroupRangeAssignment
                    .computeKeyGroupRangeForOperatorIndex(maxParallelism, parallelism, subtask);
            if (r.getStartKeyGroup() != prevEnd + 1) contiguous = false;
            prevEnd = r.getEndKeyGroup();
            covered += (r.getEndKeyGroup() - r.getStartKeyGroup() + 1);
        }
        check("subtask ranges tile [0,maxP) contiguously", contiguous && prevEnd == maxParallelism - 1,
                "covered=" + covered + " lastEnd=" + prevEnd);
        check("total key groups covered == maxParallelism", covered == maxParallelism, covered + " vs " + maxParallelism);

        // (c) each key's key group falls inside the range of the subtask it's routed to.
        boolean ownershipConsistent = true;
        for (int i = 0; i < 5000; i++) {
            String key = "k-" + i;
            int kg = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
            int subtask = KeyGroupRangeAssignment.assignKeyToParallelOperator(key, maxParallelism, parallelism);
            KeyGroupRange owned = KeyGroupRangeAssignment
                    .computeKeyGroupRangeForOperatorIndex(maxParallelism, parallelism, subtask);
            if (!owned.contains(kg)) ownershipConsistent = false;
        }
        check("every key's keyGroup ∈ its routed subtask's range (§13.5 ownership)", ownershipConsistent, "");
    }

    /**
     * §3 / §0 — Inside a running operator on the MiniCluster, reconstruct the subtask's
     * KeyGroupRange exactly as the design proposes, using BOTH the getTaskInfo() path and the
     * deprecated direct RuntimeContext getters (to prove 1.20 supports both). Confirms
     * open(Configuration) is the signature. The reconstructed range must be non-empty and valid.
     */
    private static void checkKeyGroupRangeReconstructionInOperator() throws Exception {
        System.out.println("\n[Group B] KeyGroupRange reconstruction in a live operator (§3/§0)");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.setMaxParallelism(64);   // 64 key groups

        var src = env.fromSequence(0, 999).map(Long::intValue).returns(Integer.class)
                .setParallelism(1);

        // keyBy so the map is a keyed operator with a valid KeyGroupRange per subtask.
        List<String> collected = new ArrayList<>();
        try (var it = src.keyBy(i -> i % 50)
                .map(new RangeReportingMapper())
                .executeAndCollect()) {
            int guard = 0;
            // The collect iterator can throw once the job/MiniCluster finishes; treat as end-of-stream.
            while (guard++ < 2000) {
                try {
                    if (!it.hasNext()) break;
                    collected.add(it.next());
                } catch (IllegalStateException endOfJob) {
                    break;   // MiniCluster shut down after all records delivered — normal termination
                }
            }
        }

        // Every emitted record is a reconstruction report; parse & validate.
        boolean openConfigCalled = collected.stream().anyMatch(s -> s.contains("open(Configuration)=OK"));
        boolean taskInfoPathOk   = collected.stream().anyMatch(s -> s.contains("taskInfoRange=OK"));
        boolean deprecatedPathOk = collected.stream().anyMatch(s -> s.contains("deprecatedGettersMatch=true"));
        boolean rangesNonEmpty   = collected.stream().noneMatch(s -> s.contains("EMPTY_RANGE"));

        check("open(Configuration) invoked (1.20 signature)", openConfigCalled, "");
        check("KeyGroupRange reconstructed via getTaskInfo() (§3)", taskInfoPathOk, "");
        check("deprecated RuntimeContext getters agree with TaskInfo (§0)", deprecatedPathOk, "");
        check("all reconstructed ranges non-empty & valid", rangesNonEmpty, "");
    }

    /** Keyed mapper that reconstructs its KeyGroupRange in open() and reports it once per subtask. */
    public static final class RangeReportingMapper extends RichMapFunction<Integer, String> {
        private transient String report;
        private final AtomicInteger seen = new AtomicInteger();

        @Override
        public void open(Configuration parameters) {  // §0: 1.20 uses open(Configuration)
            var rc = getRuntimeContext();

            // §3 recommended path: getTaskInfo()
            var ti = rc.getTaskInfo();
            int maxP = ti.getMaxNumberOfParallelSubtasks();
            int par  = ti.getNumberOfParallelSubtasks();
            int idx  = ti.getIndexOfThisSubtask();
            KeyGroupRange viaTaskInfo =
                    KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(maxP, par, idx);

            // §0: deprecated-but-present direct getters on RuntimeContext (1.20 only).
            @SuppressWarnings("deprecation")
            int maxP2 = rc.getMaxNumberOfParallelSubtasks();
            @SuppressWarnings("deprecation")
            int par2  = rc.getNumberOfParallelSubtasks();
            @SuppressWarnings("deprecation")
            int idx2  = rc.getIndexOfThisSubtask();
            boolean deprecatedGettersMatch = (maxP2 == maxP && par2 == par && idx2 == idx);

            String rangeState = (viaTaskInfo.getNumberOfKeyGroups() > 0) ? "taskInfoRange=OK" : "EMPTY_RANGE";
            this.report = String.format(
                    "subtask=%d range=[%d,%d] open(Configuration)=OK %s deprecatedGettersMatch=%s",
                    idx, viaTaskInfo.getStartKeyGroup(), viaTaskInfo.getEndKeyGroup(),
                    rangeState, deprecatedGettersMatch);
        }

        @Override
        public String map(Integer value) {
            // Emit the report only once per subtask to keep output small.
            return seen.getAndIncrement() == 0 ? report : "";
        }
    }
}
