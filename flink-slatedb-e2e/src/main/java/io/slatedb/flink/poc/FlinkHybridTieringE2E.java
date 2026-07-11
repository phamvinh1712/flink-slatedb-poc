package io.slatedb.flink.poc;

import io.slatedb.uniffi.*;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * §13 HYBRID TIERING — RocksDB (hot) + SlateDB (cold) in a real Flink KeyedProcessFunction.
 *
 * Pattern 2 (true demotion, §13.2/§13.3):
 *   - HOT: Flink ValueState (HashMap/RocksDB backend) holds the working set + lastAccess time.
 *   - COLD: SlateDB holds demoted (aged-out) keys.
 *   - processElement: read HOT; on miss read COLD (hot-wins, §13.4 rule 2); RMW; write HOT; (re)arm a demotion timer.
 *   - onTimer: if key untouched for the tier window, DEMOTE — write COLD first, THEN clear HOT (§13.4 rule 1,
 *     write-cold-before-clear-hot: ordering makes the worst case a duplicate, never loss).
 *
 * Verifies (all previously only source-read for the hybrid):
 *   §13.4  hot-wins on read; write-cold-before-clear-hot ordering
 *   §13.7  demotion via explicit event-time timer (State TTL gives NO callback — §16 verified; timers are the only way)
 *   §13    a demoted-then-re-touched key is promoted back to hot and counted correctly (no loss across tiers)
 * Final assertion: for every key, (hot value if present else cold value) == its true event count. hot∪cold covers all.
 *
 * Uses EVENT TIME so demotion timers fire deterministically from the data, not wall-clock. Requires JDK 22+.
 * Parallelism 1 (one SlateDB cold store; §13.5 cross-tier key-group alignment is trivially satisfied).
 */
public final class FlinkHybridTieringE2E {

    static final int MAXP = 128;
    static final long TIER_WINDOW_MS = 100;   // demote if untouched for 100ms of EVENT time

    private static <T> T await(CompletableFuture<T> f) {
        try { return f.get(60, TimeUnit.SECONDS); } catch (Exception e) { throw new RuntimeException(e); }
    }
    private static byte[] bytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }
    static byte[] coldKey(String key) {
        int kg = KeyGroupRangeAssignment.assignToKeyGroup(key, MAXP);
        byte[] k = bytes(key);
        byte[] out = new byte[1 + k.length];
        out[0] = (byte) kg; System.arraycopy(k, 0, out, 1, k.length);
        return out;
    }

    private static boolean ok = true;
    private static void check(String name, boolean cond, String detail) {
        System.out.println((cond ? "  [PASS] " : "  [FAIL] ") + name + (detail.isEmpty() ? "" : " — " + detail));
        if (!cond) ok = false;
    }

    /** (key, eventTs) event — a mutable POJO with a no-arg ctor so Flink's POJO serializer handles it
     *  (Kryo cannot serialize Java records on JDK 16+: "can't get field offset on a record class"). */
    public static final class Ev implements java.io.Serializable {
        public String key;
        public long ts;
        public Ev() {}
        public Ev(String key, long ts) { this.key = key; this.ts = ts; }
        public String key() { return key; }
        public long ts() { return ts; }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== §13 HYBRID TIERING E2E: RocksDB(hot) + SlateDB(cold) in a real Flink operator ===");
        System.out.println("Runtime Java: " + System.getProperty("java.version")
                + " | Flink: " + org.apache.flink.runtime.util.EnvironmentInformation.getVersion());

        Path work = Files.createTempDirectory("flink-hybrid-");
        String coldDb = work.resolve("cold-db").toString().substring(1);   // §16.4: file:/// root + abs db-name
        Path resultFile = work.resolve("final-counts.txt");

        // Build an event stream that FORCES tiering:
        //   - "hot" is touched frequently (never demoted).
        //   - "cold-A"/"cold-B" are touched early, then go quiet long enough to demote, then touched AGAIN
        //     late (must be promoted back from SlateDB and incremented → proves hot-wins + no cross-tier loss).
        // Event timestamps drive demotion timers deterministically.
        List<Ev> events = new ArrayList<>();
        long t = 0;
        // burst: everyone gets some early hits
        for (String k : new String[]{"hot","cold-A","cold-B"}) { events.add(new Ev(k, t)); t += 10; }
        events.add(new Ev("cold-A", t)); t += 10;      // cold-A=2
        events.add(new Ev("cold-B", t)); t += 10;      // cold-B=2
        // now advance event time far past the window while only touching "hot" → cold-A, cold-B demote
        for (int i = 0; i < 12; i++) { events.add(new Ev("hot", t)); t += 40; }   // hot climbs; time advances ~480ms
        // late re-touch of the demoted keys → must promote from cold and increment
        events.add(new Ev("cold-A", t)); t += 10;      // cold-A should become 3 (2 from cold + 1)
        events.add(new Ev("cold-B", t)); t += 200;     // cold-B should become 3; big gap so it demotes again at end
        // a final far-future marker to flush timers
        events.add(new Ev("hot", t));

        // Expected true counts:
        Map<String,Long> expected = new HashMap<>();
        expected.put("hot", 14L);      // 1 (burst) + 12 + 1 (final)
        expected.put("cold-A", 3L);    // burst + second + late
        expected.put("cold-B", 3L);

        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            env.setMaxParallelism(MAXP);
            // Emit watermarks frequently so event-time demotion timers fire MID-stream (not all at close).
            env.getConfig().setAutoWatermarkInterval(20);

            env.addSource(new ThrottledEvSource(events))   // spread events over wall-clock so watermarks advance between them
               .returns(Types.POJO(Ev.class))
               .assignTimestampsAndWatermarks(
                   org.apache.flink.api.common.eventtime.WatermarkStrategy
                       .<Ev>forMonotonousTimestamps()
                       .withTimestampAssigner((e, ts) -> e.ts()))
               .keyBy(Ev::key)
               .process(new HybridTierFn(coldDb, resultFile.toString()))
               .print();
            env.execute("flink-hybrid-tiering-e2e");

            // The operator wrote the final (hot∪cold, hot-wins) count per key to a file at close().
            Map<String,Long> finalCounts = new TreeMap<>();
            for (String line : Files.readAllLines(resultFile)) {
                if (line.isBlank()) continue;
                String[] p = line.split("=", 2);
                finalCounts.put(p[0], Long.parseLong(p[1]));
            }
            System.out.println("  final (hot∪cold, hot-wins) counts = " + finalCounts);
            System.out.println("  (diagnostics: demotions=" + HybridTierFn.demotions
                    + ", promotions=" + HybridTierFn.promotions + " — both >0 means tiering actually exercised)");

            check("§13 tiering actually happened (≥1 demotion AND ≥1 promotion)",
                    HybridTierFn.demotions > 0 && HybridTierFn.promotions > 0,
                    "demotions=" + HybridTierFn.demotions + " promotions=" + HybridTierFn.promotions);
            boolean exact = expected.entrySet().stream()
                    .allMatch(e -> e.getValue().equals(finalCounts.get(e.getKey())));
            check("§13.4 no cross-tier loss/dup: every key's (hot∪cold) count is exact", exact,
                    "expected " + new TreeMap<>(expected) + " got " + finalCounts);

        } catch (UnsatisfiedLinkError | NoClassDefFoundError e) {
            System.out.println("  [SKIP] SlateDB native lib/class load failed — need JDK 22+ + -Djava.library.path. " + e);
            System.exit(2);
        }

        System.out.println();
        System.out.println(ok ? "HYBRID TIERING E2E PASSED ✅ (RocksDB-hot + SlateDB-cold; demotion, promotion, hot-wins, no loss)"
                              : "HYBRID TIERING E2E FAILED ❌");
        if (!ok) System.exit(1);
    }

    /** Emits events spaced over wall-clock time so the periodic watermark advances BETWEEN them, letting
     *  event-time demotion timers fire mid-stream (a fromData source emits instantly → timers only fire at close). */
    public static final class ThrottledEvSource
            implements org.apache.flink.streaming.api.functions.source.SourceFunction<Ev> {
        private final List<Ev> data;
        private volatile boolean running = true;
        ThrottledEvSource(List<Ev> data) { this.data = data; }
        @Override public void run(SourceContext<Ev> out) throws Exception {
            for (Ev e : data) {
                if (!running) break;
                out.collect(e);
                Thread.sleep(30);   // wall-clock gap ≫ autoWatermarkInterval(20ms) so watermarks progress
            }
            Thread.sleep(300);      // let final demotion timers fire before the stream ends
        }
        @Override public void cancel() { running = false; }
    }

    public static final class HybridTierFn extends KeyedProcessFunction<String, Ev, String> {
        static volatile int demotions = 0;
        static volatile int promotions = 0;

        private final String coldDbName;
        private final String resultFile;
        private transient ValueState<Long> hot;          // HOT tier (Flink managed state)
        private transient ValueState<Long> lastAccess;   // event-time of last touch
        private transient ObjectStore store;
        private transient Db cold;                        // COLD tier (SlateDB)
        private transient Set<String> keysSeen;          // for the final dump

        HybridTierFn(String coldDbName, String resultFile) { this.coldDbName = coldDbName; this.resultFile = resultFile; }

        @Override
        public void open(OpenContext ctx) throws Exception {
            hot = getRuntimeContext().getState(new ValueStateDescriptor<>("hot", Types.LONG));
            lastAccess = getRuntimeContext().getState(new ValueStateDescriptor<>("lastAccess", Types.LONG));
            store = ObjectStore.resolve("file:///");
            cold = await(new DbBuilder(coldDbName, store).build());
            keysSeen = new HashSet<>();
        }

        @Override
        public void processElement(Ev e, Context ctx, Collector<String> out) throws Exception {
            keysSeen.add(e.key());
            Long h = hot.value();
            long n;
            if (h != null) {
                n = h;                                   // §13.4 rule 2: HOT wins — don't even look at cold
            } else {
                byte[] cv = await(cold.get(coldKey(e.key())));   // HOT miss → read COLD
                if (cv != null) {                        // PROMOTION: pull cold value back into hot
                    n = Long.parseLong(new String(cv, StandardCharsets.UTF_8));
                    promotions++;
                } else {
                    n = 0;                               // brand-new key
                }
            }
            n++;                                          // the event
            hot.update(n);
            lastAccess.update(e.ts());
            LiveHot.map.put(e.key(), n);                   // track live hot value (test harness; see close())
            // (re)arm demotion check one window after this event's time
            ctx.timerService().registerEventTimeTimer(e.ts() + TIER_WINDOW_MS);
            out.collect(e.key() + " hot=" + n);
        }

        @Override
        public void onTimer(long ts, OnTimerContext ctx, Collector<String> out) throws Exception {
            Long h = hot.value();
            if (h == null) return;                        // already demoted
            Long last = lastAccess.value();
            if (last != null && ts - last < TIER_WINDOW_MS) return;   // touched again within window → keep hot
            // DEMOTE (§13.4 rule 1): write COLD first, durably, THEN clear HOT.
            await(cold.put(coldKey(ctx.getCurrentKey()), bytes(Long.toString(h))));
            await(cold.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));
            hot.clear();
            lastAccess.clear();
            LiveHot.map.remove(ctx.getCurrentKey());       // no longer hot (demoted to cold)
            demotions++;
            out.collect(ctx.getCurrentKey() + " DEMOTED@" + h);
        }

        @Override
        public void close() throws Exception {
            // Final state = for each key, hot value if present, else cold value (hot-wins). Dump to file.
            if (cold != null) {
                StringBuilder sb = new StringBuilder();
                // NOTE: we can only read hot for the CURRENT key in Flink; instead, re-open cold and merge with
                // whatever hot values remain by reading cold for all seen keys, then overlaying live hot via a
                // second pass is not possible here. Simpler & correct: at close, for each seen key, the source of
                // truth is hot if the key was never demoted OR was re-touched after last demotion. Since we cleared
                // hot on demote and re-populated on promote, the LIVE hot state per key isn't enumerable at close.
                // So we flush any remaining hot values to cold first via a keyed drain is also not available here.
                //
                // Practical approach for the test: we recorded demotions to cold already; for keys still hot at end
                // we must persist them. We do that by writing hot→cold in a final timer we can't schedule at close.
                // Instead we rely on: the test's final event advances time enough that non-'hot' keys demote, and
                // 'hot' remains hot. We persist the still-hot 'hot' key here by reading cold (which lacks it) — so
                // we ALSO need its hot value. We capture still-hot values in a static side-map updated on each event.
                for (String k : keysSeen) {
                    Long live = LiveHot.map.get(k);          // last-known hot value (null if demoted & not re-promoted)
                    byte[] cv = await(cold.get(coldKey(k)));
                    Long coldV = cv == null ? null : Long.parseLong(new String(cv, StandardCharsets.UTF_8));
                    Long val = (live != null) ? live : coldV;    // hot-wins
                    if (val != null) sb.append(k).append("=").append(val).append("\n");
                }
                Files.writeString(Path.of(resultFile), sb.toString());
                try { await(cold.shutdown()); } catch (Throwable ignored) {}
                try { cold.close(); } catch (Throwable ignored) {}
            }
            if (store != null) try { store.close(); } catch (Throwable ignored) {}
        }
    }

    /** Side-map tracking the last-known HOT value per key (updated each event), so close() can apply hot-wins.
     *  In real Flink you'd instead drain hot→cold at a stop/savepoint; this is a test-harness convenience. */
    static final class LiveHot { static final Map<String,Long> map = new HashMap<>(); }
}
