# flink-slatedb-poc

Runnable Maven projects that **empirically verify** the load-bearing claims of the SlateDB-in-Flink design.
No Docker — plain `mvn` + local JDKs. **The full design doc is included below** (see "Design Doc" section);
this top part is the runnable PoC that verifies it.

All modules have been run and **PASS** on this machine (see "Results" below).

## Layout

| Module | Verifies | Compiles on | Runs on |
|---|---|---|---|
| `flink-1.20-poc` | Flink **1.20** design claims: §3 KeyGroupRange reconstruction, §0 `open(Configuration)` + deprecated-but-present RuntimeContext getters, §13.5 key-group ownership partition | JDK 11 | JDK 11 (also 25) |
| `flink-2.3-poc` | Flink **2.3** claims: §0 `open(OpenContext)`, getTaskInfo()-only, §0/§15.2 `enableAsyncState()` exists **and** is runtime-enforced | JDK 17 | JDK 17 (also 25) |
| `slatedb-verify` | SlateDB **correctness**: §12.8/§6.6 clone-from-checkpoint exactly-once (excludes post-checkpoint writes, retains pre-checkpoint), and the **§6.3 memtable-flush finding** | **JDK 22+** only | **JDK 22+** only |
| `flink-slatedb-e2e` | **THE compositions** (5 mains): real Flink `KeyedProcessFunction` + SlateDB in ONE JVM — clone-restore exactly-once, real checkpoint+failure+recovery, §6.4 rescale merge, §13 hybrid tiering, §7 compaction, and §4/§4.1/§12.7 parallel shard-per-bucket (P=4) | **JDK 22+** | **JDK 22+** (Flink on JDK 25) |
| `slatedb-jna-j11` | §17: the Java-22 floor is a **packaging** choice — the SlateDB native lib runs on **JDK 11/17/25** via a regenerated **Kotlin/JNA** binding (real put/get + MEMTABLE flush + detached checkpoint) | JDK 11 | **JDK 11, 17, 25** |

`slatedb-verify` and `flink-slatedb-e2e` require **JDK 22+ by necessity**: `io.slatedb:slatedb-uniffi:0.14.1`
is compiled to Java 22 (class version 66, **FFM** API). A JDK 11/17 `javac` cannot even *read* those class
files (`wrong version 66.0`), so they cannot live in the Flink 1.20/2.3 projects. SlateDB is pulled from
Maven Central as a normal dependency (no manual JAR). **That floor is not fundamental** — the `slatedb-jna-j11`
module reaches the *same native library* through a **JNA** front-end and runs on JDK 11 (see §17).

## Prerequisites

- Maven 3.9+
- JDKs: **11** (for 1.20), **17** (for 2.3), and **22+** (for SlateDB; tested with Temurin **25**).
  Install 25 via: `sdk install java 25.0.3-tem`
- First build downloads Flink (~200MB) and slatedb-uniffi (~55MB fat multi-platform JAR) from Maven Central.

## Run

### 1. Flink 1.20 design checks (JDK 11)
```bash
cd flink-1.20-poc
export JAVA_HOME=~/.sdkman/candidates/java/11.0.26-tem
mvn -q compile
mvn -q dependency:build-classpath -Dmdep.outputFile=cp.txt
"$JAVA_HOME/bin/java" -cp "target/classes:$(cat cp.txt)" io.slatedb.flink.poc.FlinkKeyedStatePoc
```
(`exec:java` does NOT work for the MiniCluster job — classloader issue; use plain `java` as above.)

### 2. Flink 2.3 async-state checks (JDK 17)
```bash
cd flink-2.3-poc
export JAVA_HOME=~/.sdkman/candidates/java/17.0.15-tem
mvn -q compile
mvn -q dependency:build-classpath -Dmdep.outputFile=cp.txt
"$JAVA_HOME/bin/java" -cp "target/classes:$(cat cp.txt)" io.slatedb.flink.poc.Flink23AsyncStatePoc
```

### 3. SlateDB clone-restore exactly-once (JDK 22+)
The native lib is bundled in the JAR but not auto-extracted; extract it and point `java.library.path` at it:
```bash
cd slatedb-verify
export JAVA_HOME=~/.sdkman/candidates/java/25.0.3-tem
mvn -q compile
mvn -q dependency:build-classpath -Dmdep.outputFile=cp.txt
# one-time: extract the native lib for your platform from the fat JAR
mkdir -p native && (cd native && unzip -oj \
  ~/.m2/repository/io/slatedb/slatedb-uniffi/0.14.1/slatedb-uniffi-0.14.1.jar \
  'darwin-aarch64/libslatedb_uniffi.dylib')   # linux: linux-x86-64/libslatedb_uniffi.so
"$JAVA_HOME/bin/java" --enable-native-access=ALL-UNNAMED -Djava.library.path=native \
  -cp "target/classes:$(cat cp.txt)" io.slatedb.flink.poc.SlateDbCloneRestoreMain
```

### Bonus: prove Flink runs on JDK 22+ (so SlateDB + Flink can share one JVM)
Both Flink projects also run on JDK 25 with module flags — this is what makes in-process SlateDB+Flink possible:
```bash
export JAVA_HOME=~/.sdkman/candidates/java/25.0.3-tem
"$JAVA_HOME/bin/java" --add-opens java.base/java.util=ALL-UNNAMED \
  --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.time=ALL-UNNAMED \
  -cp "target/classes:$(cat cp.txt)" io.slatedb.flink.poc.<MainClass>
```

### 4. SlateDB on JDK 11 via the JNA binding (§17)
Proof the Java-22 floor is removable. Uses the Kotlin/JNA binding checked into `slatedb-jna-j11`; reuses the
same published native lib (no Rust toolchain to run). Runs unchanged on JDK **11, 17, and 25**:
```bash
cd slatedb-jna-j11
mkdir -p native && (cd native && unzip -oj \
  ~/.m2/repository/io/slatedb/slatedb-uniffi/0.14.1/slatedb-uniffi-0.14.1.jar \
  'darwin-aarch64/libslatedb_uniffi.dylib')   # linux: linux-x86-64/libslatedb_uniffi.so
export JAVA_HOME=~/.sdkman/candidates/java/11.0.26-tem   # or 17.0.15-tem, 25.0.3-tem
mvn -q clean compile
mvn -q exec:java -Djna.library.path="$PWD/native"
# → RESULT: PASS — SlateDB (JNA binding) ran real ops + checkpoint on Java 11.0.26
```
(No `--enable-native-access` / `--add-opens` — those are FFM-only; JNA needs neither.)

## Results (verified on this machine)

```
flink-1.20-poc     → ALL CHECKS PASSED ✅  (Flink 1.20.1, JDK 11 and JDK 25)
flink-2.3-poc      → ALL CHECKS PASSED ✅  (Flink 2.3.0,  JDK 17 and JDK 25)
slatedb-verify     → SLATEDB CHECKS PASSED ✅ (slatedb-uniffi 0.14.1, JDK 25)
flink-slatedb-e2e  → E2E PASSED ✅  (real Flink keyed op + SlateDB + exactly-once clone-restore, JDK 25)
                     RECOVERY E2E PASSED ✅  (real Flink checkpoint + induced failure + recovery = exactly-once)
                     RESCALE E2E PASSED ✅  (§6.4 clone+scan-copy merge, downscale 3→2 & upscale 2→4)
                     HYBRID TIERING E2E PASSED ✅  (§13 RocksDB-hot + SlateDB-cold: demote/promote/hot-wins)
                     COMPACTION E2E PASSED ✅  (§7 compactor merges L0→sorted-runs, drains L0, no data loss)
                     PARALLEL SHARD-PER-BUCKET E2E PASSED ✅  (§4/§4.1/§12.7 P=4, 16 shards, shared cache, exact)
                     GC E2E PASSED ✅  (§7/§16.13 compaction orphans files; compactor 900s checkpoint pins them; GC retains, no loss)
                     LONG GC E2E PASSED ✅  (§16.13 ~17min: orphans physically deleted at t+900s; 15→4 files; no loss)
                     MARSHALLING BENCH 📊  (§16.14 serialization cheap ~4µs/16KB; async round-trip ~44µs dominates; pipeline→97k/s)
                     READ-YOUR-WRITES PASSED ✅  (§16.15 consistent reads before any flush; put/overwrite/delete/RMW/scan; visibility≠durability)
                     FLINK-SERDE PASSED ✅  (§16.16 Flink TypeSerializer ⇄ SlateDB byte[]; PojoSerializer round-trips 1000 objects + RMW)
slatedb-jna-j11    → JNA BINDING PASSED ✅  (§17 real ops + checkpoint on JDK 11, 17, AND 25 — no FFM, no flags)
```

Untested (need real infra/load, not a MiniCluster): §8 L0 write-stall backpressure, §9/§9A memory-OOM/disk,
§14 resharding, real parallel savepoint→rescale, and all §11 operational risks (S3 tail latency, cost, pre-1.0
format stability, failover fencing under concurrency).

`flink-slatedb-e2e` has TWO mains:
- `FlinkSlateDbE2E` — barrier-sentinel checkpoint + clone-restore (simpler).
- `FlinkSlateDbRecoveryE2E` — **real** `enableCheckpointing` + throttled replayable source + induced failure
  + genuine Flink recovery; asserts each key counted exactly 10 across the failure. Run it the same way,
  just swap the main class:
  ```bash
  "$JAVA_HOME/bin/java" --enable-native-access=ALL-UNNAMED -Djava.library.path=native \
    --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED \
    --add-opens java.base/java.time=ALL-UNNAMED \
    -cp "target/classes:src/main/resources:$(cat cp.txt)" io.slatedb.flink.poc.FlinkSlateDbRecoveryE2E
  # → final counts = {key-0=10 ... key-5=10}  after cloning from the checkpoint captured pre-failure
  ```
- `SlateDbRescaleE2E` — the **§6.4 rescale merge** (clone-with-projection + scan-copy) on the handle list
  Flink union list state carries across a parallelism change; downscale 3→2 and upscale 2→4; asserts exact
  key-group partition (no loss/dup/foreign-key). Same run command, class `io.slatedb.flink.poc.SlateDbRescaleE2E`
  (no `--add-opens` needed — it uses no Flink runtime, only `KeyGroupRangeAssignment` + SlateDB).
- `FlinkHybridTieringE2E` — the **§13 hybrid**: real Flink `KeyedProcessFunction` with hot state in Flink
  `ValueState` and cold state in SlateDB; event-time demotion timers, promotion on cold-hit, hot-wins reads;
  asserts exact `(hot∪cold)` counts through demote→promote→re-demote. Same run command, class
  `io.slatedb.flink.poc.FlinkHybridTieringE2E`.

### Run the end-to-end test (JDK 22+)
```bash
cd flink-slatedb-e2e
export JAVA_HOME=~/.sdkman/candidates/java/25.0.3-tem
mvn -q compile
mvn -q dependency:build-classpath -Dmdep.outputFile=cp.txt
mkdir -p native && (cd native && unzip -oj \
  ~/.m2/repository/io/slatedb/slatedb-uniffi/0.14.1/slatedb-uniffi-0.14.1.jar \
  'darwin-aarch64/libslatedb_uniffi.dylib')
"$JAVA_HOME/bin/java" --enable-native-access=ALL-UNNAMED -Djava.library.path=native \
  --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED \
  --add-opens java.base/java.time=ALL-UNNAMED \
  -cp "target/classes:src/main/resources:$(cat cp.txt)" io.slatedb.flink.poc.FlinkSlateDbE2E
# → clone counts = {a=2, b=1}  (post-barrier a→3, c=1 discarded → exactly-once)
```

## Findings that came out of RUNNING it (not just reading source)

1. **§6.3 correction — `flush()` is not enough before a checkpoint.**
   `Db.flush()` (WAL enabled) is `FlushType::Wal` — WAL only, not memtable→L0. A checkpoint pins the
   *manifest*, which only advances on a **memtable** flush. So `flush()` + `createDetachedCheckpoint()`
   pins a manifest that does NOT contain the just-written data (a pinned reader saw nothing across 20
   checkpoints / 5s). **Fix:** `flushWithOptions(FlushType.MEM_TABLE)` before checkpointing. Without this,
   a Flink checkpoint would silently lose the operator's SlateDB state on every restore.

2. **§0 correction — Flink 1.20 AND 2.3 both RUN on JDK 25.** The Java-version conflict (SlateDB needs
   Java 22+ FFM; Flink officially supports ≤17/21) is real but **surmountable**: the whole cluster on
   JDK 22+ with `--add-opens` / `--enable-native-access` works (empirically, on a MiniCluster). Officially
   unsupported by Flink, so validate under real load before production.

3. **§12.8 confirmed empirically** — clone-from-checkpoint excludes post-checkpoint writes AND retains
   pre-checkpoint data. Exactly-once restore via clone-on-restore is sound (given finding #1's memtable flush).

4. **§5 API corrections confirmed against the real JAR** (via `javap`):
   - `CloneSourceSpec` is a plain data class: `new CloneSourceSpec(String path, String checkpoint, KeyRange projectionRange)` — NOT a `withCheckpoint(...)` factory.
   - `CheckpointCreateResult.id()` returns a **String** (not UUID); `.manifestId()` is a `long`.
   - Accessors are method-style (`.id()`, `.key()`, `.value()`), builders are non-chainable void setters,
     `AdminBuilder.build()` is sync, all async ops return `CompletableFuture`.
5. **§7/§8 API — `Settings.set(key, valueJson)` takes JSON values**: numbers bare (`"4096"`), durations as
   quoted strings (`"\"200ms\""`), not JSON maps.
6. **`ObjectStore.resolve` requires an EMPTY path** — the store is the root (`file:///`, `s3://bucket`) and the
   DB path goes to the *builder* as the db-name; `resolve("file:///tmp/x")` fails.

The `§`-references throughout point into the **Design Doc** section below (this file is self-contained).

---
---

# Design Doc

> The full design document follows. It was written and verified first; the PoC above exists to empirically
> check its load-bearing claims. §16 records what was verified by *running* (including bugs found that
> source-reading missed).



**Status:** Draft / design exploration
**Date:** 2026-07-11
**SlateDB version verified against:** `0.14.1` (crate published 2026-07-01) + `io.slatedb:slatedb-uniffi` Java binding
**Target Flink:** **both 1.20 (LTS) and 2.3** — this design must run on both. ⚠️ They differ substantially (async state, ForSt, API signatures). See **§0 Version matrix** first; it governs the whole doc.

> **Provenance note.** Every load-bearing SlateDB API signature, default value, and behavior was verified against the shipped 0.14.1 crate source, its UniFFI export layer, and the `Default` impls — not RFC prose. Flink-side facts were verified against **both Flink 1.20 and 2.3** docs (see §0 and §13.8). Items still inferred or unverified are called out in **§12 Open Questions** and each section's provenance note. Do not treat inferred items as facts when building.
>
> **Contents:** **§0 version matrix (1.20 vs 2.3) — read first.** §1–§12 core SlateDB-as-keyed-state design. **§13 hybrid tiered state (RocksDB hot + SlateDB cold)** — the recommended way to actually adopt SlateDB in Flink.

---

## 0. Version Matrix — Flink 1.20 vs 2.3 (read first)

This design must run on **both Flink 1.20 (LTS) and 2.3**. They differ enough that several code paths must branch by version. Verified against each version's docs; this table governs the whole doc.

| Capability | Flink 1.20 | Flink 2.3 | Design impact |
|---|---|---|---|
| **Async state API** (`StateFuture`, async `ValueState`/`MapState`) | ❌ none; **synchronous only** | ✅ via `keyBy(...).enableAsyncState()`; descriptors under `org.apache.flink.api.common.state.v2`; `asyncValue()`/`asyncUpdate()`/`asyncGet()`… | On 2.3 the hot tier / non-blocking reads can use native async state; on 1.20 they cannot. |
| **ForSt / disaggregated backend** | ❌ does not exist | ✅ available; **recommended backend for async state** ("spill to remote FS when necessary") | The "just use ForSt" alternative is **real on 2.3**, **unavailable on 1.20**. |
| **State backends** | `HashMapStateBackend`, `EmbeddedRocksDBStateBackend` | + `ForStStateBackend` | Hot tier (§13): RocksDB on 1.20; RocksDB **or** ForSt on 2.3. |
| **`getMailboxExecutor()` on `RuntimeContext`** | ❌ absent | ❌ **also absent** | Neither version exposes a mailbox executor to a `KeyedProcessFunction`. The intended non-blocking path on 2.3 is `enableAsyncState()`, **not** a mailbox grab. On 1.20 the only non-blocking option is a **custom operator** (`AbstractStreamOperator` + `OneInputStreamOperator`) that receives the `MailboxExecutor` at setup. |
| **Parallelism getters** (`getNumberOfParallelSubtasks` etc.) | ✅ on `RuntimeContext` (deprecated since 1.19) **and** via `getTaskInfo()` | ❌ **removed** from `RuntimeContext` — **must** use `getTaskInfo()` | §3 code must use `getTaskInfo()` to compile on 2.3 (works on 1.20 too). |
| **`open()` signature** | `open(Configuration)` | `open(OpenContext)` | Branch per version, or target the common ancestor. |
| **`onTimer`/`processElement` synchronization** | ✅ guaranteed | ✅ guaranteed (verbatim, unchanged) | §13.4 consistency argument holds on both. |
| **JVM required to run SlateDB in-process** | Java 22+ (SlateDB FFM floor); 1.20 stock = Java 11/17 | Java 22+; 2.3 stock = Java 17/21 | ⚠️ SlateDB needs **Java 22+**. Both Flink versions **do run on JDK 25** (verified §16.3) with `--add-opens`, so one shared JDK-22+ JVM works — officially unsupported by Flink, validate under load. |

**Portability guidance:** write the SlateDB integration against `getTaskInfo()` (works on both) and isolate the `open()` signature + any `enableAsyncState()` usage behind a thin version-specific shim. The SlateDB-facing code (§5–§9) is Flink-version-independent — only the Flink-API glue (§3, §6.2, §13.6) branches.

**"Offload RocksDB" alternatives, per version:**
1. **Changelog State Backend** (`state.backend.changelog`) — exists in **both**, fully integrated, exactly-once. If the goal is faster checkpoints/recovery/rescale (not literally removing local disk), **cheapest win — try first.**
2. **ForSt** — **2.3 only.** If true disaggregated state is the goal and you're on 2.3, weigh ForSt (integrated, native async) *before* building this DIY design. Not available on 1.20.
3. **This DIY SlateDB design** — justified only if you need SlateDB's object-store-native engine specifically, and either run on 1.20 (no ForSt) or prefer SlateDB's data structure over ForSt's. Note: to run on **both** versions, this DIY path is the *only* single design that works uniformly (since ForSt is 2.3-only) — a point in its favor if cross-version uniformity matters to you.

### 0.1 Decision guide — pick the path by goal × version

Answer top-to-bottom; first match wins.

| # | If your situation is… | On Flink 1.20 → | On Flink 2.3 → |
|---|---|---|---|
| 1 | Pain is **slow checkpoints / recovery / rescale**, not "local disk exists" | **Changelog backend** (config flag) | **Changelog backend** |
| 2 | Want **true disaggregated state**, willing to use an integrated backend | Not available → Changelog, or upgrade to 2.3, or path 4 | **ForSt** via `enableAsyncState()` — the intended answer |
| 3 | Need **exactly-once keyed state**, only want to **bound RocksDB size** (mirror writes OK) | **Hybrid Pattern 1** (§13.2, write-through; SlateDB authoritative) | Pattern 1, **or** ForSt if disaggregation is acceptable |
| 4 | Need SlateDB's **object-store-native engine specifically**, cold tail **rarely read** | **Hybrid Pattern 2** (§13), blocking cold reads, `KeyedProcessFunction` | Same — but first ask if **ForSt** suffices (usually yes) |
| 5 | Need SlateDB engine, cold tail **frequently read** (needs non-blocking) | **Custom operator** + mailbox (§13.6) — significant effort | **ForSt** via `enableAsyncState()` — don't hand-roll SlateDB here |
| 6 | Need **one design that runs uniformly on both versions** | DIY SlateDB (§5–§13) is the only cross-version-uniform option (ForSt is 2.3-only) | " |

**Compressed heuristics:**
- **Try the Changelog backend before anything in this doc** — it solves the most common pain for a config change, on both versions.
- **On 2.3, ForSt is almost always the better answer than DIY SlateDB.** The DIY path earns its keep mainly on **1.20** (no ForSt) or when you have a hard requirement for SlateDB's specific engine/cost model.
- **The moment you need non-blocking cold reads, the calculus flips toward ForSt (2.3) or a custom operator (1.20)** — a plain `KeyedProcessFunction` cannot do non-blocking state on either version.
- **Cross-version uniformity is the one clear structural win for the DIY SlateDB design** — it's the only option here that behaves identically on 1.20 and 2.3.

---

## 1. Purpose & Scope

Design for using **SlateDB** (an LSM key-value store built on object storage) as the **keyed-state store inside a Flink `KeyedProcessFunction`**, operated *outside* Flink's managed keyed-state backend.

This means we re-implement, by hand, the machinery Flink normally provides for free: key-group partitioning, checkpoint-barrier coordination, rescaling, and native-memory accounting. This doc specifies how, and flags every place where the DIY path costs more than the integrated backends.

**Recurring theme:** almost every hard part here (async state, rescale, memory accounting, compaction isolation) is something Flink's *integrated* native backends (RocksDB, ForSt) already solve. Choosing SlateDB-in-a-ProcessFunction is choosing to own that work in exchange for SlateDB's disaggregated-storage properties.

---

## 2. Motivation

### Why disaggregated state at all
Local RocksDB state is *the* operational pain in Flink: checkpointing, rescaling, and recovery all suffer because state is pinned to local disk. Disaggregated storage (state lives in S3/GCS, addressable by any node) targets exactly this: elastic rescaling without shuffling local state, trivial snapshots, no rebalancing.

### Why not ForSt (Flink's native disaggregated backend)
ForSt is the integrated answer and should be the default consideration. The critiques motivating a SlateDB exploration instead:
- **Async-API tax** — ForSt's performance depends on the Flink 2.0 async state model (`StateFuture`). Only operators rewritten to the async API benefit; the whole synchronous ecosystem is left behind.
- **LSM-on-object-storage mismatch** — ForSt is a RocksDB fork (leveled LSM + compaction) retrofitted onto remote storage, whose access pattern (many small random ops) is the opposite of what object storage rewards. SlateDB was *designed around* the object-store cost model (write batching, request-cost awareness).
- **Leaky disaggregation** — ForSt still leans on local disk as a working-set cache; cold reads fall off a latency cliff.
- **Fork maintenance** — a C++ RocksDB fork to maintain forever.

**Counterpoint (important):** ForSt is *natively integrated* with the checkpoint coordinator, rescaling, and timer system. SlateDB is not. This doc is the price of that gap. **Re-evaluate ForSt before committing** — the DIY SlateDB path only wins if you specifically want SlateDB's data structure / cost model and accept the integration burden.

### 2.1 Architecture at a glance

SlateDB is an LSM key-value store whose **durable state lives entirely in object storage**; the process holds only volatile state (memtables + caches). This diagram is verified against the v0.14.1 source — §§ below expand each piece.

```
┌────────────────────────────────────────────────────────────────────────────────────┐
│  PROCESS  (e.g. Flink TaskManager JVM)                                             │
│                                                                                    │
│  Java/Kotlin app ──FFM (§17) or JNA──▶ SlateDB binding                             │
│                        │             └─▶ shared Tokio runtime (1/process, §4.1)    │
│                        ▼                                                           │
├────────────────────────────────────────────────────────────────────────────────────┤
│  Db (single writer)                                                                │
│                                                                                    │
│  WRITE PATH                              READ PATH  get(k): newest → oldest        │
│                                                                                    │
│  put/delete/merge/write                       ┌──────────────┐  (mutable)          │
│      │ (1) durability                         │   memtable   │                     │
│      ▼                                        └──────┬───────┘                     │
│  [WAL buffer] ──flush──▶ wal/*.sst                   ▼                             │
│      │ (2)              (object store)        ┌──────────────────┐                 │
│      ▼                                        │ immutable memtbls │                │
│  [memtable] ──freeze──▶ imm memtables         └──────┬───────────┘                 │
│                            │ flush (MEM_TABLE, §16.2)  │ miss                      │
│                            ▼                          ▼                            │
│                      compacted/*.sst  ◀── read ── on-disk LSM                      │
│                        (L0 SSTs)                (via block cache)                  │
│                                                                                    │
│  IN-MEMORY CACHES  (off-heap, native — NOT Flink-managed, §9):                     │
│    • block cache 512 MiB  (foyer default / moka, §9.1a) → SST blocks               │
│    • meta  cache 128 MiB  → SST indexes + BLOOM FILTERS (§5B)                      │
│                                                                                    │
│  BACKGROUND TASKS  (on the shared Tokio runtime):                                  │
│    • Memtable flusher  — imm memtable ─▶ L0 SST                                    │
│    • Compactor         — L0 SSTs ─▶ sorted runs (rewrites manifest;                │
│                          leaves orphans, pins them via 900s checkpoint)            │
│    • Garbage collector — deletes orphaned SSTs after expiry (§16.13)               │
└────────────────────────────────────────────────────────────────────────────────────┘
       │ CAS + epoch fencing → single-writer invariant (⇒ one DB per subtask)
       │  all durable state lives in object storage (nothing durable on local disk)
       ▼
╔════════════════════════════════════════════════════════════════════════════════════╗
║  OBJECT STORAGE  (S3 / GCS / Azure / file://)                                      ║
║                                                                                    ║
║  <db-root>/                                                                        ║
║    ├── wal/         00…001.sst, 00…002.sst      — write-ahead log SSTs             ║
║    ├── compacted/   <ULID>.sst, <ULID>.sst      — L0 SSTs + sorted-run SSTs        ║
║    ├── manifest/    <n>.manifest (CAS-updated)  — LSM state + checkpoints          ║
║    ├── compactions/ <n>.compactions             — compactor job state              ║
║    └── gc/          *.boundary                  — GC bookkeeping                   ║
║                                                                                    ║
║  LSM tree (described by the manifest):                                             ║
║    L0 : [sst][sst][sst]…   newest, overlapping (l0_max_ssts_per_key=8 →            ║
║                            write backpressure when exceeded, §8)                   ║
║    SR1: [──── sorted run ────]         compacted, non-overlapping                  ║
║    SR2: [──────── sorted run ────────]    ↑ compaction merges L0 → SR              ║
║    …    (size-tiered; older/larger runs toward the bottom)                         ║
╚════════════════════════════════════════════════════════════════════════════════════╝

OPTIONAL (off by default): object-store disk cache → local NVMe (caches SST
                           parts, §9A) — the ONLY path that touches local disk.
```

**Reading it:**
- **Write = two steps:** WAL buffer first for durability (→ `wal/*.sst`), then the memtable (`batch_write.rs`). A full memtable freezes → the flusher writes it as an **L0 SST**.
- **Read = newest→oldest:** mutable memtable → immutable memtables → on-disk LSM (L0, then sorted runs), first hit wins (`reader.rs`); bloom filters (§5B) skip SSTs that can't hold the key.
- **The disaggregation line is the process boundary** — everything durable is in object storage; in-process is only volatile memtables + caches (native/off-heap, the §9 OOM risk). Local disk is untouched unless the optional disk cache is enabled.
- **Compaction ≠ deletion** — the compactor rewrites the manifest and orphans old SSTs; the GC reclaims them only after a 900s checkpoint expiry (§16.13).
- **Single-writer** via CAS + epoch fencing on the manifest — the reason the Flink model is **one DB per subtask** (§4).

---

## 3. Background: Flink Keyed State & Rescaling Model

We must replicate this model on top of SlateDB.

- **Key groups** partition the key space; count = `maxParallelism` (set at job start, **immutable**).
  `keyGroup = murmurHash(key.hashCode()) % maxParallelism`
- Each subtask owns **one contiguous `KeyGroupRange`** (e.g. `[0,341]`).
- **Rescaling** reassigns key-group ranges to subtasks. A new subtask's range may span parts of several old instances → the core rescale problem.

### `KeyGroupRange` is NOT exposed to ProcessFunction — reconstruct it
`KeyGroupRange` lives in `org.apache.flink.runtime.state` and is deliberately kept out of the DataStream API. Reconstruct it deterministically from `RuntimeContext`:

```java
// Use getTaskInfo() — REQUIRED on 2.3 (direct getters removed), works on 1.20 too. Portable across both.
TaskInfo ti = getRuntimeContext().getTaskInfo();
int maxParallelism = ti.getMaxNumberOfParallelSubtasks();   // == number of key groups
int parallelism    = ti.getNumberOfParallelSubtasks();
int subtaskIndex   = ti.getIndexOfThisSubtask();

KeyGroupRange range = KeyGroupRangeAssignment
    .computeKeyGroupRangeForOperatorIndex(maxParallelism, parallelism, subtaskIndex);

int kg = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
```

> ⚠️ `KeyGroupRangeAssignment` is runtime-internal (`flink-runtime`), no `@Public` stability guarantee. Fine for operational logic; signatures can shift across versions.
> **Version branches (§0):** `open(Configuration)` on 1.20 vs `open(OpenContext)` on 2.3. The direct `RuntimeContext` parallelism getters are deprecated-but-present on 1.20 and **removed** on 2.3 — so use `getTaskInfo()` as above to compile on both.

---

## 4. Physical Layout Decision (the most important architectural choice)

Three ways to map SlateDB instances to Flink subtasks:

| Layout | Rescale cost | Per-subtask steady-state overhead | Trade-off |
|---|---|---|---|
| **Shard-per-bucket** (fixed N shards, each = contiguous bucket of key groups, one SlateDB per shard) | **~zero byte movement** — pure ownership reassignment | **∝ N/P instances per subtask** — see §4.1 | Zero-copy rescale; a shard is never split across subtasks; **but per-subtask cost multiplies by N/P**, not "fixed and small" |
| **One-DB-per-subtask** (RocksDB-style) | near-zero for base (clone), scan-copy for the rest | **exactly 1 instance per subtask — lowest** | Fewest instances → lowest steady-state overhead; pays at rescale (scan-copy on downscale); hand-rolled merge loop |
| **One shared DB** (prefix by key group) | ~zero | — | ❌ **Infeasible** — SlateDB is single-writer (manifest epoch fencing); can't have all subtasks writing one DB |

### 4.1 The N/P trap (correcting an earlier over-simplification)

Shard-per-bucket does **not** give "a fixed, small number of DBs per TaskManager." It gives a fixed number of DBs *total* (N); **per subtask you run `N/P` instances** (P = current parallelism). Every per-instance cost multiplies by N/P. Since N must be ≥ max intended parallelism (so a shard never splits across subtasks), running at a lower current P makes N/P large — e.g. **N=512, P=8 → 64 SlateDB instances per subtask.**

**What is shared vs. per-instance across those N/P instances (verified from source):**

| Resource | Shared? | Consequence |
|---|---|---|
| **Tokio runtime** | ✅ **process-wide** (`static OnceLock<Runtime>`, sized to `available_parallelism()`, tune via `SLATEDB_UNIFFI_RUNTIME_THREADS`) | No thread explosion from many instances — the biggest fear is unfounded. But this fixed native pool competes with Flink task threads for CPU. |
| **Block/meta cache (`DbCache`)** | ✅ **shareable & collision-safe** — `Arc<dyn DbCache>`; build one, pass to every `withDbCache(...)` | Collapses the §9 cache multiplier from 640 MiB × (N/P) to a single shared pool. ✅ Cache keys are per-DB scoped (`CachedKey.scope_id` via `DbCacheWrapper`) — verified safe across instances (§12.7). |
| **Object-store disk cache** | ✅ shareable via same `root_folder` | §9A disk multiplier collapses too. |
| **`max_unflushed_bytes` buffers** | ❌ **per-instance** | Dominant memory cost: **N/P × (default 1 GiB)** per subtask unless trimmed. Cut to 64–128 MiB. |
| **Embedded compactor (coordinator+worker)** | ❌ **per-instance** | **N/P compactors per subtask**, each polling its manifest every 5 s → **N manifest GETs / 5 s across the job** (scales with N, not P) + background CPU. Offload to sidecar. |
| **Object-store connections / file handles** | ❌ per-instance | N/P × connection pools. |

**Does it hurt Flink performance?** Yes, in three ways, all **linear in N/P and tunable** (not catastrophic):
1. **CPU:** shared runtime + N/P compactors do native background work inside the TM JVM, competing with task threads. Bound via `SLATEDB_UNIFFI_RUNTIME_THREADS` + `max_concurrent_compactions`.
2. **S3 request cost/rate:** N manifest polls / 5 s + N compactors' GET/PUT — a standing tax that scales with **N (total shards), even at low P**.
3. **Memory:** N/P × `max_unflushed_bytes` (cache portion mitigated by sharing).

### 4.2 Recommendation — pick by where you want to pay

The real trade is **steady-state per-instance overhead (∝ N/P) vs. rescale cost**:

- **Shard-per-bucket with N tuned close to typical P** (N/P in low single digits) → **best of both:** zero-copy rescale *and* low per-subtask overhead. This is the sweet spot. Do **not** set N enormous "to be safe."
- **Shard-per-bucket with N ≫ P** → rescale win is real, but per-subtask overhead (N/P × unflushed + N/P compactors) is significant. Only worth it if rescales are frequent enough to justify it, *and* you apply all mitigations below.
- **One-DB-per-subtask** → **lowest steady-state overhead (exactly 1 instance/subtask)**; pays at rescale via scan-copy. **If rescales are rare and N/P would be large, this is the better choice** — the opposite of what an earlier draft implied.

**Mitigations that make many-shard viable:** (1) share one `DbCache` across all shards on the subtask (collision-safe — per-DB scoped, §12.7); (2) cap `SLATEDB_UNIFFI_RUNTIME_THREADS`; (3) cut `max_unflushed_bytes` to 64–128 MiB/DB; (4) offload compaction to a sidecar so N/P compactors aren't in the TM (§7). With all four, per-subtask overhead reduces to ≈ N/P × (small unflushed buffer) + shared pools — manageable for small N/P.

> **Correction note.** Earlier drafts of this doc asserted shard-per-bucket gives a "fixed, small DB count → bounded memory." That is wrong when N ≫ P: per-subtask instance count is N/P and most per-instance costs multiply. Shard-per-bucket wins on **rescale**; it does **not** universally win on steady-state overhead.

---

## 5. Verified SlateDB API Surface

### 5.1 Java binding basics
- Maven: `io.slatedb:slatedb-uniffi`; package `io.slatedb.uniffi`.
- UniFFI-generated (via `uniffi-bindgen-java`); **generated `.java` sources are NOT committed** — produced into `build/generated/...` at build time. Native lib bundled as a JNA resource.
- UniFFI runs the Tokio runtime; async Rust methods surface as **`CompletableFuture`** in Java. You do **not** write JNI.
- Handles are `AutoCloseable` (use try-with-resources for the native destructor).

### 5.2 `Db` methods (all async → `CompletableFuture`, unless noted)

| Java method | Returns | Notes |
|---|---|---|
| `get(byte[])` | `CompletableFuture<byte[]>` | **null if absent** |
| `put(byte[],byte[])` | `CompletableFuture<WriteHandle>` | **not `Void`** |
| `delete(byte[])` | `CompletableFuture<WriteHandle>` | single-key only |
| `merge(byte[],byte[])` | `CompletableFuture<WriteHandle>` | ✅ merge operator — race-free associative RMW |
| `write(WriteBatch)` | `CompletableFuture<WriteHandle>` | batch |
| `scan(KeyRange)` | `CompletableFuture<DbIterator>` | takes a `KeyRange` object |
| `scanPrefix(byte[],KeyRange)` | `CompletableFuture<DbIterator>` | prefix + sub-range |
| `flush()` | `CompletableFuture<Void>` | |
| `begin(IsolationLevel)` | `CompletableFuture<DbTransaction>` | ✅ transactions (SSI) |
| `snapshot()` | `CompletableFuture<DbSnapshot>` | |
| `shutdown()` | `CompletableFuture<Void>` | **the graceful close** — `close` reserved for the AutoCloseable destructor |
| `status()` | `DbStatus` | **synchronous** (only non-async method) |

### 5.3 Checkpoint & clone (NOT on `Db` — on `Admin`)

- **Checkpoint:** `admin.createDetachedCheckpoint(CheckpointOptions)` → `CompletableFuture<CheckpointCreateResult>`. "Detached" = outlives the `Db` handle (exactly what we need to fold into a Flink checkpoint). Also on `Admin`: `listCheckpoints`, `deleteCheckpoint`, `refreshCheckpoint`, `runGcOnce`.
- **Clone** (shallow, shares SSTs):
  ```java
  CloneSourceSpec src = CloneSourceSpec.withCheckpoint(parentPath, cpId); // cpId is a String; or .new(parentPath). Exact Java shape: §12.1
  CloneBuilder cb = admin.createCloneBuilderFromSource(src);   // SYNC — returns builder
  cb.withClonePath(newPath);        // String; setters return void (NOT chainable)
  cb.withObjectStore(store);
  // projection to clone only a key-range slice is set on CloneSourceSpec.projectionRange (§14.2), not the builder
  await(cb.build());                // async terminal → CompletableFuture<Void>
  ```
  Clone is a **shallow copy**: new manifest references the same SSTs, no copy except WAL. **But the clone reads the parent's SSTs at the parent path and pins them alive via the checkpoint** → GC must be reference-aware; repeated clones build a chain (see §6.5).

### 5.4 Builders — ⚠️ NOT fluent
Exported builder setters return `Result<()>` → **`void` in Java** (throw on error), *not* `this`. Call each as a separate statement.

| Builder | Constructor | `build()` |
|---|---|---|
| `DbBuilder` | `new(path, store)` sync | **async** → `CompletableFuture<Db>` |
| `AdminBuilder` | `new(path, store)` sync | **sync** → `Admin` (no future!) |
| `DbReaderBuilder` | sync | async |
| `CloneBuilder` | **no public ctor** — only via `admin.createCloneBuilderFromSource(spec)` | async → `Void` |

`DbBuilder` methods: `withSettings(Settings)`, `withWalObjectStore`, `withDbCache(DbCache)`, `withDbCacheDisabled()`, `withMergeOperator`, `withFilterPolicies`, `withSstBlockSize`, `withSegmentExtractor`, `withMetricsRecorder`, `withSeed`.

### 5.5 ❌ What does NOT exist
- **Range delete** — only single-key `delete` / `delete_with_options`. Confirmed absent in 0.14.1 (README lists range deletions, DB splitting, DB merging as "under development"). **Consequence: logical clipping on rescale is mandatory (§6.4).**
- **`Db.create_checkpoint`** exists in the *Rust* crate but is **not exported on `Db` in the binding** — use `Admin.createDetachedCheckpoint`.
- **`with_compaction_runtime`** is Rust-only; not on the Java `DbBuilder`.
- **`CheckpointScope`** enum is Rust-only (not bound). Not needed: the Java checkpoint path is `Admin.createDetachedCheckpoint(CheckpointOptions)` — no scope param.

### 5A. Supporting types — Java availability (all verified in the binding)

Every non-`Db` type the design calls is confirmed present in the UniFFI/Java binding:

| Type | uniffi kind | Java usage | Verified |
|---|---|---|---|
| `WriteBatch` | Object | `new()`, `put/delete/merge(+WithOptions)`; **single-use once submitted** | `write_batch.rs` |
| `DbIterator` | Object | `next()` → `CompletableFuture<KeyValue>` (**null = end**), `seek()` — both async | `iterator.rs` |
| `KeyValue` | Record | `.key` (`byte[]`), `.value` (`byte[]`), **`.seq`, `.createTs`, `.expireTs`** | `types.rs` |
| `DbReader` + `DbReaderBuilder` | Object | `new(path, store)` sync; `withCheckpointId(String)`; `build()` async → `DbReader`; read-only `get`/`scan` families | `builder.rs`, `db_reader.rs` |
| `CheckpointOptions` | Record | fields `lifetimeMs`, `source`, `name` (all nullable) | `config.rs` |
| `CheckpointCreateResult` | Record | **`.id` is a `String`** (not `UUID`), `.manifestId` (`long`) | `types.rs` |
| `KeyRange` | Record | `start`/`end` (`byte[]` bounds) + inclusivity flags (§14.5) | `types.rs` |
| `CloneSourceSpec` | ⚠️ used by `createCloneBuilderFromSource`; exact Java shape not yet captured (§12.1) | | `types.rs` |

**Checkpoint ids are `String` end-to-end in Java** (`CheckpointCreateResult.id` → `DbReaderBuilder.withCheckpointId` / `CloneSourceSpec`). Rust uses `Uuid` internally and parses at the boundary (throws `InvalidCheckpointId` on malformed input). **Anywhere this doc wrote `UUID cpId`, the Java type is `String`.**

**Rescale-read path confirmed viable:** `DbReaderBuilder.withCheckpointId(cpId)` opens a source DB pinned to its checkpoint, and `DbReader.scan(range)` + `DbIterator.next()` drive the §6.4 merge loop. No missing primitive.

### 5B. Bloom filters — yes, like RocksDB (default-on, tunable, exposed in Java)

SlateDB writes **per-SST bloom filters**, same purpose as RocksDB's: on a point `get`, skip reading an SST's data blocks (and, on object storage, avoid a whole S3 GET) when the key is definitely absent. Verified against source (`slatedb/src/config.rs`, `filter_policy.rs`, binding `builder.rs`/`filter_policy.rs`):

| Aspect | Value | Note |
|---|---|---|
| Default | **ON** | An SST gets a filter when it holds **≥ `min_filter_keys` = 1000** keys (`config.rs` default). Smaller SSTs skip it — a full scan is cheaper than a filter lookup there. |
| Sizing | **10 bits/key** default | The default `FilterPolicy` is a single bloom filter at 10 bits/key (`builder.rs`) — comparable to RocksDB's default. Memory lands in the **128 MiB meta cache** (§9), off-heap. |
| Java control | `DbBuilder.withFilterPolicies(List<FilterPolicy>)` | `FilterPolicy` + `BloomFilterOptions` are bound; tune bits/key or disable. Reader side must match the writer's `min_filter_keys` (there is a matching `ReaderOptions.min_filter_keys`). |
| Prefix filters | `PrefixExtractor` (bound) | Beyond stock RocksDB: a prefix-bloom mode — useful if your Flink keys share a key-group byte prefix (§6.1), letting a `scan_prefix` skip SSTs by prefix. |

**Why it matters more here than in RocksDB:** a RocksDB false-negative-avoidance saves a local disk read (µs); a SlateDB filter check saves an **object-store round-trip** (ms + request cost). So for point-lookup-heavy keyed state, the filter is doing proportionally more work. Nuance vs RocksDB: RocksDB can pin filter blocks separately in memory; SlateDB caches them in the meta cache (§9) — functionally equivalent for lookups, but subject to the same soft cache-eviction caveat.

### 5C. On-disk file formats

What SlateDB actually writes to object storage. Verified against `schemas/*.fbs` and `slatedb/src/format/*` (v0.14.1; SST format v2, manifest format v2). Useful when debugging state, sizing storage (§9A), or reasoning about the checkpoint/clone paths (§6.6).

**Five file kinds, all in object storage, all write-once (immutable + CAS-ordered):**

```
<db-root>/
├── wal/          00000000000000000001.sst   ← WAL SSTs             (zero-padded u64 id)
├── compacted/    01J79C21YKR31J2BS1EFXJZ7MR.sst  ← L0 + sorted-run SSTs (ULID id)
├── manifest/     00000000000000000002.manifest   ← LSM state snapshots  (zero-padded u64; highest id = current)
├── compactions/  00000000000000000005.compactions ← compactor job state (zero-padded u64)
└── gc/           manifest.boundary  compactions.boundary  ← GC low-water marks (bare integer)
```

#### SST files (`wal/*.sst` and `compacted/*.sst`)

Same binary format for both; `sst_type` (Wal vs Compacted) and the manifest tell them apart. An SST = sorted **data blocks** + a **footer**:

```
╔════════════════════════════════════════════════════════════════════════════════╗
║  SST FILE                                                                        ║
║  ┌─────────── DATA BLOCKS (sorted by key) ───────────┐                           ║
║  │ Block0 [rows…][restarts:u16…][num_restarts:u16] +CRC32 │  ~4 KiB target each  ║
║  │ Block1 …                                           +CRC32                     ║
║  │ …                                                                             ║
║  ├──────────────────────── FOOTER ───────────────────────┤                       ║
║  │ FILTER block   composite bloom filter(s)          +CRC32  (omitted if <1000 keys)║
║  │ INDEX  block   [BlockMeta{offset, first_key}…]    +CRC32                       ║
║  │ STATS  block   SstStats{puts,deletes,merges,sizes}+CRC32                       ║
║  │ SsTableInfo    flatbuffer: {first_entry,last_entry, index/filter/stats         ║
║  │                offset+len, compression, sst_type, filter_format}              ║
║  │ meta_offset:u64  version:u16   ← fixed 10-byte tail (read first, backwards)    ║
║  └───────────────────────────────────────────────────────┘                       ║
╚════════════════════════════════════════════════════════════════════════════════╝

Read: last 10 B → version+meta_offset → SsTableInfo → points at index/filter/stats.
get(k): bloom filter rules k out, else index binary-searches first_keys → ONE block.
```

Data block (V2, prefix-compressed, RocksDB-style): `[entry0]…[entryN][restart:u16…][num_restarts:u16] + CRC32`; restart points every 16 entries store a full key (enable binary search), entries between them delta-encode against the previous key.

Per-row (record) layout — MVCC, with tombstones and TTL:

```
┌────────┬──────────┬───────────┬────────────┬───────┬──────┬───────┬────────────┬────────────┐
│ shared │ unshared │ value_len │ key_suffix │ value │ seq  │ flags │ [expire_ts]│ [create_ts]│
│ varint │ varint   │ varint    │ var        │ var   │ u64  │ u8    │ i64        │ i64        │
└────────┴──────────┴───────────┴────────────┴───────┴──────┴───────┴────────────┴────────────┘
  shared = bytes in common w/ previous key (0 at a restart point)
  value  = OMITTED for tombstones (value_len=0)
  seq    = MVCC sequence number      flags = TOMBSTONE 0x1 | HAS_EXPIRE_TS 0x2 | HAS_CREATE_TS 0x4 | MERGE_OPERAND
```

This row encoding is exactly why the Java `KeyValue` exposes `.seq()`, `.createTs()`, `.expireTs()` — they come straight off the wire. WAL SSTs are identical except `sst_type=Wal` and `first_entry` is a sequence number, not a key.

#### Manifest (`manifest/<u64>.manifest`) — the root of truth

The LSM tree lives **here**, not in the SSTs. Framing = `[u16 big-endian version][flatbuffer ManifestV2]`. A new manifest is written on every state change (flush/compaction/checkpoint) via **CAS**; the highest id is the current state.

```
[version:u16 BE = 2] + flatbuffer ManifestV2 {
   manifest_id
   writer_epoch, compactor_epoch      ← single-writer FENCING tokens
   initialized                        ← clone-bootstrap flag
   ssts:       [CompactedSsTableV2]   ← id → SST references
   l0:         [CompactedSsTableView] ← overlapping, newest-first
   compacted:  [SortedRunV2]          ← non-overlapping sorted runs  }  the LSM tree
   segments:   [Segment]              ← optional (RFC-0024)
   checkpoints:[Checkpoint{id, manifest_id, expire_time_s, create_time_s}]
                                      ← incl. the 900s compactor checkpoints (§16.13)
   replay_after_wal_id, wal_id_last_seen   ← WAL-replay start on recovery
   last_l0_seq, last_l0_clock_tick, recent_snapshot_min_seq, sequence_tracker
   external_dbs:[ExternalDb]          ← clone lineage (the exactly-once restore path, §6.6)
}
```

This is what the checkpoint/clone/rescale machinery reads and pins (`writer_epoch`/`compactor_epoch` are the fencing tokens behind the single-writer invariant).

#### Compactions (`compactions/<u64>.compactions`) — compactor work state

Kept separate from the manifest so tracking in-flight jobs doesn't churn it. Framing = `[u16 BE version][flatbuffer CompactionsV1]`.

```
[version:u16 BE] + flatbuffer CompactionsV1 {
   compactor_epoch : u64              ← fencing token
   recent_compactions : [Compaction{
       id : ULID                      ← start time drives the GC low-watermark (§16.13)
       spec : Tiered/DrainSegment     ← inputs (L0 view ids, sorted-run ids)
       status : Submitted|Scheduled|Running|Compacted|Completed|Failed
       output_ssts : [CompactedSsTable]
       worker : {worker_id, last_heartbeat_ms} }]
}
```

#### GC boundary files (`gc/manifest.boundary`, `gc/compactions.boundary`)

Tiny files holding a single **monotonic integer** — the id below which manifest/compactions files have been GC'd. No flatbuffer. They don't exist on a fresh DB (hence the harmless `NotFound` logs at startup) until the first GC pass.

**The unifying pattern:** every file is **write-once, content-addressed by a monotonic/ULID id**; state advances by writing a *new* file and CAS-ing the manifest — never mutating in place. SSTs carry per-block CRC32; manifest/compactions rely on the object store for integrity and on CAS + `[version]` prefix for ordering/evolution. That immutability is what makes the object-store LSM viable and is the substrate the checkpoint (§6.6), clone-restore (§16.6), and GC (§16.13) logic all build on.

### 5D. Size limits — how big can one instance get?

**There is no explicit total-size limit in SlateDB.** No `MAX_DB_SIZE`, no cap on SST count, no configured ceiling exists in the source (v0.14.1). A single instance's capacity is bounded by your object store's capacity plus a few type-width limits on *individual* items and one practical scaling pressure (the manifest).

**Hard limits (from the wire format — real ceilings, not tunables):**

| Item | Max | Source |
|---|---|---|
| **Key size** | **65,535 bytes** (`u16`), and non-empty | binding `db.rs` |
| **Value size** | **~4 GiB** (`u32::MAX`) | binding `db.rs` |
| **Single data block** | **64 KiB** | block-internal restart offsets are `u16` (`block_v2.rs`) — why `SstBlockSize` (§5C) maxes at 64 KiB |
| **Items per collection crossing FFI** | `i32::MAX` (~2.1 B) | UniFFI `Vec` length is `i32` — a per-*call* cap, not a DB cap |

**Unbounded by the format:** total DB size, key count, and SST count. SST block *offsets* are `u64`, the manifest lists SSTs in a `Vec` (`i32::MAX` entries), and there is no aggregate cap — so an instance scales to **terabytes–petabytes**, whatever the bucket holds. That elastic, node-independent capacity is the whole point vs. local RocksDB (§2).

**The practical ceiling is the MANIFEST, not disk space.** The manifest lists *every live SST* (`ManifestV2.ssts[]`), is read on open, and is **rewritten whole on every state change** (flush/compaction/checkpoint) via CAS. So the cost that grows with DB size is *metadata management*, not storage:
- Millions of SSTs → a large manifest that must be serialized + CAS-written on each change and re-read on recovery. Bounded by memory + manifest write latency, not a hard limit.
- **Compaction is what keeps this in check** — it merges many small SSTs into fewer sorted runs, shrinking the live-SST count. A well-compacted multi-TB DB has a manageable manifest; an under-compacted one (compaction falling behind, §8) grows the manifest, L0 count, and read amplification. So "how big can it get" is really "can compaction keep up with your write rate."

**Per-file sizing defaults (tunable, not limits):** `l0_sst_size_bytes` 64 MiB (memtable freeze target), `max_sst_size` 1 GiB (compaction output cap), `max_unflushed_bytes` 1 GiB (in-memory write buffer before backpressure — a *memory* bound per §9, not a DB-size bound).

**For Flink keyed state:** per-key values < 4 GiB and keys < 64 KiB are trivially satisfied. Per-subtask DB size is effectively unbounded by SlateDB — it's gated by your S3 quota and by keeping compaction healthy. ⚠️ *Confidence:* the per-item limits are hard facts from the format; the "no total limit, manifest is the practical ceiling" conclusion is from reading the code (no cap exists) + the manifest lifecycle — **not** load-tested at multi-TB / millions-of-SSTs scale (that needs real object storage; see §11 unverified-at-scale).

---

## 6. Detailed Design: Keyed State Integration

### 6.1 Key encoding
SlateDB has **no column families** (unlike RocksDB), so multiplex all states into one keyspace. **Key group must be the leading bytes** so sorted-order range ops work:

```
[ keyGroup (1–2 bytes) ][ stateId ][ serialized key ][ namespace ]
   ^ leading, enables       ^ which ValueState/MapState/…
     contiguous key-group
     scans
```
`keyGroupPrefixBytes` = 1 if `maxParallelism <= 128`, else 2 (mirror Flink). Sorted LSM order ⇒ a *range* of key groups is one contiguous byte range → same trick Flink plays for rescale.

### 6.1a Value serialization — reuse Flink's `TypeSerializer` (✅ verified, `FlinkSerdeSlateDbE2E`)

SlateDB stores `byte[]`, no type info — exactly like RocksDB (§5B, §16.14). So serialize objects **the same way Flink's RocksDB backend does**: obtain a `TypeSerializer<T>` from `TypeInformation` and encode via `DataOutputSerializer` / `DataInputDeserializer` — the identical primitives `RocksDBValueState` uses internally. Don't hand-roll (never `ObjectOutputStream`).

```java
TypeSerializer<T> ser = TypeInformation.of(MyPojo.class).createSerializer(execConfig);
// reuse buffers per operator (single-threaded per subtask) — no per-call alloc
DataOutputSerializer out = new DataOutputSerializer(64);
DataInputDeserializer in  = new DataInputDeserializer();
byte[] toBytes(T v)  { out.clear(); ser.serialize(v, out); return out.getCopyOfBuffer(); }  // → db.put
T fromBytes(byte[] b){ in.setBuffer(b, 0, b.length); return ser.deserialize(in); }           // ← db.get
```

Verified by `FlinkSerdeSlateDbE2E`: Flink's `PojoSerializer` round-trips a POJO (incl. object RMW: read→mutate→write) through SlateDB byte-for-byte across 1000 keys. Caveats found by running:
- **Use POJOs with a no-arg ctor** (→ Flink `PojoSerializer`), not Java `record`s — Kryo can't serialize records on JDK 16+ (§16.10), and this path runs on JDK 22+/25.
- **Use mutable collections** (`ArrayList`, not `List.of(...)`) for collection fields — Flink routes them through Kryo, which deserializes into a mutable list; an immutable one round-trips on read but breaks on a later `.add()`.
- **Schema evolution is NOT free.** Flink's RocksDB backend migrates state across savepoints via `TypeSerializerSnapshot`; SlateDB isn't wired to that machinery, so you version your own schema or persist the serializer snapshot. Part of the "reimplement the state-backend integration layer" cost (§15).
- **Order-preservation:** Flink serializers aren't guaranteed byte-order-preserving — fine for point `get`/`put`, a caveat if you rely on `scan` ordering by value.

**Avoiding repeated deserialization on hot reads.** SlateDB's block cache (§9) caches decoded SST *blocks* (bytes) — it cannot cache your Java object, so every `get` still pays FFI copy + `deserialize` even on a cache hit. From §16.14 that's only a **few µs**, and the block cache already removes the expensive part (the ~1–10 ms S3 read) — so **relying on SlateDB's block cache is the right default**; re-deserializing per hit is µs-noise. Only if profiling shows deserialize is a genuine hotspot (same key read at very high frequency) add a **bounded on-heap object cache** (`key → live object`) in front — write-through for read-heavy, write-back-at-barrier for RMW-heavy (that's the §13 hot tier). It doesn't *defeat* SlateDB's cache (different layers: bytes vs. objects), but at full size the two double-cache the hot set — so if you add it, shrink SlateDB's block cache (§9.1a) to reclaim off-heap. Don't build it preemptively.

### 6.2 Write/read path & the sync/async boundary
The one boundary that's yours: **`CompletableFuture` vs. Flink's single-threaded-per-subtask operator model.** Two shapes:

| Operator does… | Use | Why |
|---|---|---|
| RMW keyed aggregation, timers, per-key correctness | **`KeyedProcessFunction`** (blocking) | single-threaded per key ⇒ no races |
| Pure lookup / enrichment / **idempotent or associative** writes | **`RichAsyncFunction` + `AsyncDataStream`** | non-blocking, high throughput |

**Blocking `KeyedProcessFunction` (matches "SlateDB IS my keyed state"):**
```java
byte[] cur = await(db.get(k));    // BLOCKS ms-scale remote read
byte[] nxt = update(cur, e);
await(db.put(k, nxt));            // safe: single-threaded per key
```
RMW here is correct because Flink processes one element at a time per subtask. Cost = throughput. Mitigate with an in-memory write-back cache for the hot set, `WriteBatch` coalescing, and flush-at-barrier.

**Read-your-writes is guaranteed within the instance, before any flush** (⚡ verified, §16.15). The RMW above depends on the `get` seeing the prior `put` even though nothing was flushed — this holds because a write lands in the **mutable memtable synchronously** and reads go newest→oldest (memtable → imm memtables → SSTs), so unflushed writes are found first; MVCC sequence numbers make overwrites/deletes resolve to the latest. Crucially, **visibility is independent of durability**: even `await_durable=false` writes are immediately readable by the same instance (flush/checkpoint only govern crash-safety + what a *separate* reader/clone sees, not your own reads). So the write-back-cache mitigation is a throughput optimization, **not** a correctness requirement — SlateDB itself already gives you consistent reads pre-flush.

**`RichAsyncFunction`** keeps N requests in flight → throughput, **but two in-flight updates to the same key can interleave, and there are no timers / no Flink keyed state inside it.**
- ✅ read-through lookups, idempotent/blind writes.
- ❌ **unsafe for RMW** unless the write is associative → use **`db.merge`** (below), which sidesteps the race entirely.

**Third path (both correctness and non-blocking):** fire `db.get()`, schedule the continuation back on the task's `MailboxExecutor`, drain in-flight requests before `snapshotState`. This is exactly what Flink 2.0 async state does internally. ⚠️ **On Flink 1.20 this cannot be done from a `KeyedProcessFunction`** — there is no `getMailboxExecutor()` on `RuntimeContext` (§0). It requires a **custom operator** (`AbstractStreamOperator` + `OneInputStreamOperator`), which receives the `MailboxExecutor` at setup. And since 1.20 has no ForSt, "reconsider ForSt" means "upgrade to 2.0 first" (§0).

**Race-free RMW options** (verified shipped in binding): `db.merge(k, operand)` for associative aggregations (counters, sums, set-union), or `db.begin(IsolationLevel)` transactions. `merge` makes Option-2 (`RichAsyncFunction`) viable for a whole class of aggregations.

### 6.3 Checkpoint integration (`CheckpointedFunction`)
```java
void snapshotState(FunctionSnapshotContext ctx) {
    // ⚠️ MUST be a MEMTABLE flush, not plain flush(). VERIFIED BY RUNNING (see §16): Db.flush() with WAL
    // enabled is FlushType::Wal (WAL only) — it does NOT advance the manifest, and a checkpoint pins the
    // MANIFEST. flush()+checkpoint pins a manifest WITHOUT this data → silent state loss on restore.
    await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));   // memtable → L0 → manifest advances
    CheckpointCreateResult cp = await(admin.createDetachedCheckpoint(checkpointOptions));
    String cpId = cp.id();           // verified: id is a String (not UUID); also cp.manifestId()
    handleState.clear();             // UNION operator list state
    handleState.add(new Handle(dbPath, cpId, keyGroupRange));
}
void initializeState(...) { /* MUST clone-from-pinned-checkpoint, NOT plain writer reopen — §6.6. Writer reopen resurrects post-barrier WAL. */ }
```
- Store the handle triple `(dbPath, slateCheckpointId, keyGroupRange)` — **this is your equivalent of Flink's `KeyedStateHandle`**.
- Use **union operator list state**, not keyed state: on restore every new subtask gets the *full* list of old handles and self-selects the ones it must pull from.
- **DB naming:** name by a stable identity (`s3://bucket/<jobId>/<operatorUid>/<uuid>/`), **NOT by `subtaskIndex`** (reassigned on rescale).

### 6.4 Rescale (one-DB-per-subtask case)
Emulates `RocksDBIncrementalRestoreOperation.restoreWithRescaling`:
```java
KeyGroupRange target = computeKeyGroupRangeForOperatorIndex(maxP, newP, subtaskIndex);
List<Handle> sources = allHandles.filter(h -> intersects(h.range, target));  // from union state

Handle base = sources.max(byOverlapWith(target));          // 1. base = largest overlap
// 2. Adopt base via shallow clone at its pinned checkpoint — near-zero copy
CloneSourceSpec spec = CloneSourceSpec.withCheckpoint(base.dbPath, base.slateCpId); // slateCpId: String
// optional physical clip: set projection on the SOURCE SPEC (not the builder) to clone only your slice — §14.2
// spec = spec.withProjectionRange(targetAsKeyRange);
CloneBuilder cb = admin.createCloneBuilderFromSource(spec);
cb.withClonePath(myNewPath); cb.withObjectStore(store);
await(cb.build());
Db db = await(new DbBuilder(myNewPath, store).build());
// 3. Merge every OTHER source: scan only the key-group slice in `target`, copy in
for (Handle s : sources) { if (s==base) continue;
    KeyGroupRange slice = intersection(s.range, target);
    DbIterator it = await(srcDb.scan(keyRangeFor(slice)));   // scan-and-write (no SST ingest)
    /* put each kv into db */
}
// 4. Clip out-of-range key groups — see below
```
**Clipping without range-delete:**
- **Logical clip = do nothing** — every read/write is already scoped to a key group you own (`assignToKeyGroup(key) ∈ target`), so dragged-in key groups are never addressed. Correct immediately, free. **This is mandatory, not optional** (no `deleteRange` exists).
- **Physical reclaim** — options: `withProjectionRange` on the clone to only bring your slice in the first place; or background scan-and-tombstone (point deletes); or let it ride and GC on next full rebuild.

Key property: each key group is owned by exactly one old subtask → **no key-level merge conflicts, no last-writer-wins** — clean partition copy.
- **Upscale:** target usually inside one old range → clone base, copy at most a sliver. Cheap.
- **Downscale:** target = union of several old ranges → clone biggest, scan-copy the rest. **This is where byte movement shows up.**

### 6.5 Clone lifecycle caveat (GC)
Clones are **not self-contained**: they read the parent's SSTs at the parent path and pin them via the checkpoint. Therefore:
- Old pre-rescale DBs **cannot be blindly deleted** while any clone references their SSTs → **GC must be checkpoint-reference-aware**.
- Repeated rescales build **chains of clones** → periodic full compaction/rewrite to collapse the chain, else read amplification and cross-path fan-out grow.
- Configure via `garbage_collector_options`; `Admin.runGcOnce` is the manual trigger.

### 6.6 Exactly-once semantics — the trap (VERIFIED: restore path is load-bearing)

SlateDB writes to S3 are durable **independently** of Flink checkpoints. After a restore, the DB on S3 contains writes that happened *after* the last Flink barrier. **How you reopen decides whether exactly-once holds — and the two open modes behave oppositely (verified from source):**

| Open mode | Post-checkpoint writes | Verified in | Safe as authoritative state? |
|---|---|---|---|
| **Checkpoint-pinned READER** (`DbReaderBuilder.withCheckpointId`) | ✅ **excluded** — `replay_new_wals=false` ⇒ WAL replay bounded by manifest `next_wal_sst_id`, not a storage scan. Test `should_get_from_checkpoint` confirms. | `db_reader.rs` | ✅ clean point-in-time snapshot |
| **Normal WRITER reopen** (`DbBuilder.build` on the existing DB) | ❌ **RESURRECTED** — `WriterFencer::fence` sets the replay upper bound via `next_wal_sst_id` **scanning object storage**, so all post-barrier WAL replays into new L0. | `fence.rs` | ❌ **violates exactly-once** |

**Consequence — the restore procedure is mandatory, not optional:** on `initializeState` you **must NOT** plain-reopen the writer on the existing DB (that replays post-barrier WAL). Instead **clone from the pinned checkpoint** (`CloneSourceSpec.withCheckpoint`) → a fresh writable DB rooted at exactly the checkpoint's manifest, **WAL not carried over** → then write to the clone. Clone-on-restore is therefore not just a rescale tool (§6.4) — **it is the mechanism that makes exactly-once possible at all.** A direct writer reopen is unsafe.

Alternative: **idempotent/`merge`-only writes** that are replay-safe, so resurrection doesn't matter.

> ✅ **VERIFIED SAFE (§12.8):** `clone.rs` reads the parent manifest at `checkpoint.manifest_id` and `copy_wal_ssts` bounds WAL copy to the checkpoint's `[replay_after_wal_id+1, next_wal_sst_id)` — post-checkpoint WAL is **not carried over**. Test `should_clone_from_checkpoint` confirms. **So clone-from-pinned-checkpoint gives a clean point-in-time restore, and the exactly-once procedure above is sound** — for the single-source case (`clone_sources.len()==1`, i.e. restore & rescale base-adoption). Residuals: the post-checkpoint-WAL exclusion is inferred from bounds logic (no targeted test), and rests on `create_checkpoint` snapshotting `next_wal_sst_id` correctly.

Flink's integrated backends hide all of this; here it's your responsibility.

---

## 7. Compaction Strategy

SlateDB's compactor runs **embedded in the writer process by default** — i.e. **inside the TaskManager JVM**, on the UniFFI Tokio threads.

### 7.1 Deployment options

| | Embedded (default) | Standalone sidecar | Distributed (RFC 0025) |
|---|---|---|---|
| Setup | none | fleet of `slatedb run-compactor` processes | — |
| CPU/IO on TM | **competes with stream work + N compactors/TM** | isolated | isolated |
| Shipped in 0.14.1? | ✅ | ✅ (CLI) | ❌ **roadmap only** |
| Best when | low write volume, simplicity | high write volume | many DBs (future) |

- **Disable the embedded worker** to offload: set `compactor_options.worker = null`. This keeps the *coordinator* (scheduler + manifest committer) in-process but stops the *worker* (CPU work); run `slatedb --path <db> run-compactor` as the external worker. ⚠️ omitting the `worker` key defaults it to `Some(...)` — you must explicitly null it.
- ⚠️ `compactor_options` is **mutually exclusive** with `DbBuilder::with_compactor_builder` — setting both errors. Pick one path.
- **Single-writer invariant:** exactly one process commits manifest updates. Do **not** run embedded + standalone compactor on the same DB simultaneously.

### 7.2 Why shard-per-bucket helps compaction
One-DB-per-subtask = N compactors per TM *and* N shifting compactor targets on every rescale (paths change with clone UUIDs). Shard-per-bucket = fixed, rescale-invariant DB paths → a fixed set of `run-compactor` targets → no re-orchestration.

### 7.3 Does compaction block writes?
**Not directly.** Compaction reads immutable SSTs and writes new ones; writes flow to WAL/memtable/new-L0 on a separate path. Writer and compactor coordinate via **CAS + epoch fencing** on the manifest (retry on conflict, no mutual blocking; disjoint parts). **This is why a standalone compactor can safely run against a live Flink writer.**

**Provenance (SlateDB's own source/tests, not our test):**
- **`rfcs/0002-compaction.md:383`** states the compactor runs *"alongside the main writer-reader in the same process"* (or a separate process) — only possible because they don't block each other.
- The **CAS coordination protocol** (`rfcs/0002-compaction.md:184–190, 331–333`): writer and compactor each update the manifest by optimistic **compare-and-swap keyed on `writer_epoch`/`compactor_epoch`**; a CAS conflict causes a **retry, never a wait** — there is no shared lock.
- Verified by SlateDB's test **`test_compactor_compacts_l0`** (`slatedb/src/compactor.rs:1602`): writes to a live `Db` **with the embedded compactor active**, awaits compaction, then reads every key back intact. (Plus the fencing tests, e.g. `test_should_fail_when_compactions_store_fences_compactor`.)
- *We did not add our own test here:* a single-machine test can't cleanly separate "compaction blocking" from **L0 backpressure** (below) — an over-aggressive flush rate induces backpressure stalls that look like blocking but aren't. SlateDB's in-tree tests already prove the property against the real coordination code.

**But writes DO block via L0 backpressure when compaction falls behind:**
```
compaction can't keep up → L0 SST count climbs → hits threshold → writes PAUSE
→ processElement blocks on db.put/merge → operator stalls → Flink backpressure
→ checkpoint slows / times out
```
This is the pipeline-wide failure mode. See §8 for the exact knobs.

Separately, `await_durable=true` (default) blocks each write until durable in object storage (a WAL flush to S3, ms-scale). You generally **cannot** set it to `false` in an exactly-once design (you need writes durable before checkpoint completes) — so you pay that latency; `merge` + write batching mitigate.

---

## 8. Write Backpressure & Tuning (verified defaults)

`Settings::default()`:

| Field | Default | Meaning / action |
|---|---|---|
| **`l0_max_ssts_per_key`** | **8** | ⚠️ **PRIMARY write-stall driver** — writes pause after 8 uncompacted L0 SSTs on a key. Raise (24–48) to survive bursts; cost = point-read amplification |
| `l0_max_ssts` | 8 | total L0 cap (manifest bookkeeping); indirectly stalls flushes |
| `max_unflushed_bytes` | **1 GiB** | memory backpressure; also the dominant memory consumer (§9) — cut to 64–128 MiB per DB |
| `l0_sst_size_bytes` | 64 MiB | memtable freeze size |
| `max_wal_flushes_before_l0_flush` | 4096 | force-freeze after N WAL flushes |
| `l0_flush_parallelism` | 4 | parallel memtable→L0 flushers |
| `flush_interval` | 100 ms | WAL flush cadence to object store |

`CompactorOptions::default()`:

| Field | Default | Action |
|---|---|---|
| `poll_interval` | **5 s** | how often compactor checks manifest. With `l0_max_ssts_per_key=8`, a burst can fill 8 SSTs *within one poll window* → up to ~5 s stall. Lower to ~1 s to react faster (cost: N× manifest GETs with N DBs) |
| `max_concurrent_compactions` | 4 | lower if embedded (stop N compactors saturating TM CPU) |
| `commit_compacted_interval` | 1 s | |
| `manifest_update_timeout` | 300 s | |
| `worker_heartbeat_timeout` | 30 s | reclaim stalled worker's job |

**The critical interaction:** `l0_max_ssts_per_key=8` (low, tuned for a steady-state service) × `poll_interval=5s` = bursty stream ingest can stall writes for up to 5 s. **`l0_max_ssts_per_key` is the single most important knob** to keep compaction from stalling your Flink pipeline.

**Deadlock-safety invariant:** the default `SizeTieredCompactionScheduler` sets its L0 trigger *below* `l0_max_ssts` so compaction fires before backpressure. If you replace the scheduler, maintain this invariant.

**Post-rescale spike:** after a clone/union merge, `l0_max_ssts` can be temporarily exceeded (merged sources' L0 SSTs) → expect transient backpressure right after a downscale until compaction drains. Consider temporarily raising the cap or forcing a compaction before resuming processing.

Set via the dynamic JSON setter:
```java
Settings s = new Settings();
s.set("l0_max_ssts_per_key", "32");
s.set("max_unflushed_bytes", "134217728");   // 128 MiB
s.set("compactor_options.poll_interval", "...");
s.set("compactor_options.max_concurrent_compactions", "2");
DbBuilder b = new DbBuilder(path, store); b.withSettings(s);
```

---

## 9. Memory Model & TaskManager Sizing

### 9.1 SlateDB memory is 100% native / off-heap
The Java objects (`Db`, `WriteHandle`, returned `byte[]`) are thin FFM/Panama handles (KB) — the binding uses `java.lang.foreign` (**not** JNA, despite the README's wording; verified: 240 `foreign` refs, class version 66 = **Java 22+ required to run**; see §16.3). The real memory is **native Rust allocations the JVM never sees and GC never reclaims.**

| Component | Location | Bound by | Default |
|---|---|---|---|
| Block cache (in-mem SST blocks) | **off-heap** | `DbCache.max_capacity` | **512 MiB** (`DEFAULT_BLOCK_CACHE_CAPACITY`) |
| Metadata cache (indexes + bloom filters) | **off-heap** | split-cache meta capacity | **128 MiB** (`DEFAULT_META_CACHE_CAPACITY`) |
| Unflushed buffers (memtable+WAL) | **off-heap** | `max_unflushed_bytes` | **1 GiB** |
| Bloom filters (see §5B) | **off-heap** (in meta cache) | `bits_per_key` | 10 bits/key, default-on ≥1000 keys |
| Scan read-ahead | **off-heap**, pinned during scan | `ScanOptions.read_ahead_size` | one block |
| Object-store cache (optional) | **local disk** | `object_store_cache_options` | off; 16 GiB if enabled, 4 MiB parts |

**The default `DbBuilder` auto-installs a populated `SplitCache`** (confirmed in `db/builder.rs`: `default_db_cache()` → `SplitCache::new().with_block_cache(DEFAULT_BLOCK_CACHE_CAPACITY).with_meta_cache(DEFAULT_META_CACHE_CAPACITY)`). Only `withDbCacheDisabled()` sets it to None.

**Feature-gate status — RESOLVED (✅ cache is ON in the published JAR).** The caches are `#[cfg]`-gated on the `foyer`/`moka` Cargo features. Verified: `bindings/uniffi/Cargo.toml` declares `slatedb = { features = ["all"] }`, and core `slatedb/Cargo.toml` defines `all = [.., "foyer", "moka", ..]` (core `default` is also `["aws","foyer"]`). **So the published `io.slatedb:slatedb-uniffi` JAR ships with both cache backends compiled in → `default_db_cache()` installs a populated `SplitCache` (foyer-backed by default).** You are on the **cache-ON** path below. Both `newFoyerCache(...)` and `newMokaCache(...)` are available if you construct a cache explicitly.

```
per-DB native footprint (defaults):
  block 512 MiB + meta 128 MiB + unflushed 1024 MiB ≈ 1.66 GiB   ← YOUR CASE (cache ON, confirmed)
  unflushed 1024 MiB                               ≈ 1.0  GiB   (cache OFF — not applicable; would need withDbCacheDisabled())
```

### 9.1a Foyer vs Moka — the two block-cache backends

The "block cache" above (`DbCache`) has **two interchangeable implementations**, both compiled into the published JAR. They cache the same thing (decoded SST **blocks, index, and filters**, keyed by SST id) and implement the same `DbCache` trait — the choice is an operational trade-off, not a feature difference. ⚠️ Don't confuse this **in-memory block cache** with the **object-store disk cache** (§9A) — different layer.

| | **Foyer** (`newFoyerCache`) | **Moka** (`newMokaCache`) |
|---|---|---|
| Default? | ✅ **Yes** — `default_db_cache()` is foyer-backed (core `default = ["aws","foyer"]`) | No — opt-in via `DbCache.newMokaCache(...)` |
| Tiering | **Hybrid: memory + local disk** (`FoyerHybridCache` with a disk device) — can spill cached blocks to NVMe instead of evicting | **Memory only** |
| Lineage | Rust-native, purpose-built for hybrid caching | Port of Java's Caffeine (W-TinyLFU eviction) |
| Miss coalescing | dedup-aware `fetch` (concurrent misses for one key share a single load) | via the trait's default loader |

**Which to pick for Flink:**
- **Moka** — simplest, purely in-memory. Prefer it when you do **not** want SlateDB touching local disk (the §9A contention concern — e.g. a TaskManager where RocksDB is the hot tier in the §13 hybrid design). This is what `FlinkShardPerBucketParallelE2E` used (shared moka cache per subtask, §16.12).
- **Foyer** — default; choose its **hybrid mode** when you want a large block cache that overflows to local NVMe rather than evicting hot blocks (trades §9A disk for fewer cold S3 reads). Adds a disk-space/IOPS cost — size it against everything else on that volume (§9A).
- Either way the cache is **shareable across many DB instances** and collision-safe (§4.1, §12.7), which is what collapses the per-instance memory multiplier.

### 9.2 Which Flink memory section — NONE that Flink manages
Flink TaskManager memory: `Total Process = JVM Heap + Managed Memory + Framework/Task Off-heap + Network + JVM Overhead + Metaspace`.

- ❌ **Not JVM Heap** — native; heap size doesn't bound it, GC doesn't see it.
- ❌ **Not Managed Memory** — RocksDB (integrated) is sized *out of* Managed Memory. **SlateDB has no such integration → Flink will not carve its footprint out of any section.**
- ✅ Comes out of raw process native memory, at best loosely under JVM Overhead (default ~10%, far too small).

**Consequence:** `container RSS = Flink's full budget + SlateDB's native footprint` → the cgroup/kernel **OOM-kills the TaskManager** with **no `OutOfMemoryError`, no heap dump, no JVM metric anomaly** — just `exit 137`. This is the hardest OOM class to diagnose and is the *default* outcome without manual reservation.

### 9.3 OOM risk, spilling, and limits — direct answers
- **Risk:** structurally high and *silent*. Compounds with one-DB-per-subtask: ~1.66 GiB/DB × slots (8 slots ≈ 13 GiB unaccounted native).
- **Does SlateDB spill to disk under memory pressure?** **No.** Its disk mechanisms (object-store read cache; flush-to-S3 durability) are not memory safety valves. Under pressure it **evicts cache** (re-fetch from S3 later) and **applies write backpressure** — it does not page in-memory structures to local disk to stay under a limit.
- **Can you limit it?** Yes, per consumer — **but limits are soft.** The block cache (`moka::future::Cache`/foyer) **evicts asynchronously**, so a 512 MiB cache can transiently exceed 512 MiB; accounting is only as good as `CachedEntry::size()` (excludes allocator overhead); native allocator fragmentation adds more. `max_unflushed_bytes` is firmer (a backpressure threshold) but still not an instantaneous cap.

### 9.4 Sizing recipe
```
budgeted per-DB   = block_cache + meta_cache + max_unflushed_bytes + scan_readahead
container reserve = budgeted per-DB × DBs_per_TM × 1.25   (soft-limit + fragmentation slack)
```
1. **Reserve native headroom Flink won't touch** — inflate `taskmanager.memory.jvm-overhead.{min,max,fraction}` or reduce Total Flink Memory so Flink thinks it has less RAM than the container; the gap is SlateDB's. Ensure K8s pod limit = Flink Total Process + SlateDB reserve.
2. **Cut the two dominant defaults:** `max_unflushed_bytes` 1 GiB → 64–128 MiB (biggest win); block cache 512 MiB → small or `withDbCacheDisabled()` on many-DB TMs.
3. **Minimize DB count per TM** → shard-per-bucket, or fewer slots.
4. **Monitor container RSS directly** (not JVM metrics); alert on RSS→cgroup limit; watch cache size / unflushed bytes via SlateDB's metrics recorder.
5. Apply the **~1.25 safety factor** for soft-limit overshoot + fragmentation — validate with a soak test (the factor is a rule-of-thumb, not a SlateDB measurement).

---

## 9A. Local Disk Usage & Contention

The mirror image of §9 (memory): SlateDB uses **almost no local disk by default**, because its primary data lives in object storage. Three disk paths exist; only one is significant, and it's opt-in.

| Path | Default | Local disk |
|---|---|---|
| Primary data (SSTs, WAL, manifest) | — | **None locally** — all in object storage. SlateDB's core premise; the durable path never touches local disk. |
| Object-store disk cache (`object_store_cache_options`) | **OFF** (`root_folder = None`) | **0 by default.** When enabled, caches SST parts locally to avoid S3 reads. |
| Foyer hybrid-cache disk tier | not configured | 0 unless you explicitly build a `FoyerHybridCache` with a disk device. |

**Object-store disk cache defaults (verified `impl Default for ObjectStoreCacheOptions`, source):**
```rust
root_folder: None,                                    // OFF unless set
max_cache_size_bytes: Some(16 * 1024 * 1024 * 1024),  // 16 GiB cap WHEN enabled (usize::MAX on 32-bit)
part_size_bytes: 4 * 1024 * 1024,                     // 4 MiB parts
cache_puts: false,
preload_disk_cache_on_startup: None,
scan_interval: Some(3600s),                           // rescans dir hourly to rebuild the evictor map
max_open_file_handles: 1000,
```

### Can SlateDB compete for disk with other Flink processes?
- **By default: no.** SlateDB writes no meaningful local disk → no contention with RocksDB state, Flink `io.tmp.dirs`, locally-staged checkpoints, logs, or shuffle/spill.
- **If you enable the disk cache: yes, and significantly.** Three cautions:
  1. **16 GiB default cap is per-DB.** Setting `root_folder` alone defaults the cap to 16 GiB; with one-DB-per-subtask that's **16 GiB × N slots** aimed at the same disk (8 slots → 128 GiB budget). **Always set `max_cache_size_bytes` explicitly.**
  2. **Contends with what Flink needs disk for** — especially in the hybrid design (§13) where RocksDB *is* the hot tier: RocksDB SSTs, local checkpoint dir, `io.tmp.dirs`. Shared volume → contention for **space and IOPS**; a full disk can crash the TM or fail RocksDB flushes.
  3. **Cap is soft** — eviction is driven by a directory rescan every `scan_interval` (1 h default); on-disk usage can drift above the cap between scans. Budget headroom / shorten `scan_interval` if disk-tight.

### Object-storage space reclaim lags compaction by ~15 min (⚡ verified, §16.13)
Distinct from local disk: this is your **object-store** footprint. Compaction rewrites the manifest to drop old
SSTs but does not delete the files; a background GC (on by default) deletes orphans later. **Verified by running:
physical deletion is gated by a hardcoded 900s (15-min) compactor checkpoint, NOT by `min_age`** — the compactor
pins each just-compacted generation for 15 min so it can't be deleted "out from underneath the writer." So
steady-state object-store usage ≈ live state **+ ~15 min of post-compaction orphans** (plus WAL). Budget for it;
the 900s window is not tunable from the Java binding. Details and the runnable proof: §16.13.

### Recommendations
- **Default posture: leave the disk cache OFF.** The in-memory block cache (§9, foyer, on by default) already absorbs hot reads. Enable disk cache only if cold-read S3 latency/cost is a *measured* problem.
- **If enabled:** set `max_cache_size_bytes` explicitly; place `root_folder` on a **separate volume** from RocksDB and Flink tmp/checkpoint dirs.
- **Hybrid design (§13):** sharpest contention point — hot-tier RocksDB + SlateDB disk cache on one disk will fight. Separate volumes, or skip the disk cache.
- **Standalone compactor (§7):** runs on its own host → its disk cache is isolated from the TM — another point for the sidecar model.

---

## 10. Configuration Reference

### SlateDB (via `Settings.set("<dotted.key>", "<value>")`)
```
l0_max_ssts_per_key            = 32          # raise from 8 to survive bursts
l0_max_ssts                    = 32
max_unflushed_bytes            = 134217728   # 128 MiB (from 1 GiB)
l0_sst_size_bytes              = 67108864    # 64 MiB
flush_interval                 = ...
compactor_options.worker       = null        # to offload compaction to sidecar
compactor_options.poll_interval          = ...   # lower toward 1s for faster reaction
compactor_options.max_concurrent_compactions = 2 # lower if embedded
garbage_collector_options      = ...         # reference-aware GC for clone chains
object_store_cache_options.root_folder = ... # optional local disk read cache
```
Plus builder-level: `withDbCacheDisabled()` / `withDbCache(cache)` with explicit `max_capacity`.

### Flink TaskManager (`flink-conf` / config)
```
taskmanager.memory.jvm-overhead.min / max / fraction   # carve out SlateDB native reserve
taskmanager.memory.task.off-heap.size                  # or reserve here
# reduce Total Flink Memory / Managed Memory to leave native room
# K8s: pod memory limit = Flink Total Process Memory + SlateDB native reserve
```

---

## 11. Risks Summary

| Risk | Severity | Mitigation |
|---|---|---|
| Silent container OOM (native memory unaccounted) | **High** | §9.4 reserve + trim + RSS monitoring |
| Write stall → checkpoint timeout (compaction lag) | **High** | raise `l0_max_ssts_per_key`, sidecar compactor, lower `poll_interval` |
| **Exactly-once — writer reopen resurrects post-barrier WAL (VERIFIED, `fence.rs`); clone-restore is the fix (VERIFIED safe, `clone.rs`)** | **High (mechanism now sound)** | **MUST clone-from-pinned-checkpoint on restore, never plain writer reopen** (§6.6); or idempotent/`merge`-only writes. Both reader-pinned and single-source clone paths confirmed to exclude post-checkpoint WAL (§12.8) |
| **Failover fencing window — zombie writer keeps writing until next CAS fails (VERIFIED, manifest RFC)** | **High** | only neutralized by pinned-checkpoint restore (above); coupled to §12.8 |
| **Pre-1.0 on-disk/manifest format stability** (0.14.1) — a format change could make checkpointed state unreadable on upgrade | **High (operational)** | pin exact SlateDB version; plan state migration for any upgrade; no downgrade path |
| **Zero production precedent** — SlateDB-behind-Flink is unproven; every hand-rolled part (rescale merge, clone-chain GC, checkpoint coord) is battle-untested | **High** | prototype + soak test before trusting; prefer §13 hybrid where SlateDB is a cold side-store |
| **S3 tail latency (p99/p999 = 100s ms–s), not median** drives checkpoint timeouts on blocking reads | **High** | keep cold tail rarely read; async path; generous checkpoint timeout; benchmark before committing |
| **S3 request cost unmodeled** — N manifest polls/5s + compaction + per-miss GET | Med–High | model $ before committing; may dwarf compute savings |
| Rescale byte-copy cost (downscale) + clone-chain GC | Med | shard-per-bucket; reference-aware GC; periodic compaction |
| Internal Flink API drift (`KeyGroupRangeAssignment`) | Med | pin Flink version; treat as operational, not `@Public` |
| Building the async-state mailbox dance | Med | if you reach here, reconsider ForSt |
| Runtime-internal / roadmap features (distributed compaction, DB split/merge) | Low | don't design against RFC-only features |
| **[Hybrid]** Data loss if hot cleared before cold write durable | **High** | write-cold-before-clear-hot ordering (§13.4) |
| **[Hybrid]** Cold-miss read blocks task thread → checkpoint timeout | **High** | ensure cold tail is rarely read; else async/mailbox path (§13.6) — signals wrong tier boundary |
| **[Hybrid]** Tiers disagree on key ownership after rescale | **High** | identical `maxParallelism`/key groups across both tiers (§13.5) |
| **[Hybrid]** Timer explosion (one demotion timer/key) | Med | coalesce into time-bucket timers (§13.7) |

---

## 12. Open Questions (verify before building)

1. ~~**Java accessor names**~~ — ✅ **RESOLVED.** `CheckpointCreateResult` is `uniffi::Record { id: String, manifest_id: u64 }` → Java `r.id()` (a **String**, not `UUID`) and `r.manifestId()`. `KeyValue` is `uniffi::Record { key: byte[], value: byte[], seq, create_ts, expire_ts }` → `.key`/`.value`. `KeyRange` construction verified (§14.5). See §5A.
   - ⚠️ **Remaining sub-item:** `CloneSourceSpec` exact Java construction — used by `createCloneBuilderFromSource(source)`, defined in `types.rs`; Rust form is `withCheckpoint(path, uuid)` / `new(path)`, but the exact Java binding shape (Record vs. constructor, and that checkpoint id is a **String** on the Java side) is not yet captured. Confirm from the JAR.
2. ~~**Published JAR cache feature flags**~~ — ✅ **RESOLVED.** `bindings/uniffi/Cargo.toml` uses `slatedb = { features = ["all"] }`; core `all` includes both `foyer` and `moka` (core `default` = `["aws","foyer"]`). **The JAR ships with the in-memory cache ON** → per-DB native ≈ **1.66 GiB** at defaults (foyer-backed `SplitCache`). Cache-off (~1 GiB) only if you call `withDbCacheDisabled()`. See §9.1.
3. **`with_compactor_builder` in the Java binding** — exists in Rust; unconfirmed whether exported to Java (likely Rust-only).
4. **`Admin` construction in Java** — `AdminBuilder(path, store).build()` (sync) confirmed at Rust export level; confirm the exact generated Java signature.
5. **Fragmentation safety factor (~1.25)** — a native-allocator rule-of-thumb, not a SlateDB measurement. Validate with a soak test under real workload.
6. **Concurrent standalone-compactor + live-writer fencing details** — CAS + epoch fencing confirmed; the precise `.compactions` claim protocol is partly RFC-0025 (roadmap). Confirm shipped behavior before relying on multi-worker compaction.
8. ~~**SHOWSTOPPER — clone-from-checkpoint WAL exclusion**~~ — ✅ **RESOLVED (safe).** Verified in `clone.rs`: clone reads the parent manifest at `checkpoint.manifest_id` (frozen snapshot), and `copy_wal_ssts` bounds WAL copy to `[replay_after_wal_id+1, next_wal_sst_id)` **from the checkpoint's state** — post-checkpoint WALs fall outside `next_wal_sst_id` and are **not carried over**. Test `should_clone_from_checkpoint` confirms the clone sees only checkpoint-era data. **⇒ Exactly-once restore via clone-from-pinned-checkpoint is sound.** Three residuals to be aware of (not blockers, but unverified one layer down): (a) no test specifically plants a *post-checkpoint WAL* and asserts exclusion — inferred from bounds logic; (b) rests on `create_checkpoint` correctly snapshotting `next_wal_sst_id` (not separately verified); (c) the clean WAL-copy path is guarded by `clone_sources.len() == 1` — single-source (restore, rescale base-adoption) is confirmed clean; multi-source merge may differ.
9. **Failover fencing timing** — confirmed a zombie writer can persist writes until its next CAS fails (write-time detection). Confirm the worst-case window against your Flink failover timing; neutralized only by pinned-checkpoint restore (§6.6, coupled to #8).
10. **Latency + S3-cost benchmark** — no measurement done; "ms reads" is median-optimistic (p99/p999 far higher). Benchmark real read latency (incl. tail) and model S3 request cost under representative load before committing.

7. ~~**Shared-cache key namespacing**~~ — ✅ **RESOLVED (safe to share).** Verified in `slatedb/src/db_cache/mod.rs`: `CachedKey { scope_id: u64, sst_id, block_id }`. Each `Db` opened against a shared `DbCache` gets its own `DbCacheWrapper` with a unique `scope_id` (atomic counter from 1), applied to every key via `with_scope(...)`. Source comment: *"ensures that multiple Db instances sharing the same underlying cache do not collide."* Test `test_cache_wrapper_scopes_keys` confirms. **So sharing one `DbCache` across all N/P shards is correctness-safe** — the memory-multiplier mitigation (§4.1) holds. (Only the bare `CachedKey::from((sst_id, block_id))` path uses reserved `scope_id: 0`; the normal `Db`→`DbCacheWrapper` path — the only one reachable via the Java binding — always scopes. Nothing to do beyond "build one cache, pass to every shard's builder.")

---

## 13. Hybrid Tiered State: RocksDB (hot) + SlateDB (cold)

The design that makes SlateDB-in-Flink actually pay off: keep the **hot working set on the RocksDB state backend** (µs latency, full Flink integration — checkpoint/rescale/timers for free) and **offload the cold tail to SlateDB**. This confines the painful parts of §1–§9 (blocking S3 reads, hand-rolled rescale, native-memory accounting) to data that is rarely touched.

The core difficulty this section addresses: **two state stores with different checkpoint and rescale lifecycles that must stay mutually consistent across failures.**

### 13.1 First decision — what does the tier boundary key off?

- **(A) Recency / access-age** — demote a *key* when it hasn't been touched for the tier window (e.g. 1 day). Whole values move hot↔cold (an LRU/TTL tier). **Most common; designed as primary below.**
- **(B) Event-time within a key** — one key holds a time-series; the last day's points are hot, older points cold. This splits a single key's value across tiers → a substantially more involved data model (composite time-bucketed keys, cross-tier scans on read). Not designed here; flag if this is the intent.

### 13.2 Two architectures

**Pattern 1 — Write-through mirror (SlateDB = source of truth, RocksDB = TTL cache).**
Every write goes to both: RocksDB with 1-day State TTL, SlateDB permanently. Reads check RocksDB → miss → SlateDB.
- ✅ **Consistency is trivial** — SlateDB always holds everything; RocksDB is a pure cache. No demotion, no two-store atomicity, no migration timers. Recovery is safe by construction (SlateDB authoritative).
- ❌ **Not real offload** — SlateDB stores *all* data including hot; you pay S3 write cost + backpressure risk (§8) on **every** write.

**Pattern 2 — True tiering / demotion (what "1 day hot, older cold" describes).**
RocksDB holds hot exclusively; data ages out to SlateDB; SlateDB holds only cold.
- ✅ Real offload — S3 write only on demotion; RocksDB stays small.
- ❌ You own hot→cold migration and its consistency across two independent checkpoint lifecycles.

**Recommendation:** If you mainly want *bounded RocksDB size* and can afford mirrored writes, **default to Pattern 1** — the consistency proof is one line. Choose **Pattern 2** only if the cold tail is genuinely rarely read and the S3-write savings justify the complexity. Pattern 2 is specified below.

### 13.3 Pattern 2 skeleton

```java
public class TieredStateFn extends KeyedProcessFunction<String, Event, Out>
        implements CheckpointedFunction {

    // HOT — normal Flink managed state (RocksDB). Checkpointed & rescaled by Flink for free.
    private transient ValueState<byte[]> hot;
    private transient ValueState<Long>   lastAccessMs;

    // COLD — SlateDB, per §5/§6 (shard-per-bucket preferred), key-group-prefixed.
    private transient Db    cold;
    private transient Admin admin;

    public void processElement(Event e, Context ctx, Collector<Out> out) throws Exception {
        byte[] v = hot.value();
        if (v == null) {
            v = await(cold.get(coldKey(ctx.getCurrentKey())));   // COLD miss → BLOCKING ms read (§13.6)
            // optional promotion: re-warm frequently-read cold keys back into hot
        }
        byte[] nv = update(v, e);
        hot.update(nv);
        lastAccessMs.update(ctx.timerService().currentProcessingTime());
        ctx.timerService().registerEventTimeTimer(alignToBucket(ctx.timestamp() + ONE_DAY)); // §13.7
        out.collect(project(nv));
    }

    public void onTimer(long ts, OnTimerContext ctx, Collector<Out> out) throws Exception {
        byte[] v = hot.value();
        if (v == null) return;                                   // already demoted/deleted
        if (touchedWithin(lastAccessMs.value(), ONE_DAY)) {      // touched again → keep hot
            ctx.timerService().registerEventTimeTimer(alignToBucket(ts + ONE_DAY));
            return;
        }
        // ---- DEMOTION — ORDER IS LOAD-BEARING (§13.4) ----
        await(cold.put(coldKey(ctx.getCurrentKey()), v));        // 1. write COLD, durably, FIRST
        hot.clear(); lastAccessMs.clear();                       // 2. ONLY THEN clear HOT
    }

    public void snapshotState(FunctionSnapshotContext c) throws Exception {
        await(cold.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));  // §16: MEMTABLE flush, not flush() — else manifest omits data
        String cpId = await(admin.createDetachedCheckpoint(opts)).id();   // id is a String
        // record (dbPath, cpId, keyGroupRange) in UNION operator list state (§6.3)
    }
}
```

### 13.4 Consistency contract (the part that bites if skipped)

Both tiers checkpoint at the **same Flink barrier** but as **separate artifacts** (Flink snapshots RocksDB; `snapshotState` flushes SlateDB + records its checkpoint id). Hold one invariant:

> **At every barrier: `hot ∪ cold` covers all live data, and where a key exists in `hot`, hot is authoritative.**

Three enforcing rules:
1. **Write-cold-before-clear-hot.** Never `hot.clear()` until the SlateDB write is durable. Clearing first then crashing = key gone from checkpointed RocksDB *and* the SlateDB write rolled back → **data loss**. Correct ordering makes the worst case a *duplicate*, never a loss.
2. **Hot-wins-on-read.** Read RocksDB first; fall through to SlateDB only on miss. A stale cold copy (demote → re-promote) is then harmless.
3. **Tie SlateDB's checkpoint id into Flink state** so both tiers restore to the same logical point.

**Why the ordering is sufficient — verified.** Flink guarantees (a) *"synchronizes invocations of `onTimer()` and `processElement()`"* → no concurrent state modification, and (b) checkpoints snapshot at a **clean record boundary**: *"all updates to the state from records before the barriers have been made, and no updates that depend on records from after the barriers"* — barriers *"never overtake records."* So `onTimer` runs to completion between records; a barrier cannot land *between* `cold.put()` and `hot.clear()`. At any barrier, per key, you are either fully before the demotion (hot has it, cold doesn't) or fully after (cold has it, hot cleared) — never torn.

> **Subtlety:** Flink's boundary guarantee covers `hot.clear()` (Flink state) but **not** `cold.put()` (an external S3 side effect Flink doesn't track). The guarantee that saves you is the *ordering within the uninterrupted `onTimer`*, not Flink treating both writes atomically. On **recovery**, `cold.put()` durability is independent of the Flink checkpoint → you may get a duplicate (hot restored + a cold copy). "Hot wins" (rule 2) absorbs it. Same exactly-once trap as §6.6, now spanning two stores: you get **at-least-once with hot-wins dedup**, not free exactly-once.

### 13.5 Rescale — tiers must agree on key ownership

- **Hot (RocksDB):** Flink rescales automatically. Nothing to do.
- **Cold (SlateDB):** hand-rolled per §6.4 (shard-per-bucket strongly preferred).
- **The alignment that makes it correct:** both tiers must partition by the **same key groups with the same `maxParallelism`**. Flink redistributes hot state by key group; if SlateDB's key-group prefix uses that identical `maxParallelism`, then after rescale a subtask owns the **same key set in both tiers** — no key can end up hot-here / cold-there. **This is the single most important cross-tier constraint.** Shard-per-bucket with buckets = key-group ranges aligns naturally with Flink's redistribution *and* gives zero-copy cold rescale (§4).

### 13.6 The blocking-read problem and the async/mailbox path

A hot-miss read `await(cold.get(...))` runs on Flink's **task thread** — the same thread that ingests records, fires timers, and processes checkpoint barriers. Awaiting an S3 round trip **parks that thread** for the whole ms-scale read:
- Throughput collapses to ≈ `1 / S3-latency` per subtask on cold misses.
- **Barriers can't be processed while parked → checkpoints stall/time out.**
- Backpressure propagates upstream.

Acceptable **only if cold misses are genuinely rare** (true cold tail). If cold is hit often, you need a non-blocking read path — and **how you get one differs sharply by version (§0):**

> **Flink 2.3:** use native async state — `keyBy(...).enableAsyncState()` and the `v2` async state methods (`asyncValue()`, etc.). The framework manages the mailbox/continuation internally; you don't touch `MailboxExecutor`. This is the intended path and integrates with checkpoint draining automatically. **However**, note that async state is designed to front *Flink-managed* state (ForSt); interleaving it with hand-driven SlateDB `CompletableFuture`s in the same operator still requires care — you'd typically let ForSt *be* the cold tier on 2.3 rather than SlateDB (see §0: on 2.3, ForSt may obviate this whole DIY design).
>
> **Flink 1.20:** there is **no** `getMailboxExecutor()` on `RuntimeContext` **and no async state API**, so a non-blocking cold read **cannot be done in a `KeyedProcessFunction`.** You must write a **custom operator**, and the `MailboxExecutor` arrives one of two ways (both verified in 1.20 docs):
> - **`AbstractStreamOperator<Out>`** (classic, V1) implements `YieldingOperator`, which declares the **setter** `setMailboxExecutor(MailboxExecutor)` (`@Internal`). No getter — capture the reference into a field when it's set. `MailboxExecutor` is in `org.apache.flink.api.common.operators`.
> - **`AbstractStreamOperatorV2<Out>`** is constructed with a **`StreamOperatorParameters`** that exposes **`getMailboxExecutor()`** directly (also `getContainingTask()`, `getProcessingTimeService()`, `getOutput()`). This is the cleaner route on 1.20.
>
> Either way it's lower-level than a ProcessFunction (you manage keyed-state access, timer service, watermarks yourself). This is the only non-blocking option on 1.20.

Mechanism (1.20 custom operator; on 2.3 prefer `enableAsyncState()` instead):
1. On a hot miss, **do not `await`.** Fire `cold.get(key)` (returns a `CompletableFuture` immediately) and return from `processElement` without emitting yet — freeing the task thread.
2. The future completes on SlateDB's Tokio thread, where **touching Flink state is illegal** (state access is task-thread-only). So schedule the continuation onto the operator's **`MailboxExecutor`** — the queue the task thread drains between records (the same primitive `AsyncWaitOperator` uses):

```java
// Custom operator on 1.20. Mailbox from StreamOperatorParameters (V2) or setMailboxExecutor (V1) — NOT RuntimeContext.
private final MailboxExecutor mailbox;   // V2: params.getMailboxExecutor() in ctor; V1: captured in setMailboxExecutor()

// on hot miss:
cold.get(coldKey(key)).whenComplete((val, err) ->
    mailbox.execute(() -> {                  // runs ON the task thread, serialized with processElement/onTimer
        byte[] nv = update(val, e);
        hot.update(nv);                        // legal here
        out.collect(new StreamRecord<>(project(nv)));   // emit deferred result
    }, "cold-read-continuation"));
```

Because mailbox continuations are serialized with element/timer/barrier handling, the "no concurrent state modification" guarantee (§13.4) still holds.

**The catch:** you must **drain all in-flight `cold.get` continuations before the snapshot completes** — override `prepareSnapshotPreBarrier`/`snapshotState` in the operator and block until pending futures resolve (else a checkpoint captures hot state missing a pending update → inconsistency). Also **bound in-flight requests** (backpressure when too many cold reads outstanding) and handle ordering. Building this correctly = **re-implementing Flink 2.0's async state framework by hand** — which 1.20 does not provide.

Rule of thumb:
- **Cold reads rare (either version)** → blocking `await` in a `KeyedProcessFunction`, skip the async complexity.
- **Cold reads frequent, on 1.20** → custom-operator mailbox path **or** re-tier. No integrated shortcut exists.
- **Cold reads frequent, on 2.3** → strongly consider making **ForSt** the cold (or whole) tier via `enableAsyncState()` instead of hand-driving SlateDB — you get the non-blocking path, checkpoint integration, and rescale for free (§0). The DIY SlateDB cold tier mainly earns its keep on 1.20 or when you specifically need SlateDB's engine.

### 13.7 Hybrid-specific gotchas

- **TTL gives no demotion hook — verified.** Flink State TTL *"explicitly removes on read … periodically garbage collected in the background"* with **no user callback**. TTL only deletes; it never hands you the value to write to SlateDB. **Explicit timers are the only demotion mechanism.** If you also set a hot-state TTL as a backstop, make it strictly *longer* than the demotion window and treat `NeverReturnExpired` reads as cold misses — don't let TTL and demotion timers fight over the same key.
- **Timer explosion.** One demotion timer per key ≈ millions of timers (themselves in RocksDB, checkpointed). **Coalesce into time buckets** (`alignToBucket`): one timer per bucket, scan-and-demote a batch on fire.
- **Two checkpoints, two failure surfaces.** SlateDB checkpoint/flush failures can now fail a Flink checkpoint. Budget `snapshotState` time (flush + `createDetachedCheckpoint` are S3 round trips) against the checkpoint timeout.
- **Memory is additive.** RocksDB block cache/write buffers (Managed Memory, Flink-accounted) **plus** SlateDB native footprint (§9, *not* Flink-accounted). Size the TaskManager for both; the silent-OOM risk of §9 still applies to the cold tier.
- **GC of demoted data.** Overwritten cold values (demote → re-promote → re-demote) and clone-chain SSTs need reference-aware GC (§6.5).

### 13.8 Verification provenance (this section)
- `onTimer`/`processElement` synchronization: verified verbatim on **both** Flink 1.20 and 2.3 ProcessFunction docs — unchanged across versions. §13.4 consistency argument holds on both.
- TTL has no expiration callback; cleanup strategies + visibility modes: Flink 1.20 State docs.
- Checkpoint snapshots at clean record boundaries ("never overtake records"): Flink stateful-stream-processing docs. The "single task thread / mailbox" phrasing is an implementation detail *not* asserted by the docs; the consistency argument rests on the **record-boundary** guarantee, which the docs do state.
- Version-specific API facts (async state, ForSt, `getMailboxExecutor` absence on both, parallelism-getter removal in 2.3, `open()` signature): verified against 1.20 and 2.3 docs respectively — see §0.
- `MailboxExecutor` acquisition in a 1.20 custom operator: **verified against 1.20 API docs.** Two routes — `AbstractStreamOperator` (V1) via the `YieldingOperator.setMailboxExecutor(MailboxExecutor)` setter (`@Internal`, no getter), or `AbstractStreamOperatorV2` via `StreamOperatorParameters.getMailboxExecutor()` (`@Experimental`). `MailboxExecutor` is in `org.apache.flink.api.common.operators` and is **not** on `RuntimeContext` in either 1.20 or 2.3.

---

## 14. Resharding & Changing Bucket Size

**Changing bucket size = changing N (shard count) = changing the key→shard routing function.** That re-routes keys to different physical DBs → it is a **state migration, not a config change, and cannot be live.** How painful it is depends entirely on a decision made *before* you ever reshard (§14.1).

**Two things you must not conflate:**
- **Bucket size** = key groups per shard (→ N). Changeable (§14.1 free, or §14.3 offline).
- **`maxParallelism`** = total key-group count. **Fix generously once, never change** — changing it rehashes every key (full rebuild, not a reshard). Set high up front (e.g. 4096/32768) so you never need to. Key groups are the fixed atomic unit; resharding only redraws bucket boundaries *over* them — it never splits within a key group.

### 14.1 Recommended: decouple *logical shard* from *physical DB* → resharding is free

**Don't make bucket size a physical property.** Choose a **fine, fixed physical shard granularity once** (many small physical SlateDBs, each = a small fixed bucket of key groups — e.g. 512–1024 physical shards) and **never physically reshard.** Then "bucket size" is a **routing-layer knob**: how many physical shards a subtask opens.

- **"Change bucket size" = change the physical-shard→subtask mapping = zero data movement** — the exact same operation as rescale (§4): recompute assignment, re-open. No clone, no scan-copy.
- This mirrors how Flink/RocksDB/ForSt work internally (key groups = fine fixed unit; parallelism just regroups them). You're copying the model that already solved this.
- **Cost:** many small physical DBs → the N/P per-subtask overhead of §4.1 (compactors, unflushed buffers). Mitigated by shared cache (§12.7, safe), sidecar compaction (§7), trimmed `max_unflushed_bytes`. This is the price of never physically resharding — pay it once by design.

**Takeaway: make the physical shard the fine, immutable unit; make bucket size a logical grouping.** Then the answer to "how do I reshard later" is "you don't — you re-map, for free."

### 14.2 Split/merge primitives — what's shipped

- ❌ **No native DB split or DB merge** — both "under development" in README. You cannot split one DB into two by key range, or merge two DBs, as a primitive.
- ✅ **Clone with projection = the split tool.** In the Java binding the projection lives on **`CloneSourceSpec`**, not the builder:
  `CloneSourceSpec.projection_range: Option<KeyRange>` — *"restrict the visible keys from this source."* `KeyRange { start: Option<byte[]>, start_inclusive, end: Option<byte[]>, end_inclusive }` expresses any half-open key-group prefix range `[kgStartPrefix, kgEndPrefix)`.
  ⚠️ **Semantics (verified):** projection limits *visible* keys; the clone still **references the full parent SSTs** (shallow). Storage is **not** reduced until each child's own compaction rewrites in-range-only data *and* the parent is GC'd. With no range-delete, out-of-range data lingers in shared SSTs meanwhile → **expect total storage to transiently grow, not shrink, right after a split.**
- ✅ **Scan-copy = the merge tool** (and split fallback): `scan(KeyRange)` each source's key-group range, `put` into the target DB — same primitive as the §6.4 rescale merge loop.

### 14.3 Offline physical-reshard procedure (only if §14.1 wasn't followed and a physical shard is now too coarse/fine)

A maintenance-window operation:
1. **Flink savepoint** — quiesce the job; no in-flight writes.
2. **Detached checkpoint per physical shard** (`admin.createDetachedCheckpoint`); pin the UUIDs.
3. **Split** a shard: two clones from the same checkpoint, each with a `CloneSourceSpec` carrying `withCheckpoint(path, uuid)` **and** a `projection_range` `KeyRange` for one half's key-group byte-range → two new DBs.
   **Merge** shards: `scan` each source's key-group range → `put` into one new DB.
4. **Rewrite the shard map** (key-group → physical DB → subtask); persist as job config / in union operator state.
5. **Restore from the savepoint** against the new shard map; each subtask opens its newly-assigned DBs.
6. **GC old DBs — reference-aware** (§6.5): a split leaves the parent's SSTs referenced by both children until they compact. Don't delete eagerly; expect the transient storage bump from §14.2.

### 14.4 Decision

| Approach | Reshard cost | When |
|---|---|---|
| **Fine-fixed physical shards; bucket = logical grouping (§14.1)** | **~zero** (re-map like rescale) | ✅ **Default. Set physical granularity fine once; never physically reshard.** |
| Coarse physical shards; physical reshard later (§14.3) | offline: savepoint → clone/scan-copy → restore → GC | only if granularity was set wrong; windowed, real work |

### 14.5 Verification provenance (this section)
- `KeyRange` fields (`Option<Vec<u8>>` bounds + inclusivity) and that it expresses arbitrary byte ranges: `bindings/uniffi/src/types.rs`.
- Projection lives on `CloneSourceSpec.projection_range: Option<KeyRange>`, *"restrict the visible keys from this source"*: verified (binding types + docs.rs). Rust `CloneBuilder.with_projection_range` takes a generic `RangeBounds<Bytes>`; the binding routes projection through `CloneSourceSpec`.
- Shallow clone references full parent SSTs (projection ≠ physical prune): clones design doc.
- No native DB split/merge (under development): README feature list.

---

## 15. Gap Analysis: SlateDB vs the RocksDB State Backend

**Question:** what is missing in SlateDB that prevents it from being a drop-in replacement for Flink's `EmbeddedRocksDBStateBackend`?

**Headline answer:** there is **no single SlateDB engine feature** whose addition flips it into a drop-in replacement — because the primary blocker is a **latency / execution-model mismatch**, not a missing capability. Most of "what's missing" is the **Flink-side integration layer**, and the engine gaps that remain are two specific holes (range-delete, compaction-filter-in-binding).

### 15.1 What Flink's RocksDB backend relies on, vs SlateDB

| # | RocksDB capability Flink uses | SlateDB status | Blocking? |
|---|---|---|---|
| **1** | **Synchronous µs point reads** — `ValueState.value()` returns immediately | ❌ reads are **ms-scale remote** | **THE blocker** (§15.2) |
| **2** | **Range delete** (`deleteRange`) — clears MapState / window / namespace state **and** clips key groups on rescale | ❌ **absent** — single-key delete only ("under development") | **High** — a real functional gap, not only rescale (§5.5) |
| **3** | **Compaction filter** for background TTL state cleanup | ⚠️ exists in the Rust engine (`CompactionFilter`/`CompactionFilterSupplier`, RFC 0017; built-in TTL filter RFC 0003) but **NOT exposed in the Java/UniFFI binding** (`DbBuilder` has no `with_compaction_filter_supplier`) | **High** for a binding-based backend |
| **4** | **SST ingest / bulk load** (`IngestExternalFile`) — RocksDB rescale ingests + clips | ❌ absent → scan-copy; `clone` mitigates base adoption (§6.4) | Medium |
| **5** | **Column families** — Flink uses one CF per state descriptor | ❌ none → multiplex via `stateId` key prefix (§6.1) | Low (clean workaround) |
| 6 | Prefix/range iteration (MapState, key enumeration) | ✅ `scan` / `scan_prefix` | not a gap |
| 7 | Merge operator | ✅ `merge` (in binding) | not a gap |
| 8 | Atomic write batch / snapshots / txns | ✅ `write(WriteBatch)`, `snapshot`, `begin` | not a gap |

### 15.2 Why #1 is not a SlateDB feature gap

Flink's state-backend contract (`KeyedStateBackend`, `ValueState.value()`, timers) is **synchronous**. A synchronous API over ms-scale remote reads blocks the task thread on every access → unusable throughput. The resolution is **not a SlateDB feature** — it's Flink's **async state execution framework** (`StateFuture`, `enableAsyncState()`), which:
- exists only in **Flink 2.0+** (verified: absent in 1.20, present in 2.3 — §0), and
- is exactly what **ForSt** is built to plug into.

So the most important "missing piece" for SlateDB-as-state-backend is **integration into Flink's async state framework the way ForSt is** — an *integration* deliverable, not an engine feature.

### 15.3 Precise answer

Assuming you build on Flink's async state framework (2.0+/ForSt-style), the SlateDB **engine** gaps that still need closing are:
1. **`deleteRange` / range delete** — for state clears (arbitrary MapState/namespace clear) and rescale clipping. Biggest true functional gap; today only partially worked around by logical-clip + scan-copy (§6.4), which does **not** cover arbitrary MapState clears.
2. **Compaction-filter exposure in the Java binding** — for background TTL cleanup (the engine has it; it's simply not bound).
3. *(nice-to-have, non-blocking)* column families (prefix multiplexing works, §6.1) and SST-ingest (clone + scan-copy works, §6.4).

**Meta-answer:** the bulk of "what's missing" is the **Flink integration layer** — `KeyedStateBackend` SPI, checkpoint-coordinator wiring, key-group rescale, timer service, and async execution. Building all that = "reimplement ForSt with a SlateDB core" (§2). This is why, on **2.3**, the pragmatic path is **ForSt**; and why on **1.20** (no async state at all) a true drop-in RocksDB replacement is not achievable — the hybrid tiering of §13 (RocksDB hot + SlateDB cold via a ProcessFunction) is the realistic way to get SlateDB into the picture without the full backend.

### 15.4 Verification provenance (this section)
- Compaction filter exists in Rust (`CompactionFilter`, built-in TTL filter): RFC 0017, RFC 0003. **Not** in the binding: `bindings/uniffi/src/builder.rs` (`DbBuilder` method list — no filter method).
- Range delete absent; merge/scan/snapshot/txn present: verified §5 (`bindings/uniffi/src/{db,builder}.rs`, docs.rs `struct.Db`).
- Async state / ForSt is Flink 2.0+ only: §0 (Flink 1.20 vs 2.3 docs).

---

## 16. Empirical Verification (runnable PoC)

Everything above §16 was verified by *reading* shipped source. This section records what was verified by
*running* code against the real published artifact (`io.slatedb:slatedb-uniffi:0.14.1` from Maven Central)
and real Flink (1.20.1, 2.3.0) on embedded MiniClusters. PoC lives in `flink-slatedb-poc/` (5 Maven modules,
no Docker). **Running repeatedly surfaced findings that source-reading missed** — a silent-data-loss bug
(§16.2), that Flink runs on JDK 25 (§16.3), the removable Java-22 floor (§17), and that post-compaction space
reclaim lags ~15 min behind `min_age` (§16.13). See the ⚡ list in `INVESTIGATION-JOURNAL.md`.

### 16.1 Results
| Module | What ran | Result |
|---|---|---|
| `flink-1.20-poc` | Flink 1.20.1 MiniCluster: §3 KeyGroupRange reconstruction, §0 `open(Configuration)` + deprecated getters, §13.5 ownership partition | ✅ ALL PASS |
| `flink-2.3-poc` | Flink 2.3.0 MiniCluster: §0 `open(OpenContext)`, getTaskInfo()-only, §0/§15.2 `enableAsyncState()` exists + enforced | ✅ ALL PASS |
| `slatedb-verify` | Real SlateDB 0.14.1 native lib (JDK 25): §12.8 clone-restore exactly-once | ✅ ALL PASS |
| **`flink-slatedb-e2e`** | **THE composition: a real Flink `KeyedProcessFunction` (MiniCluster) using SlateDB as per-key state, ONE JVM (JDK 25): §6.1 key-group keys + §6.2 per-key RMW + §6.3 memtable-flush checkpoint + §6.6 clone-restore exactly-once** | ✅ **ALL PASS** |

**§16.6 — the e2e result (central thesis verified).** Stream `[a,a,b,__BARRIER__,a,c]` through a real keyed operator: pre-barrier SlateDB state `{a=2,b=1}`; on the barrier the operator did memtable-flush + `createDetachedCheckpoint`; post-barrier writes `a→3,c=1` made durable; **restore via clone-from-checkpoint yielded exactly `{a=2,b=1}`** — `a=2 not 3`, `c` absent → post-barrier writes discarded, **exactly-once confirmed end-to-end**. This is §6.1+§6.2+§6.3+§6.6 verified *composed*, not just in isolation (closes the §16.5 gap).

### 16.2 FINDING (silent-data-loss bug) — `flush()` is insufficient before a checkpoint
**`Db.flush()` with WAL enabled = `FlushType::Wal` (WAL only), which does NOT advance the manifest.**
A checkpoint pins the *manifest*; the manifest only advances on a **memtable→L0** flush. Verified empirically:
after `put` + `flush()` + `createDetachedCheckpoint()`, a checkpoint-pinned reader saw the key **NEVER**
(across 20 checkpoints over 5s). Source-confirmed in `db.rs:1735` (WAL flush type). **Fix:**
`flushWithOptions(FlushType.MEM_TABLE)` before checkpointing — after which all clone-restore checks pass
(clone excludes post-checkpoint writes AND retains pre-checkpoint data). §6.3 and §13.3 code updated.
**Impact if missed:** every Flink checkpoint would pin an empty/stale SlateDB manifest → silent state loss on restore.

### 16.3 FINDING (correction to earlier "showstopper") — Flink runs on JDK 22+
The Java-version conflict (SlateDB needs Java 22+ FFM; Flink officially supports ≤17/21) is **real but
surmountable**. Verified: Flink **1.20.1 AND 2.3.0 both run on JDK 25** (MiniCluster, all checks pass) with
`--add-opens java.base/{util,lang,time}=ALL-UNNAMED`, and SlateDB's native lib loads on the same JVM. So
**in-process SlateDB + Flink IS possible** — run the whole cluster on JDK 22+. Caveat: officially unsupported
by Flink; validate under real load. (Earlier drafts called this a showstopper based on stock-image JVMs — corrected.)
**And the floor itself is removable — see §17:** regenerating the binding with UniFFI's Kotlin/JNA backend
runs SlateDB on **JDK 11/17/25** (proven by `slatedb-jna-j11`), at the cost of maintaining a binding fork.

### 16.4 Confirmed empirically
- §12.8 clone-from-checkpoint excludes post-checkpoint writes AND retains pre-checkpoint data (exactly-once sound, given §16.2 memtable flush).
- §5 API shapes confirmed via `javap` on the real JAR: `CloneSourceSpec(String,String,KeyRange)` plain ctor (NOT `withCheckpoint` factory); `CheckpointCreateResult.id():String`; non-chainable void builder setters; `AdminBuilder.build()` sync; async ops return `CompletableFuture`.
- Native lib loading: the JAR bundles per-platform native libs but does NOT auto-extract; caller must place it on `java.library.path` (or `System.loadLibrary`). Documented in the PoC README.

### 16.5 What the PoC did NOT verify (still open)
- ~~End-to-end SlateDB embedded in a running Flink job~~ — ✅ **DONE, §16.6 / `flink-slatedb-e2e`.**
- ~~Failure/recovery via real Flink checkpointing~~ — ✅ **DONE, §16.8 / `FlinkSlateDbRecoveryE2E`.**
- ~~**Rescale** merge loop (§6.4)~~ — ✅ **DONE, §16.9 / `SlateDbRescaleE2E`** (both downscale multi-source scan-copy and upscale projection-clip).
- ~~**§13 hybrid tiering** (RocksDB-hot + SlateDB-cold, demotion timers, write-cold-before-clear-hot)~~ — ✅ **DONE, §16.10 / `FlinkHybridTieringE2E`.**
- **§8 backpressure, §9/§9A memory & disk, §7 compaction** under load — no measurement.
- Operational §11 risks (S3 cost, tail latency, pre-1.0 format stability, failover fencing under concurrency) — un-benchmarked.

### 16.8 HIGH-FIDELITY: real Flink checkpoint + induced failure + recovery = exactly-once (`FlinkSlateDbRecoveryE2E`)
The strongest test run. **Real** `env.enableCheckpointing(250)` (Flink drives global checkpoints), a
throttled **replayable** source that checkpoints its offset, a `CheckpointedFunction` operator that in
`snapshotState` does memtable-flush + `createDetachedCheckpoint` and stores the SlateDB checkpoint id in
**Flink operator (union list) state**, and in `initializeState` (on `isRestored`) **clones SlateDB from that
stored id** into a new generation. An induced `RuntimeException` at record 30 (after Flink cp 2 completed)
forced a genuine restart. Verified trace:
- source rewound to checkpointed **offset 20**; operator cloned SlateDB from the checkpoint id captured in
  the *same* Flink checkpoint (`db-gen-0 @cp=…` → `db-gen-1`) — offset and SlateDB-cp restore to the **same
  logical point** (the cross-store alignment that makes exactly-once work);
- replay of records 20→60 against the cloned state produced **every key counted exactly 10** (= true input
  frequency). >10 would be double-count (at-least-once, clone failed to roll back); <10 would be loss. Exactly 10 ⇒ **exactly-once confirmed across a real Flink failure/recovery cycle.**

**Two bugs surfaced by running (not by source-reading):** (1) §16.2 `flush()`≠manifest-commit (silent loss);
(2) `env.fromData` emits all records instantly so no checkpoint captures mid-stream state — a **throttled,
offset-checkpointed replayable source** is required, and the **source offset must be checkpointed in lockstep
with the SlateDB checkpoint id** or replay/rollback desync. Both are real integration lessons for §6.3/§6.6.

**Design conclusion (empirically grounded):** SlateDB can serve as authoritative, exactly-once Flink keyed
state, restored via clone-from-pinned-checkpoint, surviving a real Flink failure/recovery cycle — at
parallelism 1. Hybrid tiering (§13) remains the untested frontier.

### 16.9 RESCALE: the §6.4 clone + scan-copy merge, both directions (`SlateDbRescaleE2E`)
Exercises the §6.4 one-DB-per-subtask rescale merge with **real** SlateDB clone/projection/scan-copy, on the
handle list `(dbName, checkpointId, keyGroupRange)` Flink union list state carries across a parallelism change.
400 keys, maxParallelism=128. **DOWNSCALE 3→2** (each new range = union of old → multi-source scan-copy):
`sub0[0,63]` cloned base `g3-sub0` (overlap 43) + scan-copied 49 from `g3-sub1`; `sub1[64,127]` base `g3-sub2` +
60 copied. **UPSCALE 2→4** (each new range = sub-range → clone+projection clip, 0 scan-copy). Chained (upscale
ran on the downscaled generation → clone-of-clone, §6.5). **All 5 invariants held both directions:** no foreign
keys, no duplicates, no loss (all 400), values intact, correct key-group ownership.

**Findings:**
- **`withProjectionRange` physically clips (verified stronger than assumed).** §6.4 assumed projection might only
  *logically* hide out-of-range key groups (lingering in shared SSTs), making logical-clip mandatory. Empirically,
  a full scan of a projection-cloned DB found **zero foreign keys** — projection genuinely bounds readable data.
- **Chained rescale is correct** — clone-of-clone across repeated rescales preserved the exact partition.
- **Scope:** this tests the *merge algorithm* with faithful inputs; it does NOT drive a full Flink
  savepoint→rescale→restore (that exercises Flink's savepoint machinery, not our merge). Parallelism-change
  redistribution of Flink operator state itself is standard Flink and separately reliable.

### 16.11 COMPACTION observed + no data loss (`SlateDbCompactionE2E`)
Forced many small L0 SSTs (`l0_sst_size_bytes=4096`, 20 memtable flushes) with an aggressive compactor
(`poll_interval="200ms"`), then polled the manifest. Observed **L0 peaked at 4 then drained to 0** while
`compacted` sorted runs grew to 4 (67KB) — i.e. the embedded compactor merged L0→sorted-runs live. **All 2000
keys intact** afterward (no loss/corruption across compaction). Verifies §7 compaction runs and preserves data.
API finding: `Settings.set(key, valueJson)` takes **JSON values** — numbers bare (`"4096"`), durations as
quoted strings (`"\"200ms\""`), not JSON maps.

### 16.13 ⚡ GARBAGE COLLECTION: orphans are physically deleted, but ~15 min AFTER compaction (`SlateDbGcE2E` / `SlateDbGcLongE2E`)
**A "found by running" correction to a design assumption.** Compaction rewrites the manifest to drop old SSTs
but does **not** delete the files — a background GC does, gated by (a) `min_age`, (b) the compaction
low-watermark, and (c) *not referenced by the latest manifest or any live checkpoint*. I assumed a small
`min_age` ⇒ prompt deletion. **Wrong.** Setting `min_age=1s` and idling, GC deleted **nothing** for many
minutes. Instrumenting SlateDB's own DEBUG logs + reading the manifest showed why: on **every manifest commit
the compactor first writes an internal checkpoint with a hardcoded 900s (15-minute) expiry** —
`compactor_state_protocols.rs:250`, *"so that it's extremely unlikely for the gc to delete ssts out from
underneath the writer."* That checkpoint pins the manifests referencing the just-compacted L0s, so condition
(c) stays false for ~15 min **regardless of `min_age`**.
- `SlateDbGcE2E` (fast, ~3 min) — ✅ verifies the STATE that explains the delay, live: compaction orphaned
  files (**54 on disk vs 5–8 manifest-referenced**), the compactor auto-created **12 checkpoints with
  `lifetimeSecs=900`**, and GC conservatively **retained** the orphans within the window; 2000 keys intact.
- `SlateDbGcLongE2E` (~17 min) — ✅ waits out the 900s expiry and observes the physical deletion: on-disk
  compacted files **15 → 7 at t+900s → 4 at t+930s** (11 SSTs deleted, logged), settling at the live set; all
  data intact. Deletion fires *exactly* at the checkpoint expiry, not before.
- **Implication for Flink:** steady-state disk holds live state **+ ~15 min of post-compaction orphans**; the
  900s window is **not tunable** from the Java binding (hardcoded; source has a TODO to make it configurable).
  It is safe/correct (deliberately conservative), just slower to reclaim than `min_age` suggests. Relevant to
  §9A disk sizing.

### 16.12 PARALLEL + SHARD-PER-BUCKET + shared cache (`FlinkShardPerBucketParallelE2E`) — the P>1 gap
The gap the other tests missed: a REAL Flink job at **parallelism=4** using shard-per-bucket. 16 fixed shards
(each = 8 key groups, one SlateDB); each subtask opened the N/P=4 shards its KeyGroupRange owns; all shards on a
subtask **shared one `DbCache`** (§12.7). Verified: 4 subtasks ran, **16 SlateDB instances open concurrently**
in one JVM on the shared Tokio runtime; 800 keys each counted **exactly 3** (concurrent RMW correct, no
corruption); clean partition (no key in >1 shard); correct key-group→shard→subtask ownership (§13.5).
- **§4/§4.1 shard-per-bucket at P>1** — N/P instances per subtask, concurrent, exact. ✅
- **§12.7 shared cache across instances** — scope-id collision-safety confirmed LIVE under concurrent load
  (not just source-read): exact values across 4 shards sharing one moka cache per subtask.
- **First test of parallelism > 1 with embedded SlateDB at all.**

### 16.10 §13 HYBRID TIERING: RocksDB(hot) + SlateDB(cold) in a real operator (`FlinkHybridTieringE2E`)
Real Flink `KeyedProcessFunction` with hot state in Flink `ValueState` and cold state in SlateDB, event-time
driven. Verified trace: keys build hot (`cold-A hot=1,2`) → **demote mid-stream** (timer fires: SlateDB
write-first then `hot.clear()`) → **late re-touch promotes from SlateDB** (`hot=3` = cold 2 + 1) → re-demote.
Diagnostics **demotions=5, promotions=2** (both >0 ⇒ tiering genuinely exercised). **Final `(hot∪cold,
hot-wins)` counts exact** for every key across demote→promote→re-demote cycles.
- **§13.4 write-cold-before-clear-hot** — held; no cross-tier loss.
- **§13.4 hot-wins reads** — promoted key served from hot after promotion.
- **§13.7 demotion via explicit timer** — the only mechanism (State TTL has no callback, §16); fired correctly.
- **Timing lesson (again):** `fromData` emits instantly so event-time timers only fire at close — needed a
  throttled source + `setAutoWatermarkInterval(20)` for mid-stream demotion. Real gotcha for event-time tiering.
- **Scope:** parallelism 1 (§13.5 cross-tier key-group alignment trivially satisfied); `close()` uses a
  test-harness side-map to apply hot-wins at end (real jobs drain hot→cold at stop/savepoint).

### 16.7 One more API finding (from running it)
`ObjectStore.resolve` **requires an empty path**: the store is the *root* (`file:///`, `s3://bucket`), and the DB path goes to the *builder* as the db-name. `resolve("file:///tmp/x")` fails with *"invalid object store path. provide path to builder instead."* Verified against the JAR. (So `new DbBuilder(dbPath, ObjectStore.resolve(rootUri))`, not a path-bearing store URI.)

### 16.14 ⚡ MARSHALLING COST: serialization is cheap; async round-trip latency dominates (`SlateDbMarshalBench`)

Question: is there a lot of Rust↔Java serialization overhead? **Benchmarked it** (not just source-read) on `memory:///` so all reads are block-cache hits — isolating FFI + RustBuffer copies + async plumbing from any storage cost. JDK 25, FFM binding, single-threaded driver, 20k measured ops/size after warmup:

| value size | serial GET (1 await at a time) | pipelined GET (256 in flight) | batched PUT (100/batch) |
|---|---|---|---|
| 16 B | 43.9 µs (23k/s) | **10.3 µs/op (97k/s)** | ~1020 µs/batch |
| 256 B | 49.5 µs (20k/s) | 12.5 µs/op (80k/s) | ~1022 µs |
| 1 KB | 53.7 µs (19k/s) | 13.9 µs/op (72k/s) | ~1021 µs |
| 4 KB | 58.8 µs (17k/s) | 14.2 µs/op (71k/s) | ~1030 µs |
| 16 KB | 63.3 µs (16k/s) | 14.2 µs/op (70k/s) | ~1021 µs |

Three findings, all from running:
1. **The copies (serialization) are cheap and bulk.** Isolating throughput via the pipelined column: 16 B → 16 KB adds only **~4 µs/op**. That's the bulk `memcpy` of the extra bytes (confirmed §5.1/binding source: `Vec<u8>` ↔ `byte[]` is a whole-buffer copy via RustBuffer + `FfiConverterByteArray`, **not** field-by-field encoding). Serialization is **not** the thing to worry about.
2. **Per-call async round-trip latency dominates, not copying.** During the serial test CPU sat at **0.1%** — the thread was *blocked waiting*, not copying. A serial `await(get)` is ~44–63 µs but pipelining 256 concurrent gets drops it to ~10–14 µs/op; that 3–4× gap is the fixed cost of driving a Rust future across the FFI and waiting for the completion callback (`upcallStub`), independent of payload size.
3. **The flat ~1 ms/batch PUT is DURABILITY, not marshalling.** Verified in source: `WriteOptions.await_durable = true` by default, so every `write()`/`put()` blocks until the WAL flush. Flat ~1 ms regardless of size/batch confirms it. Batching 100 rows amortizes that one wait → ~10 µs/row effective.

**Context that makes this a non-issue for the SlateDB-in-Flink design:** a cache-**miss** get is an S3 GET at ~1–10 ms = **100–200× the entire FFI+copy+async cost**. Storage latency dwarfs marshalling. **Practical guidance:** don't do chatty one-at-a-time `await` per key (caps ~20k ops/s/thread) — **pipeline or use `WriteBatch`** to reach ~70–97k/s and to amortize the `await_durable` WAL flush. Value size barely matters for marshalling below the multi-MB range. *Scope caveat:* single machine, `memory:///`, single-thread driver — absolute µs will shift under real Flink concurrency + S3, but the ratios (storage ≫ latency ≫ copy cost) are the robust conclusion. The JNA binding (§17) has somewhat higher per-call *latency* (reflective dispatch vs `MethodHandle`), same copy cost.

### 16.15 READ-YOUR-WRITES before any flush (`ReadYourWritesE2E`)

Verifies the pre-flush consistency guarantee §6.2 relies on: within a single `Db` instance, a read sees your own writes **before any `flush`/`checkpoint`**. Ran on `memory:///` (no disk/S3 that could mask unflushed data) with **no flush anywhere** — the only way a read succeeds is via the in-memory memtable path. All 8 checks passed:
- **put→get** sees the value; **overwrite→get** sees the LATEST (`3`, not `1`/`2` — MVCC newest-seq wins); **delete→get** sees `null` (tombstone honored pre-flush).
- **RMW loop ×500** (get→+1→put, never flushed) → final = exactly 500 (reads its own writes every iteration, no stale reads).
- **2000 keys** written then all read back → 0 missing, 0 wrong.
- **scan** over an unflushed range → rows returned in key order.
- **`await_durable=false`** → the write is still immediately readable by the same instance. Proves **visibility ≠ durability**: RYW comes from the memtable; `await_durable`/flush only govern crash-safety and what a *separate* reader/clone sees.

Mechanism (source): write lands in the mutable memtable synchronously; `get`/`scan` read newest→oldest (memtable → imm memtables → SSTs), first hit wins (`reader.rs`, `batch_write.rs`). **Scope:** single-writer, single-instance — the exact SlateDB-as-Flink-keyed-state model (one DB per subtask, same instance does the RMW). A *different* `DbReader`/clone would NOT see unflushed writes (it reads durable storage / a pinned checkpoint) — which is why the §16.2 flush-before-checkpoint rule exists for the *restore* path but live reads were always consistent.

### 16.16 FLINK OBJECT SERIALIZATION into SlateDB (`FlinkSerdeSlateDbE2E`)

Verifies §6.1a: store Java objects in SlateDB the way Flink's RocksDB backend does — via a Flink `TypeSerializer`, since SlateDB stores only `byte[]`. Flink chose `PojoSerializer` for the test POJO; all 4 checks passed: single POJO round-trips equal; **1000 POJOs** round-trip field-for-field after flush (0 wrong); **object RMW ×50** (read→mutate→write: balance 100→600, list field grew 2→52). Two gotchas surfaced by running: (1) collection fields go through Kryo → must be **mutable** (`ArrayList`, not `List.of`) or a later `.add()` throws on the deserialized immutable list; (2) POJOs not `record`s (Kryo/records break on JDK 16+, §16.10). Confirms the serialization layer is Flink's own, unchanged — the same `DataOutputSerializer`/`DataInputDeserializer` primitives `RocksDBValueState` uses — with SlateDB just holding the resulting bytes.

---

## 17. The Java-22 floor is a PACKAGING choice, not a SlateDB limit (`slatedb-jna-j11`)

§16.3 established that Flink runs on JDK 22+, so the FFM binding's Java-22 floor is *tolerable*. This section
goes further and shows the floor is **removable**: the same SlateDB native library runs on **JDK 11, 17, and
25** through a different generated front-end. Proven by the `slatedb-jna-j11` module (real ops + checkpoint,
all three JVMs green).

### 17.1 Where the floor actually comes from
The published `io.slatedb:slatedb-uniffi` artifact is 100% machine-generated by
[`uniffi-bindgen-java`](https://github.com/IronCoreLabs/uniffi-bindgen-java) (0.4.1, pinned in the repo). That
generator emits **`java.lang.foreign` / FFM (Panama)** code. Verified by decompiling the published 0.14.1 JAR:

- bytecode **major version 66** (Java 22) on every class;
- **40** classes reference `java.lang.foreign.*` — `Linker.nativeLinker()`, `SymbolLookup.loaderLookup()`,
  `downcallHandle`, `upcallStub`, **`Arena`** (a *final*-API type; the Java-17 incubator used the incompatible
  `MemorySession`/`CLinker`), one `MethodHandle` per Rust fn;
- **zero** generated classes call `com.sun.jna.*`.

So the floor is JEP 454 (FFM finalized in **Java 22**), inherited entirely from the generator — not from the
Rust core or the native lib. (Note: the 0.14.1 POM *does* declare a `net.java.dev.jna` dependency, but it's
used only for platform detection / native-lib extraction; the actual FFI is all FFM. Don't be misled by it.)

### 17.2 The removal path — regenerate with the Kotlin/JNA backend
The native library exports a plain **C ABI** (441 symbols, generator-agnostic). UniFFI's Mozilla-standard
**Kotlin** backend targets that same ABI via **JNA**, which runs on **Java 8+**. So you swap the *code
generator*, touching neither Rust nor the native lib:

```
uniffi-bindgen 0.31.1  generate --language kotlin --library libslatedb_uniffi.dylib
```

The generated binding lives in package `uniffi.slatedb.*` (distinct from the FFM binding's
`io.slatedb.uniffi.*`, so they never collide) — pure JNA (`Native.register`, `com.sun.jna.Callback`), 167
`suspend` fns, no `java.lang.foreign`.

### 17.3 Verified by RUNNING (`slatedb-jna-j11`)
A plain-Java driver (compiled with `javac 11`) drives real SlateDB ops through the JNA binding:

| JVM | 500× put/get | MEMTABLE flush | detached checkpoint | Result |
|---|---|---|---|---|
| **JDK 11** (Flink 1.20 floor) | 500/500 | ✅ | ✅ (real UUID) | **PASS** |
| **JDK 17** | 500/500 | ✅ | ✅ | **PASS** |
| **JDK 25** | 500/500 | ✅ | ✅ | **PASS** |

No `--enable-native-access`, no `--add-opens` (those are FFM-only). The async completions genuinely exercise
the JNA **upcall** path (`ffi_slatedb_uniffi_rust_future_poll_*` +
`UniffiRustFutureContinuationCallback : com.sun.jna.Callback`) — the exact mechanism FFM does with
`upcallStub`. The checkpoint is the async-heavy `Admin` op Flink needs for exactly-once.

### 17.4 The "fork tax" — real, and hit during this work
This is not free, and the costs are exactly what you'd own forever:

1. **A genuine uniffi-0.31 codegen bug.** SlateDB's Rust `Error` variants carry a `message` field, so the
   generator emits a constructor `val \`message\`` that **collides with `Throwable.message`** — it does **not
   compile on any kotlinc** (tried 1.9.24 and 2.4). Had to patch 7 sites (`override val` + drop the redundant
   getter). Every SlateDB/uniffi upgrade risks re-introducing this.
2. **Version lockstep is mandatory.** Generating from clone `HEAD` (which had drifted past 0.14.1, adding
   `admin_run_gc_once`) against the *published* 0.14.1 `.dylib` throws `UnsatisfiedLinkError: symbol not
   found` at runtime. Fix: generate from the exact **`v0.14.1`** tag.
3. **A Kotlin bridge is mandatory.** `suspend` fns surface as `(args, kotlin.coroutines.Continuation)` — not
   ergonomically callable from Java. The FFM binding's clean `CompletableFuture` API is gone; you write
   blocking wrappers (`slatedb-jna-j11`'s `SlateBridge.kt`).
4. **You maintain a fork.** SlateDB publishes only the FFM artifact to Maven Central. Regenerate → re-patch →
   recompile → re-test on every upgrade.
5. **JNA marshalling overhead** per call vs FFM `MethodHandle` downcalls — likely negligible next to
   object-storage latency, but real and unmeasured for this workload.

### 17.5 Decision
- **Can run Flink on JDK 22+** → use the official FFM binding as-is (what §16 tests use). Zero fork
  maintenance, official artifact, clean `CompletableFuture` API. **Preferred.**
- **Hard-pinned to JDK 11/17** (e.g. a managed Flink platform that refuses newer JVMs) → the JNA fork is a
  viable, now-*proven* fallback; budget for §17.4. See `slatedb-jna-j11/README.md` for the reproducible
  build.

---

## Appendix A: Verification Provenance
- API signatures: `bindings/uniffi/src/{db,admin,builder,db_cache,settings}.rs` (`#[uniffi::export]` blocks) — the layer that generates the Java methods.
- Rust crate surface: docs.rs `slatedb` `struct.Db`, `admin`, `config::{Settings,CompactorOptions}`, `CloneSourceSpec`, `CloneBuilder`.
- Defaults: `slatedb/src/config.rs` (`Settings::default`, `CompactorOptions::default`), `slatedb/src/db_cache/mod.rs` (cache capacity constants), `slatedb/src/db/builder.rs` (`default_db_cache()` wiring).
- Version: crates.io `slatedb` 0.14.1, published 2026-07-01.
- Behavior (backpressure, compaction concurrency, clone semantics): RFCs 0002/0004/0024/0025 + design docs — used for *behavior*, cross-checked against shipped code where load-bearing.

---

## Appendix B: References (all sources consulted)

All URLs verified against the `main` branch / `latest` docs during this investigation (SlateDB 0.14.1; Flink 1.20 and 2.3). SlateDB `main`-branch source links track HEAD — pin to the `v0.14.1` tag for a stable reference when building.

### SlateDB — release & package metadata
- crates.io API (version 0.14.1, published 2026-07-01): https://crates.io/api/v1/crates/slatedb
- Context7 library id: `/slatedb/slatedb` (also `/websites/slatedb_io`)

### SlateDB — Rust crate API (docs.rs)
- `Db` struct: https://docs.rs/slatedb/latest/slatedb/struct.Db.html
- `admin` module: https://docs.rs/slatedb/latest/slatedb/admin/index.html
- `admin::CloneBuilder`: https://docs.rs/slatedb/latest/slatedb/admin/struct.CloneBuilder.html
- `admin::CloneSourceSpec`: https://docs.rs/slatedb/latest/slatedb/admin/struct.CloneSourceSpec.html
- `config::Settings`: https://docs.rs/slatedb/latest/slatedb/config/struct.Settings.html
- `config::CompactorOptions`: https://docs.rs/slatedb/latest/slatedb/config/struct.CompactorOptions.html
- `config::ObjectStoreCacheOptions`: https://docs.rs/slatedb/latest/slatedb/config/struct.ObjectStoreCacheOptions.html

### SlateDB — source (github.com/slatedb/slatedb, `main`)
- Repo file tree (API): https://api.github.com/repos/slatedb/slatedb/git/trees/main?recursive=1
- UniFFI binding — `Db`: `bindings/uniffi/src/db.rs`
- UniFFI binding — `Admin`/clone: `bindings/uniffi/src/admin.rs`
- UniFFI binding — builders (`DbBuilder`/`AdminBuilder`/`CloneBuilder`): `bindings/uniffi/src/builder.rs`
- UniFFI binding — `DbCache`: `bindings/uniffi/src/db_cache.rs`
- UniFFI binding — `Settings` wrapper: `bindings/uniffi/src/settings.rs`
- UniFFI binding — `KeyRange` and types: `bindings/uniffi/src/types.rs`
- UniFFI binding — shared Tokio runtime: `bindings/uniffi/src/runtime.rs`
- UniFFI binding — `Cargo.toml` (declares `slatedb = { features = ["all"] }`): `bindings/uniffi/Cargo.toml`
- Core — `Cargo.toml` (`default`/`all`/`foyer`/`moka` features): `slatedb/Cargo.toml`
- Core — config defaults (`Settings::default`, `CompactorOptions::default`, `ObjectStoreCacheOptions::default`): `slatedb/src/config.rs`
- Core — cache capacity constants (`DEFAULT_BLOCK_CACHE_CAPACITY` etc.) + `CachedKey` scoping: `slatedb/src/db_cache/mod.rs`
- Core — Moka cache backend: `slatedb/src/db_cache/moka.rs`
- Core — default cache wiring (`default_db_cache()`): `slatedb/src/db/builder.rs`
  - raw base: `https://raw.githubusercontent.com/slatedb/slatedb/main/<path>`

### SlateDB — docs & design (slatedb.io / repo `website/` + `rfcs/`)
- Java binding README: `bindings/java/README.md` (Maven `io.slatedb:slatedb-uniffi`, package `io.slatedb.uniffi`)
- Java quickstart: `website/.../docs/get-started/quickstart.mdx`
- Caching design (block cache, `SplitCache`, cross-instance sharing): `website/.../docs/design/caching.mdx`
- Clones design (shallow SST sharing, projection, nested clones): `website/src/content/docs/docs/design/clones.mdx`
- Compaction design: `website/.../docs/design/compaction/`
- Foyer hybrid cache config: `website/.../docs/operations/foyer-cache.mdx`
- Tuning (memory, `l0_max_ssts`, `max_unflushed_bytes`): `website/.../docs/operations/tuning.mdx`
- Standalone compactor CLI (`run-compactor`): `website/.../docs/operations/cli.mdx` + `tutorials/standalone-compactor.mdx`
- RFC 0002 — Compaction (write path, L0 backpressure, CAS+epoch fencing): `rfcs/0002-compaction.md`
- RFC 0004 — Checkpoints (checkpoint/clone API, union-merge L0 spike): `rfcs/0004-checkpoints.md`
- RFC 0011 — Transactions: `rfcs/0011-transaction.md`
- RFC 0024 — Segment-oriented compaction (backpressure invariant, `l0_max_ssts`): `rfcs/0024-segment-oriented-compaction.md`
- RFC 0025 — Distributed compaction (coordinator/worker, roadmap): `rfcs/0025-distributed-compaction.md`
- RFC 0027 — Decoupled object-store cache: `rfcs/0027-decoupled-object-store-cache.md`
- Python binding README (runtime notes, `SLATEDB_UNIFFI_RUNTIME_THREADS`): `bindings/python/README.md`

### Apache Flink — docs (verified per version)
- ProcessFunction — `onTimer`/`processElement` synchronization (1.20): https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/process_function/
- ProcessFunction (2.3, `open(OpenContext)`, sync guarantee): https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/process_function/
- State & Fault Tolerance — TTL, no expiry callback (1.20): https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/fault-tolerance/state/
- State V2 — async state, ForSt (2.3): https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/state_v2/
- Stateful stream processing — barriers, record-boundary snapshots: https://nightlies.apache.org/flink/flink-docs-master/docs/concepts/stateful-stream-processing/
- `RuntimeContext` API (1.20 — getters present/deprecated): https://nightlies.apache.org/flink/flink-docs-release-1.20/api/java/org/apache/flink/api/common/functions/RuntimeContext.html
- `RuntimeContext` API (2.3 — getters removed, use `getTaskInfo()`): https://nightlies.apache.org/flink/flink-docs-release-2.3/api/java/org/apache/flink/api/common/functions/RuntimeContext.html
- `MailboxExecutor` API (1.20): https://nightlies.apache.org/flink/flink-docs-release-1.20/api/java/org/apache/flink/api/common/operators/MailboxExecutor.html
- `AbstractStreamOperator` (1.20 — `YieldingOperator.setMailboxExecutor`): https://nightlies.apache.org/flink/flink-docs-release-1.20/api/java/org/apache/flink/streaming/api/operators/AbstractStreamOperator.html
- `StreamOperatorParameters` (1.20 — `getMailboxExecutor()`): https://nightlies.apache.org/flink/flink-docs-release-1.20/api/java/org/apache/flink/streaming/api/operators/StreamOperatorParameters.html

> **Note on source stability:** SlateDB source/docs links point to the `main` branch and docs.rs `latest`; they reflect HEAD at time of writing and may drift. For a build-stable reference, pin SlateDB source to the **`v0.14.1`** git tag and use versioned docs.rs (`/slatedb/0.14.1/...`). Flink links are pinned to release-1.20 / release-2.3 except the stateful-stream-processing concept page (master), whose barrier semantics are stable across these versions.

