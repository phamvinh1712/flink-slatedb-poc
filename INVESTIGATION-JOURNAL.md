# SlateDB-in-Flink — Investigation Journal

A chronological record of the prompts and discussion that produced this PoC and its design doc
(`README.md`). **This is a faithful reconstruction, not a verbatim transcript** — user prompts are quoted
as asked; each "Discussion" summarizes the answer/findings from that turn. Where a claim was later
corrected by *running code*, that's noted.

Method throughout: **verify against source, not memory.** Claims were checked against the shipped SlateDB
0.14.1 crate, its UniFFI Java binding (via `javap` on the real JAR), Flink 1.20/2.3 docs, and finally by
building and running 9 tests. Findings that only surfaced by running are marked ⚡.

---

## Phase 0 — Warmup: SlateDB vs RocksDB

**Q: "what do you think of slatedb versus rocksdb"**
Discussion: Not feature-for-feature — it's **local-disk (RocksDB, µs) vs disaggregated object-storage
(SlateDB, ms)**. RocksDB = mature, lowest latency; SlateDB = elastic, cheap for large state, but pre-1.0,
ms-latency, tuned for write-batching + read-caching. Flagged Flink's own **ForSt** as the native
disaggregated answer to weigh first.

---

## Phase 1 — How Flink state works, and the SlateDB idea

**Q: "how does flink track key range of a rocksdb instance"**
Discussion: Not arbitrary key ranges — **key groups**. `keyGroup = murmurHash(key.hashCode()) %
maxParallelism`; one RocksDB instance ↔ one subtask ↔ one contiguous `KeyGroupRange`. On disk, a key-group
byte prefix makes "a range of key groups" == "a contiguous byte range." Rescale = pick intersecting
handles → `deleteRange`-clip → merge.

**Q: "does flink expose KeyGroupRange to processfunction"**
Discussion: Not directly, but reconstructable from `RuntimeContext` via
`KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(maxP, p, idx)`. Runtime-internal API, no
stability guarantee. (Later ✅ verified by running in `flink-1.20-poc`/`flink-2.3-poc`.)

**Q: "if i use slatedb in a flink processfunction, can i do similar rescaling operation with rocksdb"**
Discussion: Not automatically (SlateDB is outside Flink's keyed-state backend). By hand — yes, and
potentially cheaper (object-store data is node-addressable). Introduced the layout choices; flagged
single-writer fencing. Established the exactly-once trap (S3 writes durable independent of Flink
checkpoints).

**Q: "i don't like forst, its a bad design."**
Discussion: Fair critique — the async-API tax, LSM-on-object-storage mismatch, leaky disaggregation (local
cache), C++ RocksDB fork maintenance. Counterpoint: ForSt is *integrated* (checkpoint/rescale/timers), which
is the whole cost of the SlateDB path.

**Q: "how the design would look like if i do this One DB per subtask (RocksDB-style)"**
Discussion: Full design — key-group-prefixed keys, `CheckpointedFunction` with union list state, rescale
merge loop (clone base + scan-copy others + logical clip). Flagged what needed verifying: clone-from-checkpoint
semantics and whether range-delete exists.

---

## Phase 2 — Verifying the SlateDB API against source (the "yes" chain)

**Q: "yes" (verify clone + range delete)**
Findings: ✅ `create_checkpoint` + `create_clone` (shallow, shares SSTs) confirmed. ❌ **range delete does
NOT exist** ("under development") → logical clipping is *mandatory*, not optional. Bonus: `merge` operator +
transactions exist.

**Q: "yes" (check shipped crate vs RFC)**
Findings: Released **0.14.1** (2026-07-01). checkpoint ✅ exact signature. clone ✅ but a **builder**
(`CloneBuilder`), not a free function — my earlier sketch was wrong. range delete ❌ confirmed absent on the
shipped `Db`. `merge`/`begin`/`scan` all present.

**Q: "slatedb api read/write is async, how do i integrate it with flink? give me an example"**
Findings: SlateDB ships an official **Java binding** (`io.slatedb:slatedb-uniffi`) exposing
`CompletableFuture` — no JNI needed. Gave blocking `KeyedProcessFunction` (safe RMW, single-threaded per key)
vs `RichAsyncFunction` (non-blocking, unsafe for RMW). Third path: mailbox — which is "rebuild ForSt."

**Q: "yes" (check Rust API surface: CloneBuilder + CloneSourceSpec)**
Findings: `CloneBuilder` builder methods + `CloneSourceSpec { path, checkpoint: Option<Uuid>, projection_range }`
with `with_checkpoint(path, uuid)`. `build()` is the async terminal.

**Q: "yes and yes" (binding Java signatures)**
Findings/corrections: checkpoint is on **`Admin.createDetachedCheckpoint`**, NOT `Db`. `close`→exported as
**`shutdown`**. `put`/`merge` return `WriteHandle` not `Void`. Builders **not chainable** (void setters).
`AdminBuilder.build()` is **sync**. `merge` + `begin` confirmed in the binding.

**Q: "yes" (AdminBuilder construction)**
Findings: Two builder-injection routes; `AdminBuilder(path, store).build()` sync. Also caught: builder
setters return void → statement-per-call, not fluent.

---

## Phase 3 — Compaction, backpressure, memory, disk

**Q: "how do i run compaction for slatedb within flink?"**
Findings: Compactor runs **embedded in the writer process by default** (inside the TaskManager JVM).
Options: embedded, standalone sidecar (`run-compactor`), or distributed (RFC-only). Disable embedded worker
via `compactor_options.worker = null`. Single-writer invariant.

**Q: "does compaction task block write?"**
Findings: Not directly (compaction reads immutable SSTs; writer/compactor coordinate via **CAS + epoch
fencing**). BUT writes stall via **L0 backpressure** when compaction falls behind → the pipeline-wide risk.

**Q: "yes" (L0 threshold) + "yes" (defaults)**
Findings: **Two** distinct fields — `l0_max_ssts_per_key` (the real write-stall driver, default **8**) vs
`l0_max_ssts` (total, manifest bookkeeping). Defaults: `poll_interval=5s`, `max_unflushed_bytes=1GiB`,
`l0_sst_size_bytes=64MiB`. The `8 × 5s` interaction = bursty stalls.

**Q: "how does slatedb use java memory? ... which flink memory section?"**
Findings: SlateDB memory is **100% native/off-heap** (FFM, not JNA — 240 `foreign` refs). Lands in **NO
Flink-managed section** (not Heap, not Managed Memory) → **silent container OOM** (`exit 137`, no JVM error).

**Q: "yes" (block cache default) + "yes" (builder wiring)**
Findings: `DEFAULT_BLOCK_CACHE_CAPACITY=512MiB` + `128MiB` meta = **640MiB/DB** auto-installed by
`default_db_cache()`. Feature-gated on `foyer`/`moka`.

**Q: "what is the risk of out of memory ... does slatedb spill to disk ... can i limit memory?"**
Findings: **No spill** — under pressure SlateDB *evicts cache* + *applies backpressure*, never pages to
disk. Limits are **soft** (async cache eviction can overshoot). Per-DB ≈ **1.66 GiB** at defaults;
one-DB-per-subtask × slots → OOM territory.

**Q: "yes" (verify no-spill + soft limits)**
Findings: Confirmed from source — Moka cache is a soft/approximate target; `max_unflushed_bytes` is the
dominant, firmer bound.

---

## Phase 4 — The design doc, then hybrid tiering

**Q: "gather all infos we discussed in a design doc"**
Produced `slatedb-flink-integration-design.md` (§0–§12 + provenance), later folded into `README.md`.

**Q: "if i want to use a combination of rocksdb state with slatedb ... 1 day hot, older cold"**
Discussion: Pattern 1 (write-through mirror, SlateDB authoritative — simple) vs Pattern 2 (true demotion —
real offload, harder). The consistency contract: **write-cold-before-clear-hot**, **hot-wins reads**, tie
SlateDB cp id into Flink state.

**Q: "do (a). explain more what you mean by async/mailbox path"**
Findings (verified vs Flink docs): ✅ `onTimer`/`processElement` synchronized; ✅ TTL has **no** expiry
callback (so explicit timers are the only demotion mechanism); ✅ checkpoints snapshot at clean record
boundaries. Explained the `MailboxExecutor` continuation pattern and why building it = rebuilding ForSt.

**Q: "yes" (fold hybrid into doc)** → §13 added.

---

## Phase 5 — Version matrix, resharding, gap analysis

**Q: "yes, i use flink 1.20"** + mid-turn **"verify against flink 2.3 too, i use both"**
Findings: **1.20** = no ForSt, no async state, `open(Configuration)`, RuntimeContext getters
deprecated-but-present. **2.3** = ForSt + `enableAsyncState()` + `state.v2`, `open(OpenContext)`, getters
**removed**. Neither exposes `getMailboxExecutor()` on RuntimeContext (correction to an earlier claim).

**Q: "yes" (mailbox injection + decision guide)**
Findings: 1.20 mailbox via `AbstractStreamOperator.setMailboxExecutor` (V1) or
`StreamOperatorParameters.getMailboxExecutor()` (V2). Added §0.1 decision guide (goal × version).

**Q: "yes to this" (JAR cache features)**
Findings: binding `Cargo.toml` uses `features=["all"]`; core `all` includes `foyer`+`moka` → JAR ships
cache **ON** → the ~1.66 GiB/DB path.

**Q: "reevaluate shard-per-bucket ... does it hurt flink performance if i spin up many slatedb instances?"**
Findings: The **N/P trap** — per subtask you run N/P instances. ✅ Tokio runtime is process-wide (no thread
explosion); ✅ `DbCache` shareable. ❌ per-instance: `max_unflushed_bytes`, embedded compactors. **Corrected
my over-sold "fixed small DB count" claim.**

**Q: "yes" (cache sharing) + "yes" (cache-key namespacing)**
Findings: `CachedKey { scope_id, sst_id, block_id }` — per-DB `scope_id` makes shared cache
**collision-safe**. (Later ✅ confirmed live at P=4.)

**Q: "how can i reshard if i want to change the bucket size later?"** + **"yes"**
Findings: Changing bucket size = changing N = a state migration, not live. Recommended: **decouple logical
shard from physical DB** (fine fixed physical granularity → resharding = re-map, free). No native DB
split/merge; clone-with-projection is the split tool. `maxParallelism` must be fixed forever.

**Q: "if i want to use slatedb as a replacement for rocksdb statebackend ... what is the missing feature?"**
+ **"yes"**
Findings: The blocker is **not a feature** — it's the sync/async execution-model mismatch (needs Flink's
async state framework). Engine gaps that remain: **range-delete** (absent) and **compaction filter**
(exists in Rust, ❌ not exposed in the Java binding). Meta: most of "what's missing" is the Flink
integration layer = "reimplement ForSt with a SlateDB core."

**Q: "verify again all the API that we need for flink is available in java"**
Findings: All needed types present (`WriteBatch`, `DbIterator`, `KeyValue`, `DbReader`+`withCheckpointId`,
`CheckpointOptions`, `CheckpointCreateResult`). Corrections: `CheckpointCreateResult.id()` is a **String**;
`DbReader.withCheckpointId` confirms the rescale-read path; `CheckpointScope` not bound (not needed).

---

## Phase 6 — Correctness deep-dive (the showstoppers)

**Q: "review all your findings again, are there any major risk ... something you're not confident about?"**
Discussion: Named showstoppers — (1) exactly-once restore semantics **unverified**, (2) failover fencing
window, (3) pre-1.0 format stability, and unverified operational risks (tail latency, S3 cost, no
production precedent). Admitted incremental verification made it look more viable than it was.

**Q: "yes" (verify exactly-once restore + fencing)**
Findings: ✅ pinned **reader** excludes post-checkpoint WAL (test-backed). ❌ **writer reopen RESURRECTS
post-barrier WAL** (`fence.rs` — replay bound = storage scan). Fencing window confirmed to exist. The two
are coupled.

**Q: "yes" (read WAL-replay code)**
Findings: `db_reader.rs` — checkpoint-pinned reader bounds replay by manifest (`replay_new_wals=false`),
verified by a test. `fence.rs` — writer reopen scans storage → resurrects. So restore must go through
**clone-from-checkpoint**, not writer reopen.

**Q: "yes" (read clone.rs WAL handling)**
Findings: ✅ **RESOLVED SAFE** — `clone.rs` reads manifest at `checkpoint.manifest_id` and bounds WAL copy to
`[replay_after_wal_id+1, next_wal_sst_id)` from the checkpoint state → post-checkpoint WAL **not carried
over**. Test `should_clone_from_checkpoint` confirms. Exactly-once via clone-on-restore is **sound**.

**Q: "yes" (write the validation test)**
Produced `CloneRestoreExactlyOnceTest.java` (later realized as `slatedb-verify`).

---

## Phase 7 — Building & running the PoC

**Q: "create a directory flink-slatedb-poc ... sample flink apps (1.20 and 2.3) ... run via docker compose"**
Discovery: Docker daemon not running initially; ✅ `io.slatedb:slatedb-uniffi` IS on Maven Central;
⚡ **the JAR is Java-22 FFM (class version 66)** — collides with Flink's supported JVMs. Flagged before
building.

**Q: "i just start docker"** → daemon up. Then inspected the JAR: fat multi-platform native libs
(linux/darwin/win). Extracted exact Java signatures via `javap`.

**Q: "can you just import slatedb in maven?"** → yes, added as a normal Maven dependency (no manual JAR).

**Q: "just spin up 2 flink projects using maven then, ignore docker"** + **"yes"**
Built `flink-1.20-poc` (JDK 11) and `flink-2.3-poc` (JDK 17). ✅ Both compile against Maven-resolved Flink +
SlateDB. Discovered JDK 11/17 `javac` **cannot read** the Java-22 SlateDB classes → moved SlateDB code to a
separate `slatedb-verify` module (JDK 22+). Installed Temurin 25.

**Q: "does it mean it's impossible to run slatedb with flink because of java version conflict?"**
⚡ Findings: **No.** Empirically verified **Flink 1.20 AND 2.3 both run on JDK 25** (all checks pass) with
`--add-opens`. Surmountable but officially unsupported. Corrected my earlier "showstopper" framing.

---

## Phase 8 — End-to-end and the higher-fidelity tests

**Q: "do it"** (end-to-end job) / **"yes"** (record findings + README)
Built `FlinkSlateDbE2E` — real Flink keyed operator + SlateDB, clone-restore exactly-once. ⚡ **Found a
silent-data-loss bug: `flush()` ≠ manifest commit** — `Db.flush()` is WAL-only; a checkpoint pins the
manifest, which needs a **memtable** flush. Fix: `flushWithOptions(MEM_TABLE)`. ⚡ `ObjectStore.resolve`
needs empty path (root) + db-name to builder.

**Q: "is there anything in the doc you haven't verified with example app?"**
Honest audit: only ~20–25% verified; the composition and operational claims were untested. The central
thesis (SlateDB *embedded in a running Flink job*) had not been run end-to-end.

**Q: "yes"** (build end-to-end) — done above; ⚡ `fromData` drains instantly (needs throttled source).

**Q: "test rescale"**
Built `SlateDbRescaleE2E` — §6.4 merge with real clone+projection+scan-copy. ✅ downscale 3→2 (multi-source)
+ upscale 2→4 (projection clip), both directions, all invariants (no loss/dup/foreign-key). ⚡ **projection
physically clips** (stronger than the doc assumed).

**Q: "yes"** (test recovery)
Built `FlinkSlateDbRecoveryE2E` — **real** `enableCheckpointing` + throttled replayable source + induced
failure + genuine Flink recovery. ✅ every key counted exactly 10 across the failure → exactly-once through a
real recovery cycle. ⚡ source offset must be checkpointed in lockstep with the SlateDB checkpoint id.

**Q: "yes"** (hybrid test)
Built `FlinkHybridTieringE2E` — RocksDB-hot + SlateDB-cold in a real operator. ✅ demote/promote/hot-wins,
exact counts through demote→promote→re-demote (demotions=5, promotions=2). ⚡ Java records break Kryo on
JDK 25 → POJOs.

---

## Phase 9 — Being pushed to test what actually matters

**Q: "review all your findings again, are there any major risk ... not confident about?"**
Discussion: Correctness now strong (PoC even caught a data-loss bug). Remaining worries: unsupported-JVM
footing, pre-1.0 format stability, zero production precedent, and **everything operational** (tail latency,
S3 cost, memory OOM). Meta-concern: **my tests are gentle, so they're missing bugs.**

**Q: "have you tested on parallelism > 1?"**
Honest answer: **No.** Every test with embedded SlateDB ran at `setParallelism(1)`. The pure-Flink apps used
P=2/3 but no SlateDB; rescale used simulated handles single-threaded. Stated the gap plainly.

**Q: "yes. have you tested Shard-per-bucket? ... test compaction?"**
Built two tests:
- `SlateDbCompactionE2E` — ✅ observed L0 peak→drain to 0, sorted runs grew, **2000 keys intact** across
  compaction. ⚡ `Settings.set` takes JSON values (durations as quoted strings).
- `FlinkShardPerBucketParallelE2E` — ✅ **P=4, 16 shards, 16 concurrent SlateDB instances**, shared cache per
  subtask; 800 keys each exactly 3; clean partition + ownership. First real P>1 embedded test; §12.7
  shared-cache collision-safety confirmed **live**.

**Q: "why did you miss the 3 cases above? review again any cases you haven't tested and test them too."**
Honest diagnosis: **"first green checkmark = done"** — I tested each concept's *logic* at the simplest
config and skipped the production-relevant one (P>1, under compaction, at scale). Produced a full
coverage audit. Still untested (need real infra): §8 backpressure stall, §9/§9A memory-OOM/disk, §14
reshard, real parallel savepoint→rescale, all §11 operational risks.

**Q: "move the design doc inside readme of flink-slatedb-poc"**
Merged the full design doc into `README.md` (self-contained), fixed stale `../` references, removed the
standalone file.

**Q: "collect all the prompts and discussions we have here in a markdown file"**
This file.

---

## Final test scorecard (9 tests, all passing — laptop/MiniCluster only)

| Test | Verifies | Result |
|---|---|---|
| `flink-1.20-poc` | §3/§0/§13.5 key groups, signatures, ownership (1.20) | ✅ |
| `flink-2.3-poc` | §0/§15.2 async-state exists + enforced (2.3) | ✅ |
| `slatedb-verify` | §12.8 clone-restore exactly-once (standalone) | ✅ |
| `FlinkSlateDbE2E` | §6.1–6.6 Flink operator + SlateDB composed | ✅ |
| `FlinkSlateDbRecoveryE2E` | real checkpoint + induced failure + recovery = exactly-once | ✅ |
| `SlateDbRescaleE2E` | §6.4 rescale merge, both directions | ✅ |
| `FlinkHybridTieringE2E` | §13 RocksDB-hot + SlateDB-cold | ✅ |
| `SlateDbCompactionE2E` | §7 compaction observed + no data loss | ✅ |
| `FlinkShardPerBucketParallelE2E` | §4/§4.1/§12.7 P=4 shard-per-bucket, shared cache | ✅ |

## Bugs / corrections that only RUNNING surfaced (⚡)

1. **`flush()` ≠ manifest commit** → silent state loss; needs memtable flush (§16.2). *Most important.*
2. **Flink 1.20 & 2.3 run on JDK 25** → the Java-version conflict is surmountable (§16.3).
3. **`withProjectionRange` physically clips** — stronger than the design assumed (§16.9).
4. **`fromData` drains instantly** → checkpoint/timer tests need throttled replayable sources.
5. **Java records break Kryo on JDK 25** → use POJOs.
6. **`ObjectStore.resolve` needs empty path** (root) + db-name to builder (§16.7).
7. **`Settings.set` takes JSON values** — numbers bare, durations quoted strings (§16.11).

## Still genuinely unverified (need real object storage + load, not a MiniCluster)

- §8 L0 write-stall backpressure (never overwhelmed compaction).
- §9/§9A memory footprint / silent OOM / no-spill / disk contention.
- §14 resharding (changing N).
- Real parallel savepoint→rescale-across-restart (algorithm + parallel operation tested separately, not fused).
- All §11 operational risks: S3 tail latency (p99/p999), request cost, pre-1.0 on-disk format stability,
  failover fencing under real concurrency.

## The through-line

Every "yes" was a request to **verify against source rather than assert from memory** — and it repeatedly
paid off: the clone API shape, the checkpoint-on-Admin, the range-delete absence, the FFM/Java-22 floor, and
above all the `flush()`-≠-manifest **data-loss bug** were all things that memory/source-reading got wrong or
missed and only verification/running caught. The design is **correct across everything testable on a
laptop**; its **production viability remains unproven** and is where adoption should still be gated.
