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

## Phase 10 — Breaking the Java-22 floor (the JNA binding)

**Q: "i've cloned slatedb repo, review the rust java binding. is it possible to change this binding to
support java 11/17"**
Discussion (verified against the clone + the published JAR): the published `io.slatedb:slatedb-uniffi` is
**100% machine-generated** by `uniffi-bindgen-java` (IronCoreLabs), which emits **`java.lang.foreign` / FFM**
code — decompiled the 0.14.1 JAR: bytecode **major 66**, 40 classes use `java.lang.foreign` (`Linker`,
`SymbolLookup`, `Arena`, `upcallStub`, `downcallHandle`), **zero** use JNA. `Arena` is a *final*-API type →
the hard **Java 22** floor (JEP 454). So "editing" the binding to target 11/17 isn't a config knob. **But** the
native lib exports a plain **C ABI** (441 symbols, generator-agnostic), and UniFFI's Mozilla **Kotlin** backend
uses **JNA** (Java 8+). So the floor is a *packaging* choice, removable by regenerating. ⚡ Resolved a
contradiction: the repo's `build.gradle` + 0.14.1 POM declare a JNA dependency, but the generated code never
calls it — JNA is used only for platform detection / native-lib extraction; the FFI is all FFM.

**Q: "yes" (actually run the Kotlin-backend regeneration on JDK 17 → verified yes/no)**
Did it end to end: built `uniffi-bindgen` 0.31.1 + the native lib (both matching `Cargo.lock`), generated the
**Kotlin/JNA** binding (19k lines, pure JNA, 167 `suspend` fns), compiled to JVM 17 and **ran real
put/get/flush + checkpoint on JDK 11, 17, AND 25 — all PASS**, no `--enable-native-access`, no `--add-opens`.
⚡ Two costs surfaced by *doing* it: (a) a **uniffi-0.31 codegen bug** — error variants declare a `message`
ctor property colliding with `Throwable.message`, doesn't compile on any kotlinc (patched 7 sites); (b)
**version lockstep** — generating from clone HEAD (drifted past 0.14.1, added `admin_run_gc_once`) against the
published 0.14.1 `.dylib` throws `UnsatisfiedLinkError` at runtime. Also: `suspend` fns need a Kotlin bridge to
be callable from Java (the FFM binding's `CompletableFuture` API is free by comparison).

**Q: "yes" (fold into the PoC as a 5th module + journal entry)**
Built **`slatedb-jna-j11`**: the regenerated binding taken from the exact **`v0.14.1`** tag (so it matches the
published native lib) + the patch + a Kotlin blocking bridge + a plain-Java driver, wired with the Kotlin Maven
plugin. `mvn compile` + `exec:java` **PASS on JDK 11, 17, and 25** reusing the published native lib (no Rust
toolchain to run). Documented as **§17** in the README. The floor is now not just *tolerable* (§16.3, Flink on
JDK 25) but *removable* — at the cost of maintaining a binding fork.

---

## Phase 11 — Does SlateDB clean up orphan files after compaction?

**Q: "does slatedb clean up orphan files after compaction?"**
Discussion (source): compaction rewrites the manifest and leaves old SSTs as orphans; a background GC (on by
default — `Settings::default` populates all dirs; 60s interval, 300s `min_age`) deletes them, gated by
`min_age` + compaction low-watermark + *not referenced by latest manifest or any live checkpoint*. Then, to
hold to the "verify by running" standard, I wrote `SlateDbGcE2E` to observe physical deletion.

⚡ **Found by running — the design assumption was wrong.** With `min_age=1s` and a long idle, GC deleted
**nothing**. I falsified four hypotheses in a row (low-watermark pinning, multi-round watermark advance,
manifest "active" set, path-parse failure) by instrumenting with SlateDB's own DEBUG logs and decoding SST
ULIDs vs the GC cutoff. The truth: **on every manifest commit the compactor writes an internal checkpoint with
a hardcoded 900s (15-minute) expiry** (`compactor_state_protocols.rs:250`, *"so it's extremely unlikely for the
gc to delete ssts out from underneath the writer"*). That checkpoint pins the manifests referencing the
just-compacted L0s, so orphans survive **~15 min regardless of `min_age`.** Confirmed both directions:
`SlateDbGcE2E` observed 54 files on disk / 5–8 referenced / **12 checkpoints with `lifetimeSecs=900`**, GC
retaining; a 17-min run watched files drop **15 → 7 at t+900s → 4 at t+930s** (11 SSTs deleted), exactly at the
expiry. Answer: **yes, but reclaim lags compaction by ~15 min, and it's not tunable from the Java binding.**
Added `SlateDbGcE2E` (fast) + `SlateDbGcLongE2E` (~17 min) and README §16.13 / §9A.

Also answered inline (source-verified, no new tests): **bloom filters** — yes, on by default (10 bits/key,
`min_filter_keys=1000`), exposed in Java via `FilterPolicy`/`BloomFilterOptions`, plus a `PrefixExtractor` for
prefix filters. **Foyer vs Moka** — both implement the in-memory *block* cache; **foyer** is default and has a
hybrid memory+disk variant, **moka** is memory-only (Caffeine port); distinct from the object-store disk cache.
**~1000 DBs in one Flink task** — the N/P trap: Tokio runtime is a process-wide singleton (threads don't
scale), but per-DB native memory does (~1.1 GiB/DB at defaults ⇒ ~1.1 TiB for 1000 ⇒ OOM), plus 1000× logical
compactor/GC/flusher tasks and S3 request load; mitigate with a shared block cache + few physical DBs holding
many logical buckets. Not viable at defaults.

**Q: "is there a lot of serialization overhead between rust java binding?" → "do it" (benchmark it)**
⚡ Built `SlateDbMarshalBench` (memory:/// → isolates FFI+copy+async, cache hits only) and measured. Result
*corrected* my source-only reasoning: **serialization is cheap** (bulk `Vec<u8>`↔`byte[]` memcpy via RustBuffer;
16 B→16 KB adds only ~4 µs/op), but the **per-call async round-trip latency dominates** (~44 µs serial, CPU at
0.1% = blocked waiting, not copying; pipelining 256 in-flight → ~10-14 µs/op, 97k/s). Also found the flat
~1 ms/batch PUT is **durability** (`await_durable=true` default WAL flush), not marshalling. Verdict: binding
overhead is a non-issue vs S3 latency (cache-miss get ~1-10 ms = 100-200× the FFI cost); guidance = pipeline /
`WriteBatch`, don't chatty-await. Added README §16.14 + the benchmark to the e2e module.

**Q: "if i write and read in the same db instance before flushing, do i get consistent result? verify with a test"**
✅ Built `ReadYourWritesE2E` (memory:///, NO flush/checkpoint anywhere → data only in the memtable). 8 checks all
pass: put→get sees value; overwrite→get sees LATEST (MVCC newest-seq); delete→get sees null (tombstone pre-flush);
RMW loop ×500 reads own writes (final=500); 2000 keys all consistent; scan over unflushed range in order; and
`await_durable=false` writes still immediately readable. Confirms **read-your-writes is guaranteed within a
single instance before any flush**, and **visibility ≠ durability** (memtable gives RYW; flush/checkpoint only
govern crash-safety + what a *separate* reader/clone sees). This is why §16.2's flush-before-checkpoint rule was
about the *restore* path, not live reads. Source: `batch_write.rs` (sync memtable apply) + `reader.rs`
(newest→oldest). Added README §16.15 + §6.2 note.

**Q: "how can i make slatedb serialize/deserialize java objects like flink does in rocksdb?" + "how to avoid
repeated deserialization?" + "does an on-heap object cache defeat slatedb's cache?"**
✅ Built `FlinkSerdeSlateDbE2E`: reuse Flink's own `TypeSerializer` (from `TypeInformation.createSerializer`)
+ `DataOutputSerializer`/`DataInputDeserializer` — the identical primitives `RocksDBValueState` uses — and hand
the resulting `byte[]` to SlateDB (which stores bytes, no type info, like RocksDB). Flink picked `PojoSerializer`;
1000 POJOs + object RMW round-trip intact. ⚡ Two gotchas by running: collection fields go through Kryo so must be
mutable (`ArrayList`, not `List.of` — immutable breaks on later `.add()`); POJOs not records (Kryo/records break
JDK 16+). Repeated-deserialization answer: SlateDB's block cache caches decoded *blocks* (bytes), not objects, so
each hit still costs FFI-copy + deserialize (~few µs, §16.14) — but that's swamped by the ~1-10 ms S3 read the
block cache already saves, so **relying on the block cache is the right default**; only add a bounded on-heap
object cache (the §13 hot tier) if profiling shows deserialize is a real hotspot. It does NOT defeat SlateDB's
cache (different layers: objects vs bytes) but double-caches the hot set at full size → shrink the block cache if
you add one. Schema evolution is the one RocksDB feature you'd rebuild (no `TypeSerializerSnapshot` integration).
Added README §6.1a + §16.16.

**Q: "trigger compaction from a downstream Flink operator + pause/resume the writer? can Flink signal
downstream→upstream?" → "verify compaction doesn't block writes" → "just point me to the doc/test that
confirms it"**
Discussion: the premise doesn't hold — (1) compaction is non-blocking (reads immutable SSTs, swaps manifest
via CAS+epoch, no lock), so writes never need pausing; (2) the Java binding exposes only `CompactorStateView`
(read-only) — no `CompactorBuilder`/`run-compactor`, so you can't trigger compaction from Java anyway
(standalone compactor is Rust-only); (3) Flink dataflow is strictly upstream→downstream — no native back-edge.
Embedded compactor polls every 5s (default); disable via `compactor_options=null` (⚠️ then run a sidecar or L0
backpressure eventually stalls writes). ⚡ **Tried to write our own non-blocking test and abandoned it** — a
single-machine flush-per-batch workload kept inducing **L0 backpressure** (writes pausing ~9s at the
`l0_max_ssts_per_key=8` wall), which *looks* like blocking but is the §8 phenomenon, not compaction
stop-the-world. Couldn't cleanly separate the two on a laptop. Correct move: cite SlateDB's own evidence —
`rfcs/0002-compaction.md:383` (compactor runs alongside the writer) + the CAS protocol (184–190/331–333) +
in-tree test `test_compactor_compacts_l0` (`compactor.rs:1602`, writes to a live DB with compactor active, reads
back intact). Added provenance to README §7.3; deleted the abandoned test. Lesson: not everything is best
verified by our own running — sometimes the authoritative proof is upstream's tests against the real code.

**Q: "how do I use RFC-0004 projection/union for database merge/split? show an example" → "yes" (build it)**
Read RFC 0004's Projection/Union section, then verified the API is reachable from Java (via `javap` on the JAR):
**split = projection** (`CloneBuilder.withProjectionRange` / per-source `KeyRange`), **merge = union**
(`CloneBuilder.withSource` called repeatedly — it *accumulates* sources; discovered this isn't obvious from the
RFC). Built `SlateDbMergeSplitE2E`: populate 600 keys → flush → split into two projected shards → union them
back. ✅ All 5 checks pass: each shard has exactly its half (projection clips cleanly), union has all 600 keys
correct, merged DB is writable. RFC rules confirmed: union needs **non-overlapping + adjacent** ranges and
**WAL flushed to L0 first**; `checkpoint=null` clones latest. This is the clean RFC way to do the §6.4 rescale
(downscale=union, upscale=projection) — closes the gap `SlateDbRescaleE2E` left (it did merge via manual
scan-copy). Added README §6.4 note + §16.17.

---

## Phase 12 — Systematic RFC gap sweep (all 28 RFCs)

**Q: "look through each of the rfc in rfcs folder of slatedb and verify if there is any potential gaps you're
missing"** → **"a"** (fold into the design doc)
Fanned out **6 parallel readers** over all 28 RFCs, each cross-checking against the design-doc outline; one
stalled so its 5 RFCs were covered from source directly. Every load-bearing finding was **re-verified against
v0.14.1 source / the JAR (`javap`)** before writing — not taken on the agents' word. The sweep confirmed the
doc had exactly the blind spots a question-driven doc would: whole capability areas (object-store requirements,
observability, checkpoint lifecycle) never prompted. Material gaps found:
- ⚠️ **Conditional-write requirement** (RFC 0001, VERIFIED in txn-obj source: `put_if_not_exists`/`If-Match`) —
  manifest CAS + fencing + exactly-once all silently require an object store with conditional PUT (S3 only since
  Aug-2024); never stated, never tested (only `file://`/`memory://`). The headline gap.
- **Detached-checkpoint lifecycle** (0004/0005) — expiry → unrestorable Flink checkpoint; no-expiry → leak; no
  `Db::destroy` for old generations; iterator invalidation can truncate a rescale scan-copy.
- **Big-endian key-group prefix** (0016 rejected → bytewise-lexical forever) — never stated; rescale/projection
  silently break otherwise.
- **Segment-oriented compaction** (0024, VERIFIED `withSegmentExtractor` bound) — a 3rd layout option that
  attacks the N/P trap; §4 omitted it.
- **No observability plan** (0021, VERIFIED `DefaultMetricsRecorder` bound) — doc names risks, not the metrics.
- **Corrections:** TTL cleanup DOES work from Java (`putWithOptions(ttl)` bound + built-in TTL filter; VERIFIED)
  — §15.3 was wrong; `await_durable=true` NOT mandatory for exactly-once (barrier-flush is the durability
  point) — §7.3 overstated. Plus: `withWalObjectStore` bound (WAL-on-S3-Express latency fix), `CloseReason.Fenced`
  error contract, CDC/`WalReader` shipped in binding, wall-clock-only TTL (no event-time; `clock` not injectable).
Folded everything into **README §18** (12 sub-items, each tagged [VERIFIED] vs [RFC]) + inline corrections to
§7.3/§15.3 + two new §11 risk rows + two new §12 open-questions. This phase found gaps by *reading*, and the
most important ones were then confirmed against source — the same verify-don't-assert discipline, applied to
completeness rather than a single claim.

**Q: "from rfc sweep, is there something you're not confident about and should verify from actual test?" →
"yes"**
Two sweep claims backed doc changes, so I tested them (`SlateDbTtlE2E`, `SlateDbFencingE2E`) — opposite outcomes,
both valuable:
- ✅ **Fencing (§18.9) — CONFIRMED as written.** 2nd writer on the same path fences the 1st; the 1st's next
  write throws `Error.Closed{reason=FENCED, "detected newer DB client"}`. The operator's dead-handle signal is real.
- ⚡ **TTL (§18.6) — CORRECTED; the sweep AND my first doc-fix were both wrong.** I'd written "SlateDB filters
  expired rows on read." Running showed it does NOT: a point-`get` after the TTL still returns the value; expiry
  only takes effect when a compaction *merge* rewrites the SST (confirmed by forcing a merge → then NULL). So
  TTL is **lazy compaction-reclamation, not read-time expiry** — a Flink cold tier must still filter expiry
  itself. This is the **second** reasoned "correction" the tests caught wrong (cf. the abandoned
  compaction-non-blocking test): reading finds candidates, running finds truth. Rewrote §18.6/§15.3 accordingly;
  added §18.12 recording both graduations.

**Q: "the full stop-savepoint → rescale → restart cycle fused with projection/union inside a real Flink job — can you test this?" → "yes"**
⭐ Built `FlinkRescaleSavepointE2E` — the flagship integration test, closing the gap I'd explicitly flagged as *inferred, not tested*. A real `MiniCluster` (Flink 1.20.1, JDK 25) runs three jobs via `submitJob` + `stopWithSavepoint` + `SavepointRestoreSettings`: **P=2 → savepoint → P=4 (upscale) → savepoint → P=1 (downscale)**. Each subtask's SlateDB (dbPath|cpId|keyGroupRange) rides in **union list state** (the primitive Flink redistributes to every subtask on rescale); on restore each new subtask computes its `KeyGroupRange` and **unions the old shards intersecting it, projecting each to the overlap** (`CloneBuilder.withSource`) — RFC-0004 union+projection **driven by the redistributed savepoint state**. Result: every key counted **exactly 9** (3 runs × 3 rounds) across both rescales — exactly-once preserved. This graduates §6.4 rescale from "algorithm verified standalone + inferred-in-Flink" to **verified fused with the real Flink savepoint/rescale lifecycle**. Scope: single-JVM MiniCluster on `file:///`; real-S3/multi-node scale still unproven. Added README §16.18, updated §16.5, journal scorecard (18).

---

## Phase 13 — Full JDK-25 sweep + Flink-2.3-supports-25 fact-check

**Q: "i think flink 2.3 now support java 25, can you verify?"**
Checked the live Flink docs (not memory — 2.3.0 released 2026-06-25, after my Jan-2026 cutoff). **Verdict: NO.**
The Flink 2.3.0 [Java compatibility page] lists **Java 17 (default/recommended)** and **Java 21 (experimental)**
only — no Java 25. 2.3.0 is the current latest release, so the page is up to date. So "Flink 2.3 supports 25" is
wrong. The doc's §0/§16.3 stance ("runs on 25 but officially unsupported") was already correct — the likely
confusion was conflating the empirical §16.3 result (it *runs*) with official support (it doesn't).

**Q: "can you try to use java 25 for all of the tests to verify" → then "if all pass, remove jna-j11..." →
clarified to "keep it, just demote"**
Ran **every** runnable test on Temurin 25.0.3 (recompiled the two Flink modules on JDK 25 too). **All green:**
`flink-1.20-poc` (Flink 1.20.1) ✅, `flink-2.3-poc` (Flink 2.3.0, incl. async-state runtime enforcement) ✅,
`slatedb-verify` ✅, and 13 e2e mains run ✅ (FlinkSlateDbE2E, Serde, ReadYourWrites, HybridTiering,
ShardPerBucketParallel×4, Recovery, RescaleSavepoint, Rescale, Fencing, TTL, MergeSplit, Compaction) — GcLong
(~17min) + MarshalBench not re-run but have only ever run on 25. Needed `--add-opens java.base/{util,lang,time,
util.concurrent}=ALL-UNNAMED`. This fully backs §16.3: previously only the FFM/SlateDB tests were confirmed on
25; now the pure-Flink modules are too.
Consequence: since the FFM binding on JDK 25 is now fully validated, **demoted §17/`slatedb-jna-j11` to
FALLBACK-ONLY** (only for platforms hard-pinned below JDK 22). Did NOT delete it — the JDK-11/17-pinned escape
hatch is a *different* case than "runs on 25," so the knowledge + runnable proof are kept, just clearly marked
as not-the-recommended-path. Added the dated JDK-25-sweep banner + upstream-docs citation to §16.3 and the
results block; §17 gets a fallback-only banner.

**Q: "how does slatedb decide which records to evict from cache?" → "what metrics does slatedb expose, how to
wire to flink?" → "a and b"**
Eviction: corrected the premise — SlateDB evicts **blocks** (byte-weighted `CachedEntry`), not records; the
algorithm is the backend's default (moka = W-TinyLFU, foyer = S3-FIFO — both scan-resistant), SlateDB sets only
the weigher+capacity (verified in `db_cache/{moka,foyer}.rs`). Metrics: built `SlateDbMetricsE2E` — wired a
`DefaultMetricsRecorder` (default is no-op!), did work, dumped `snapshot()`. ⚡ Discovered the **real 0.14.1
catalog: 43 metric names / 125 label-series** — and several RFC-quoted names in the sweep were WRONG (e.g.
S3 latency is `object_store.request_duration_seconds` histogram, not `db.request_duration_seconds`; cache is
`db_cache.access_count{entry_kind,result}`). Wrote **§19 Observability** with the verified catalog + Flink
`MetricGroup`-polling wiring code (per-subtask recorder → Flink's existing reporter); marked §18.5 CLOSED.
Two compile fixes found by running: `snapshot()` returns an immutable list (copy before sort); `MetricValue`
variants expose `.v1()`, not `.value()`. Another case of running correcting reading (the RFC metric names).

---

## Final test scorecard (19 tests, all passing — laptop/MiniCluster only)

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
| `SlateDbGcE2E` | §7/§16.13 compaction orphans files; 900s compactor checkpoint pins them; GC retains | ✅ |
| `SlateDbGcLongE2E` | §16.13 orphans physically deleted after 900s expiry (~17-min run) | ✅ |
| `ReadYourWritesE2E` | §16.15 read-your-writes consistent before any flush (put/overwrite/delete/RMW/scan) | ✅ |
| `FlinkSerdeSlateDbE2E` | §16.16 Flink TypeSerializer ⇄ SlateDB byte[] (PojoSerializer round-trips objects + RMW) | ✅ |
| `SlateDbMergeSplitE2E` | §16.17 RFC-0004 projection split + union merge (600 keys intact, merged DB writable) | ✅ |
| `SlateDbFencingE2E` | §18.9 2nd writer fences 1st → Error.Closed{reason=FENCED} | ✅ |
| `SlateDbTtlE2E` | §18.6 native TTL: ⚡ lazy compaction-reclaim, NOT read-time expiry (corrected a wrong correction) | ✅ |
| `FlinkRescaleSavepointE2E` | ⭐ §16.18 REAL Flink savepoint→P2→P4→P1 rescale fused w/ SlateDB projection+union; exactly-once | ✅ |
| `SlateDbMetricsE2E` | §19 DefaultMetricsRecorder captures 43 metric names / 125 series; real catalog + Flink wiring | ✅ |
| `slatedb-jna-j11` | §17 Java-22 floor is removable — JNA binding, real ops + checkpoint on JDK 11/17/25 | ✅ |

## Bugs / corrections that only RUNNING surfaced (⚡)

1. **`flush()` ≠ manifest commit** → silent state loss; needs memtable flush (§16.2). *Most important.*
2. **Flink 1.20 & 2.3 run on JDK 25** → the Java-version conflict is surmountable (§16.3).
3. **`withProjectionRange` physically clips** — stronger than the design assumed (§16.9).
4. **`fromData` drains instantly** → checkpoint/timer tests need throttled replayable sources.
5. **Java records break Kryo on JDK 25** → use POJOs.
6. **`ObjectStore.resolve` needs empty path** (root) + db-name to builder (§16.7).
7. **`Settings.set` takes JSON values** — numbers bare, durations quoted strings (§16.11).
8. **The Java-22 floor is removable** — regenerating with UniFFI's Kotlin/JNA backend runs SlateDB on JDK
   11/17/25 (§17). Surfaced two fork costs by running: a uniffi-0.31 `message`-collision codegen bug (won't
   compile on any kotlinc) and mandatory binding↔native-lib version lockstep (`UnsatisfiedLinkError` otherwise).
9. **Post-compaction space reclaim lags ~15 min, not `min_age`** (§16.13). GC deletes orphaned SSTs only after a
   hardcoded **900s compactor checkpoint** expires (it pins the just-compacted generation). `min_age` is not the
   gate; the 900s is not tunable from the Java binding. Proven by a 17-min run: files 15→4 exactly at t+900s.
10. **Binding serialization is cheap; async round-trip latency dominates** (§16.14). Measured: byte marshalling
    is bulk memcpy (~4 µs/op from 16 B→16 KB), but a serial `await(get)` is ~44 µs (CPU 0.1% — blocked, not
    copying); pipelining hits ~97k/s. Corrected my source-only guess that copies were the main cost. Both are
    swamped by S3 latency (~100-200×). Also: default `await_durable=true` makes each write block on a WAL flush.

## Still genuinely unverified (need real object storage + load, not a MiniCluster)

- §8 L0 write-stall backpressure (never overwhelmed compaction).
- §9/§9A memory footprint / silent OOM / no-spill / disk contention. (Compaction GC / space-reclaim timing IS
  now verified — §16.13 — but memory OOM and disk-cache contention remain unmeasured.)
- §14 resharding (changing N / maxParallelism).
- ~~Real parallel savepoint→rescale-across-restart (algorithm + parallel operation tested separately, not fused).~~
  ✅ **NOW VERIFIED — §16.18 / `FlinkRescaleSavepointE2E`**: real MiniCluster savepoint→P2→P4→P1 rescale fused
  with SlateDB projection(upscale)+union(downscale), exactly-once (every key=9). Only real-S3/multi-node scale remains.
- All §11 operational risks: S3 tail latency (p99/p999), request cost, pre-1.0 on-disk format stability,
  failover fencing under real concurrency.

## The through-line

Every "yes" was a request to **verify against source rather than assert from memory** — and it repeatedly
paid off: the clone API shape, the checkpoint-on-Admin, the range-delete absence, the FFM/Java-22 floor, and
above all the `flush()`-≠-manifest **data-loss bug** were all things that memory/source-reading got wrong or
missed and only verification/running caught. The same pattern held to the end: "is the binding changeable to
Java 11/17?" was answered not by reasoning but by **regenerating and running it** — which both proved *yes*
(JDK 11/17/25) and surfaced the codegen bug + version-lockstep costs that reasoning alone would have missed.
The design is **correct across everything testable on a laptop**; its **production viability remains unproven**
and is where adoption should still be gated.
