package io.slatedb.flink.poc;

import io.slatedb.uniffi.*;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Verifies the load-bearing SlateDB CORRECTNESS claim of the design doc against the REAL published
 * artifact (io.slatedb:slatedb-uniffi:0.14.1, pulled from Maven Central):
 *
 *   §12.8 / §6.6 — clone-from-a-pinned-checkpoint EXCLUDES writes made AFTER the checkpoint.
 *   This is THE property that makes exactly-once restore possible (a plain writer reopen would
 *   RESURRECT post-checkpoint WAL — see design §6.6). If this observation fails, SlateDB cannot be
 *   used as authoritative Flink state.
 *
 * ⚠️ RUNTIME REQUIREMENT: JDK 22+.
 *   The slatedb-uniffi JAR is compiled to Java 22 (class major version 66) and uses the
 *   java.lang.foreign / FFM API. It will throw UnsupportedClassVersionError on JDK 11/17.
 *   Run this class ONLY under a JDK 22+ runtime (see README). The Flink PoCs run on 11/17 and do
 *   NOT touch SlateDB, so this class is deliberately separate.
 *
 * API signatures below were extracted with javap from the 0.14.1 JAR — they are exact:
 *   ObjectStore.resolve(String)                                  (static)
 *   new DbBuilder(String path, ObjectStore)  ; build(): CompletableFuture<Db>
 *   new AdminBuilder(String, ObjectStore)    ; build(): Admin            (SYNC)
 *   Db.put(byte[],byte[]) / get(byte[]) / flush() / shutdown() / scan(KeyRange)
 *   Admin.createDetachedCheckpoint(CheckpointOptions): CompletableFuture<CheckpointCreateResult>
 *   new CheckpointOptions(Long lifetimeMs, String source, String name)
 *   CheckpointCreateResult.id(): String                          (NOT a UUID — §12.1 correction)
 *   new CloneSourceSpec(String path, String checkpoint, KeyRange projectionRange)  (plain ctor — §12.1 correction)
 *   Admin.createCloneBuilderFromSource(CloneSourceSpec): CloneBuilder  (SYNC)
 *   CloneBuilder.withClonePath(String) / withObjectStore(ObjectStore) / build(): CompletableFuture<Void>
 *   new KeyRange(byte[] start, boolean startIncl, byte[] end, boolean endIncl)
 *   DbIterator.next(): CompletableFuture<KeyValue>  (null = end) ; KeyValue.key(): byte[]
 */
public final class SlateDbCloneRestoreMain {

    private static <T> T await(CompletableFuture<T> f) {
        try { return f.get(60, TimeUnit.SECONDS); }
        catch (Exception e) { throw new RuntimeException(e); }
    }

    private static byte[] b(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    /** Full-range scan → true if key present. Exercises enumeration (not just point get). */
    private static boolean containsKey(Db db, byte[] key) throws Exception {
        KeyRange all = new KeyRange(null, false, null, false); // unbounded both ends
        DbIterator it = await(db.scan(all));
        try {
            for (KeyValue kv = await(it.next()); kv != null; kv = await(it.next())) {
                if (Arrays.equals(kv.key(), key)) return true;
            }
            return false;
        } finally {
            try { it.close(); } catch (Exception ignored) {}
        }
    }

    private static boolean ok = true;
    private static void check(String name, boolean cond, String detail) {
        System.out.println((cond ? "  [PASS] " : "  [FAIL] ") + name + (detail.isEmpty() ? "" : " — " + detail));
        if (!cond) ok = false;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== SlateDB clone-restore exactly-once verification (§12.8/§6.6) ===");
        System.out.println("Runtime Java: " + System.getProperty("java.version"));

        final String parentPath = "parent-db";
        final String clonePath  = "clone-db";
        final byte[] preKey  = b("k:before-checkpoint");
        final byte[] preVal  = b("v-committed");
        final byte[] postKey = b("k:AFTER-checkpoint");
        final byte[] postVal = b("v-should-not-survive");

        try (ObjectStore store = ObjectStore.resolve("memory:///")) {
            String cpId;

            // 1. Parent: pre-checkpoint write → checkpoint (the "Flink barrier") → post-checkpoint write.
            try (DbBuilder pb = new DbBuilder(parentPath, store)) {
                Db parent = await(pb.build());
                try (Admin admin = new AdminBuilder(parentPath, store).build()) {   // build() SYNC
                    try (parent) {
                        await(parent.put(preKey, preVal));

                        // ── §6.3 FINDING (verified against slatedb src db.rs:1735) ──────────────────────
                        // Db.flush() with WAL enabled uses FlushType::Wal — it flushes the WAL ONLY, NOT the
                        // memtable→L0. A checkpoint pins the MANIFEST, and the manifest only advances when the
                        // memtable is flushed to an L0 SST. So flush()+checkpoint does NOT make data
                        // checkpoint-visible (proven: a pinned reader never saw the key across 20 checkpoints/5s).
                        //
                        // THE FIX for a Flink checkpoint barrier (§6.3): force a MEMTABLE flush so the manifest
                        // captures the data, THEN take the detached checkpoint.
                        await(parent.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));   // memtable → L0
                        check("control: live parent sees pre-checkpoint write after memtable flush",
                                containsKey(parent, preKey), "");

                        CheckpointCreateResult cp =
                                await(admin.createDetachedCheckpoint(new CheckpointOptions(null, null, null)));
                        cpId = cp.id();   // String, not UUID
                        check("checkpoint created (id is a String)", cpId != null && !cpId.isEmpty(), "id=" + cpId);

                        // Checkpoint-pinned READER sees preKey → confirms the memtable-flush fix worked
                        // (the manifest the checkpoint pins now includes preKey).
                        try (DbReaderBuilder rb = new DbReaderBuilder(parentPath, store)) {
                            rb.withCheckpointId(cpId);
                            DbReader rdr = await(rb.build());
                            byte[] v = await(rdr.get(preKey));
                            check("checkpoint-pinned READER sees pre-checkpoint write (fix confirmed)",
                                    v != null && Arrays.equals(preVal, v), "");
                            await(rdr.shutdown());
                        }

                        // POST-checkpoint durable writes (memtable-flushed) — MUST NOT appear in a clone at cpId.
                        await(parent.put(postKey, postVal));
                        await(parent.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));
                        check("control: live parent sees its own post-checkpoint write",
                                containsKey(parent, postKey), "");

                        // 2. Clone from the pinned checkpoint (the §6.6 restore mechanism).
                        CloneSourceSpec src = new CloneSourceSpec(parentPath, cpId, null); // plain ctor
                        CloneBuilder cb = admin.createCloneBuilderFromSource(src);          // SYNC
                        cb.withClonePath(clonePath);        // void setters — not chainable
                        cb.withObjectStore(store);
                        await(cb.build());
                        await(parent.shutdown());
                    }
                }
            }

            // 3. Open the clone; assert point-in-time correctness.
            try (DbBuilder cbb = new DbBuilder(clonePath, store)) {
                Db clone = await(cbb.build());
                try (clone) {
                    check("§12.8: clone EXCLUDES post-checkpoint write (exactly-once holds)",
                            !containsKey(clone, postKey),
                            "if FAIL → SlateDB cannot be authoritative Flink state");
                    check("clone RETAINS pre-checkpoint committed data",
                            containsKey(clone, preKey), "");
                    check("point-read of pre-checkpoint key matches",
                            Arrays.equals(preVal, await(clone.get(preKey))), "");
                    check("point-read of post-checkpoint key is absent in clone",
                            await(clone.get(postKey)) == null, "");
                    await(clone.shutdown());
                }
            }
        } catch (UnsatisfiedLinkError | NoClassDefFoundError e) {
            System.out.println("  [SKIP] native lib / class load failed — are you on JDK 22+? " + e);
            System.out.println("  (slatedb-uniffi is a Java-22 FFM artifact; run under JDK 22+. See README.)");
            System.exit(2);
        }

        System.out.println();
        System.out.println(ok ? "SLATEDB CHECKS PASSED ✅" : "SLATEDB CHECKS FAILED ❌");
        if (!ok) System.exit(1);
    }
}
