package io.slatedb.flink.poc

import kotlinx.coroutines.runBlocking
import uniffi.slatedb.*

/**
 * Thin Kotlin adapter that turns SlateDB's suspend fns into blocking calls
 * callable from plain Java.
 *
 * This layer is the "adapter cost" of the JNA path: the official FFM binding
 * (io.slatedb.uniffi.*, Java 22) exposes CompletableFuture directly, so Java can
 * call it with no bridge. The Mozilla Kotlin/JNA binding (uniffi.slatedb.*, Java 8+)
 * exposes `suspend fun`s that take a kotlin.coroutines.Continuation — not callable
 * ergonomically from Java. So a Kotlin bridge like this is mandatory when going the
 * JNA route.
 *
 * Every call here drives the UniFFI async future-continuation path
 * (ffi_slatedb_uniffi_rust_future_poll_* + a JNA upcall Callback that resumes the
 * coroutine) — i.e. the exact mechanism the FFM binding implements with upcallStub.
 */
object SlateBridge {
    @JvmStatic fun openStore(url: String): ObjectStore = ObjectStore.resolve(url)

    @JvmStatic fun openDb(name: String, store: ObjectStore): Db = runBlocking {
        DbBuilder(name, store).build()
    }

    @JvmStatic fun put(db: Db, key: ByteArray, value: ByteArray) = runBlocking {
        db.put(key, value); Unit
    }

    @JvmStatic fun get(db: Db, key: ByteArray): ByteArray? = runBlocking {
        db.get(key)
    }

    @JvmStatic fun flushMemtable(db: Db) = runBlocking {
        // The ⚡ bug the FFM PoC found: plain flush() is WAL-only; a checkpoint pins the
        // manifest, which needs a MEMTABLE flush. Same fix applies on the JNA path.
        db.flushWithOptions(FlushOptions(FlushType.MEM_TABLE)); Unit
    }

    @JvmStatic fun shutdown(db: Db) = runBlocking { db.shutdown(); Unit }

    // --- Admin / checkpoint path: the async-heavy operation Flink needs for exactly-once ---
    @JvmStatic fun openAdmin(name: String, store: ObjectStore): Admin =
        AdminBuilder(name, store).build()

    /** Create a detached checkpoint (no lifetime/source/name). Returns the checkpoint id. */
    @JvmStatic fun checkpoint(admin: Admin): String = runBlocking {
        admin.createDetachedCheckpoint(CheckpointOptions(null, null, null)).id
    }
}
