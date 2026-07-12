package io.slatedb.flink.poc;

import uniffi.slatedb.Admin;
import uniffi.slatedb.Db;
import uniffi.slatedb.ObjectStore;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Proves the SlateDB "Java 22 floor" is a packaging choice, not a SlateDB limit.
 *
 * This is a plain Java program (javac 11) that drives REAL SlateDB operations —
 * open, 500x put, MEMTABLE flush, detached checkpoint, 500x read-back — through
 * the Mozilla Kotlin/JNA UniFFI binding (uniffi.slatedb.*), which is JNA-based and
 * runs on Java 8+.
 *
 * Contrast with the sibling `slatedb-verify` module, which uses the OFFICIAL
 * published binding (io.slatedb.uniffi.*). That one is FFM/Panama and REQUIRES
 * JDK 22+ (class version 66). This module reaches the same native library through
 * a different generated front-end and runs on JDK 11 / 17 / 25 — no
 * --enable-native-access, no --add-opens (those are FFM requirements; JNA needs
 * neither).
 *
 * See README §17 and the investigation journal (Phase 10) for how this was built
 * and the codegen bug that had to be patched.
 */
public final class SlateDbJnaJ11Main {
    public static void main(String[] args) throws Exception {
        System.out.println("java.version    = " + System.getProperty("java.version"));
        System.out.println("java.vm.version = " + System.getProperty("java.vm.version"));

        // Local filesystem object store: root store + absolute db path with the
        // leading '/' stripped (the pattern the FFM PoC established).
        Path dir = Files.createTempDirectory("slatedb-jna-j11-");
        String dbName = dir.toAbsolutePath().toString().substring(1);

        ObjectStore store = SlateBridge.openStore("file:///");
        Db db = SlateBridge.openDb(dbName, store);
        System.out.println("opened db at file:///" + dbName);

        final int n = 500;
        for (int i = 0; i < n; i++) {
            SlateBridge.put(db,
                ("k" + i).getBytes(StandardCharsets.UTF_8),
                ("v" + i).getBytes(StandardCharsets.UTF_8));
        }
        System.out.println("put " + n + " keys");

        SlateBridge.flushMemtable(db);
        System.out.println("memtable flush OK");

        // Async-heavy Admin checkpoint — the operation Flink needs for exactly-once.
        Admin admin = SlateBridge.openAdmin(dbName, store);
        String cpId = SlateBridge.checkpoint(admin);
        System.out.println("checkpoint created: id=" + cpId);
        admin.close();

        int ok = 0;
        for (int i = 0; i < n; i++) {
            byte[] got = SlateBridge.get(db, ("k" + i).getBytes(StandardCharsets.UTF_8));
            byte[] want = ("v" + i).getBytes(StandardCharsets.UTF_8);
            if (got != null && Arrays.equals(got, want)) ok++;
        }
        byte[] missing = SlateBridge.get(db, "does-not-exist".getBytes(StandardCharsets.UTF_8));

        SlateBridge.shutdown(db);
        store.close();

        System.out.println("read back correct: " + ok + "/" + n);
        System.out.println("absent key returns null: " + (missing == null));
        boolean cpOk = cpId != null && !cpId.isEmpty();
        System.out.println("checkpoint id non-empty: " + cpOk);

        if (ok == n && missing == null && cpOk) {
            System.out.println("RESULT: PASS — SlateDB (JNA binding) ran real ops + checkpoint on Java "
                + System.getProperty("java.version"));
        } else {
            System.out.println("RESULT: FAIL");
            System.exit(1);
        }
    }
}
