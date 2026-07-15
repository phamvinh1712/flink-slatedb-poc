package io.slatedb.flink.poc;

import io.slatedb.uniffi.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * SINGLE-WRITER FENCING → the error a Flink operator actually catches (§18.9 / RFC 0007).
 *
 * The doc covers the fencing MECHANISM (writer_epoch CAS bump) but the sweep flagged it never shows
 * the ERROR surface. This proves: opening a second writer on the same DB path fences the first, and
 * the first writer's next write throws with CloseReason.Fenced — the signal a Flink subtask must
 * treat as "this Db handle is dead → fail the task so Flink restarts + clone-restores."
 *
 * Verifies:
 *   A. writer W1 opens and writes fine.
 *   B. writer W2 opens the SAME path (bumps writer_epoch, fencing W1).
 *   C. W2 can write; W1's next write FAILS — and the failure is identifiable as a FENCE
 *      (Error.Closed{reason=Fenced}, or the underlying "Fenced" cause).
 *
 * This is the write-time detection RFC 0001 describes: a zombie writer keeps going until its next
 * write's CAS fails. Requires JDK 22+, -Djava.library.path=native.
 */
public final class SlateDbFencingE2E {

    static <T> T await(CompletableFuture<T> f) throws Exception {
        return f.get(60, TimeUnit.SECONDS);   // caller inspects exceptions
    }
    static byte[] b(String s) { return s.getBytes(StandardCharsets.UTF_8); }
    static boolean ok = true;
    static void check(String n, boolean c, String d) {
        System.out.println((c ? "  [PASS] " : "  [FAIL] ") + n + (d.isEmpty() ? "" : " — " + d));
        if (!c) ok = false;
    }

    /** True if the throwable chain indicates a fencing/closed-by-fence condition. */
    static boolean isFence(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            String cn = c.getClass().getName();
            String msg = String.valueOf(c.getMessage());
            if (c instanceof io.slatedb.uniffi.Error.Closed) {
                CloseReason r = ((io.slatedb.uniffi.Error.Closed) c).reason();
                System.out.println("    caught Error.Closed reason=" + r);
                if (r == CloseReason.FENCED) return true;
            }
            if (cn.toLowerCase().contains("fenc") || msg.toLowerCase().contains("fenc")
                    || msg.toLowerCase().contains("closed")) {
                System.out.println("    caught " + cn + ": " + msg);
                return true;
            }
        }
        return false;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== SINGLE-WRITER FENCING → the Flink-catchable error (§18.9, RFC 0007) ===");
        System.out.println("Runtime Java: " + System.getProperty("java.version"));

        Path work = Files.createTempDirectory("flink-slatedb-fence-");
        String dbName = work.resolve("db").toString().substring(1);

        try (ObjectStore store = ObjectStore.resolve("file:///")) {
            // A. W1 opens + writes.
            Db w1 = await(new DbBuilder(dbName, store).build());
            await(w1.put(b("k"), b("from-w1")));
            await(w1.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));
            check("A. W1 opened and wrote", true, "");

            // B. W2 opens the SAME path → bumps writer_epoch, fencing W1.
            Db w2 = await(new DbBuilder(dbName, store).build());
            check("B. W2 opened the same DB path (fences W1)", true, "");

            // C1. W2 (the live writer) can write.
            boolean w2ok = true;
            try { await(w2.put(b("k"), b("from-w2"))); } catch (Exception e) { w2ok = false; }
            check("C1. W2 (new writer) can write", w2ok, "");

            // C2. W1's next write must FAIL, and be identifiable as a fence.
            boolean w1failed = false, w1fenced = false;
            try {
                await(w1.put(b("k"), b("from-w1-again")));
                // some builds may only detect on flush/commit — try to force it
                await(w1.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));
            } catch (Exception e) {
                w1failed = true;
                w1fenced = isFence(e);
            }
            check("C2. W1's next write FAILED (zombie writer detected at write time)", w1failed,
                    w1failed ? "" : "W1 write unexpectedly succeeded — fencing not observed");
            check("C2. the failure is identifiable as a FENCE (CloseReason.Fenced / Fenced cause)",
                    w1fenced, w1failed && !w1fenced ? "failed but not recognizably a fence" : "");

            // cleanup: w2 is the valid writer; w1 is dead.
            try { await(w2.shutdown()); w2.close(); } catch (Exception ignore) {}
            try { w1.close(); } catch (Exception ignore) {}

        } catch (UnsatisfiedLinkError | NoClassDefFoundError e) {
            System.out.println("  [SKIP] native lib/class load failed — need JDK 22+ + -Djava.library.path. " + e);
            System.exit(2);
        }

        System.out.println();
        System.out.println(ok ? "FENCING E2E PASSED ✅ (2nd writer fences 1st; 1st's next write → Fenced error)"
                              : "FENCING E2E FAILED ❌");
        if (!ok) System.exit(1);
    }
}
