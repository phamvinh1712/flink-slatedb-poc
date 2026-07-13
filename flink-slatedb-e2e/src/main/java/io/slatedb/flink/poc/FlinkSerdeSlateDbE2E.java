package io.slatedb.flink.poc;

import io.slatedb.uniffi.*;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Shows how to store JAVA OBJECTS in SlateDB the SAME WAY Flink's RocksDB backend does:
 * use Flink's own {@link TypeSerializer} to turn objects into bytes, then hand the bytes to
 * SlateDB's byte[] get/put API.
 *
 * SlateDB's binding is byte[] in / byte[] out (§5B/§16.14) — it stores no type info, exactly like
 * RocksDB. So the serialization layer is Flink's, unchanged: TypeInformation.createSerializer(...)
 * picks the POJO serializer (or Kryo/Avro/custom), and DataOutputSerializer/DataInputDeserializer
 * do the encode/decode — the identical primitives RocksDBValueState uses internally.
 *
 * Verifies: a POJO written via Flink's serializer into SlateDB round-trips back byte-for-byte and
 * field-for-field equal, across many keys, including RMW (read object → mutate → write object).
 * Requires JDK 22+, -Djava.library.path=native.
 */
public final class FlinkSerdeSlateDbE2E {

    /** Mutable POJO with a no-arg ctor → Flink's POJO serializer handles it (NOT a record: Kryo
     *  cannot serialize records on JDK 16+, see FlinkHybridTieringE2E). */
    public static final class Account {
        public long balance;
        public String owner;
        public List<String> tags;
        public Account() {}                       // required by POJO serializer
        Account(long b, String o, List<String> t) { balance = b; owner = o; tags = t; }
        boolean sameAs(Account a) {
            return a != null && balance == a.balance && Objects.equals(owner, a.owner)
                    && Objects.equals(tags, a.tags);
        }
        public String toString() { return "Account{" + balance + "," + owner + "," + tags + "}"; }
    }

    /** The reusable codec — this is the whole trick. One per operator (single-threaded per subtask). */
    static final class SlateCodec<T> {
        private final TypeSerializer<T> ser;
        private final DataOutputSerializer out = new DataOutputSerializer(64);
        private final DataInputDeserializer in = new DataInputDeserializer();
        SlateCodec(TypeInformation<T> info, ExecutionConfig cfg) { this.ser = info.createSerializer(cfg); }
        byte[] toBytes(T v) throws IOException { out.clear(); ser.serialize(v, out); return out.getCopyOfBuffer(); }
        T fromBytes(byte[] b) throws IOException { in.setBuffer(b, 0, b.length); return ser.deserialize(in); }
    }

    static <T> T await(CompletableFuture<T> f) {
        try { return f.get(60, TimeUnit.SECONDS); } catch (Exception e) { throw new RuntimeException(e); }
    }
    static boolean ok = true;
    static void check(String n, boolean c, String d) {
        System.out.println((c ? "  [PASS] " : "  [FAIL] ") + n + (d.isEmpty() ? "" : " — " + d));
        if (!c) ok = false;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== Flink TypeSerializer → SlateDB byte[] (store Java objects RocksDB-style) ===");
        System.out.println("Runtime Java: " + System.getProperty("java.version"));

        ExecutionConfig cfg = new ExecutionConfig();
        // Exactly how RocksDBValueState<Account> would obtain its serializer:
        TypeInformation<Account> info = TypeInformation.of(Account.class);
        SlateCodec<Account> codec = new SlateCodec<>(info, cfg);
        System.out.println("  serializer chosen by Flink: " + info.createSerializer(cfg).getClass().getSimpleName());

        Path work = Files.createTempDirectory("flink-serde-slate-");
        String dbName = work.resolve("db").toString().substring(1);

        try (ObjectStore store = ObjectStore.resolve("file:///")) {
            Db db = await(new DbBuilder(dbName, store).build());

            // 1. Single object round-trip.
            // NOTE: use ArrayList, not List.of(...) — Flink routes List<String> through Kryo, whose
            // CollectionSerializer deserializes into a MUTABLE list; an immutable List.of() round-trips
            // fine on read but breaks on later .add(). Real Flink state uses mutable collections anyway.
            Account a = new Account(100L, "alice", new ArrayList<>(List.of("vip", "eu")));
            await(db.put("acct:alice".getBytes(), codec.toBytes(a)));
            Account back = codec.fromBytes(await(db.get("acct:alice".getBytes())));
            check("single POJO round-trips equal", a.sameAs(back), a + " vs " + back);

            // 2. Many objects, all round-trip.
            int N = 1000;
            Map<String, Account> truth = new HashMap<>();
            for (int i = 0; i < N; i++) {
                Account acc = new Account(i * 7L, "user-" + i,
                        new ArrayList<>(List.of("t" + (i % 3), "t" + (i % 5))));
                truth.put("acct:" + i, acc);
                await(db.put(("acct:" + i).getBytes(), codec.toBytes(acc)));
            }
            await(db.flushWithOptions(new FlushOptions(FlushType.MEM_TABLE)));
            int wrong = 0;
            for (var e : truth.entrySet()) {
                Account got = codec.fromBytes(await(db.get(e.getKey().getBytes())));
                if (!e.getValue().sameAs(got)) wrong++;
            }
            check("1000 POJOs round-trip field-for-field after flush", wrong == 0, "wrong=" + wrong);

            // 3. RMW on an object: read → mutate → write (the keyed-state pattern).
            for (int i = 0; i < 50; i++) {
                Account cur = codec.fromBytes(await(db.get("acct:alice".getBytes())));
                cur.balance += 10;
                cur.tags = new ArrayList<>(cur.tags); cur.tags.add("bump" + i);
                await(db.put("acct:alice".getBytes(), codec.toBytes(cur)));
            }
            Account fin = codec.fromBytes(await(db.get("acct:alice".getBytes())));
            check("object RMW x50: balance 100→600", fin.balance == 600L, "balance=" + fin.balance);
            check("object RMW: list field grew to 52 tags", fin.tags.size() == 2 + 50,
                    "tags=" + fin.tags.size());

            await(db.shutdown()); db.close();
        } catch (UnsatisfiedLinkError | NoClassDefFoundError e) {
            System.out.println("  [SKIP] native lib/class load failed — need JDK 22+ + -Djava.library.path. " + e);
            System.exit(2);
        }

        System.out.println();
        System.out.println(ok ? "FLINK-SERDE SLATEDB PASSED ✅ (Flink TypeSerializer ⇄ SlateDB byte[], objects intact)"
                              : "FLINK-SERDE SLATEDB FAILED ❌");
        if (!ok) System.exit(1);
    }
}
