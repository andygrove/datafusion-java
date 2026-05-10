# Fetch Query Result Batches

**Date:** 2026-05-10
**Status:** Approved

## Goal

Replace the void `SessionContext.sql(String)` with a method that returns query
results as Arrow batches, so a Java caller can run `SELECT COUNT(*) FROM
lineitem` and read the count out of the resulting `ArrowReader`.

Success criterion: a JUnit test queries TPC-H SF1 lineitem with `SELECT
COUNT(*)`, receives one `RecordBatch` containing one `BigIntVector` with the
known row count `6_001_215`, and asserts on it.

## Non-goals

- True async streaming. The Rust side eager-collects the whole result into a
  `Vec<RecordBatch>` before exposing it as an `ArrowArrayStream`. Per-batch
  async-driven streaming is a follow-up.
- A Java `DataFrame` class. The single `sql(query, allocator) -> ArrowReader`
  call plans, executes, and returns the reader in one shot. Lazy chaining
  (`filter`/`limit`/`collect`) is deferred.
- Schema-only access without execution.
- Alternative `BufferAllocator` backends. Tests use `RootAllocator` from
  `arrow-memory-netty`; consumers can swap.
- Returning anything other than an `ArrowReader` (e.g., `List<RecordBatch>`,
  per-row iterator).

## Transport

Apache Arrow C Stream Interface (`ArrowArrayStream`). Java allocates the C
struct via `ArrowArrayStream.allocateNew(allocator)` and passes its memory
address to native code. Rust constructs an `FFI_ArrowArrayStream` and writes
it into that address via `std::ptr::write`. Java imports the now-populated
struct via `Data.importArrayStream(allocator, stream)` and gets back an
`ArrowReader`.

The `ArrowReader` owns the lifetime of the imported buffers from that point
on; closing the reader (try-with-resources) releases all native memory.

## Java API

### Public method on `SessionContext`

Replaces the existing void `sql(String)`:

```java
public ArrowReader sql(String query, BufferAllocator allocator) {
    if (nativeHandle == 0) {
        throw new IllegalStateException("SessionContext is closed");
    }
    ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
    try {
        executeQuery(nativeHandle, query, stream.memoryAddress());
        return Data.importArrayStream(allocator, stream);
    } catch (RuntimeException e) {
        stream.close();
        throw e;
    }
}
```

If `executeQuery` throws, we close the just-allocated `ArrowArrayStream` to
avoid leaking the 80-byte struct allocation. On success, ownership of that
allocation transfers to the imported `ArrowReader`.

### Private native method

```java
private static native void executeQuery(long handle, String sql, long ffiStreamAddr);
```

Replaces the existing `executeSql(long, String)`. The native side writes the
stream into the address; nothing is returned.

### Class-level Javadoc

The existing thread-safety warning already names `sql` as a hazardous
concurrent-with-`close` method. No change needed.

## Java dependencies

Three new dependencies in `pom.xml` (arrow-java 19.0.1, current at writing):

```xml
<dependency>
    <groupId>org.apache.arrow</groupId>
    <artifactId>arrow-vector</artifactId>
    <version>19.0.1</version>
</dependency>
<dependency>
    <groupId>org.apache.arrow</groupId>
    <artifactId>arrow-c-data</artifactId>
    <version>19.0.1</version>
</dependency>
<dependency>
    <groupId>org.apache.arrow</groupId>
    <artifactId>arrow-memory-netty</artifactId>
    <version>19.0.1</version>
    <scope>runtime</scope>
</dependency>
```

`arrow-vector` and `arrow-c-data` are compile scope because they appear in
the public method signature and import statements. `arrow-memory-netty` is
the default backend at runtime; pinning it as `runtime` lets consumers swap
in `arrow-memory-unsafe` if they prefer.

Maven will likely require Surefire's argLine to add
`--add-opens=java.base/java.nio=ALL-UNNAMED` for arrow-memory-netty's
DirectByteBuffer access on JDK 17. Update Surefire config:

```xml
<argLine>-Djava.library.path=${project.basedir}/native/target/debug --add-opens=java.base/java.nio=ALL-UNNAMED</argLine>
```

## Rust crate changes

### New `Cargo.toml` dependency

Add `arrow` matching DataFusion's transitive version. DataFusion 53 currently
resolves `arrow = 58.2.0`. Pin loosely:

```toml
arrow = "58"
```

### New imports in `lib.rs`

```rust
use std::sync::Arc;

use arrow::array::ffi_stream::FFI_ArrowArrayStream;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatchIterator;
```

### New JNI entry point

Replaces `Java_org_apache_datafusion_SessionContext_executeSql`:

```rust
#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_executeQuery<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    sql: JString<'local>,
    ffi_stream_addr: jlong,
) {
    try_unwrap_or_throw(&mut env, (), |env| -> JniResult<()> {
        if handle == 0 {
            return Err("SessionContext handle is null".into());
        }
        if ffi_stream_addr == 0 {
            return Err("ffi stream address is null".into());
        }

        let ctx = unsafe { &*(handle as *const SessionContext) };
        let sql_str: String = env.get_string(&sql)?.into();

        let ffi: FFI_ArrowArrayStream = runtime().block_on(async {
            let df = ctx.sql(&sql_str).await?;
            let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
            let batches = df.collect().await?;
            let iter = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
            Ok::<_, DataFusionError>(FFI_ArrowArrayStream::new(Box::new(iter)))
        })?;

        unsafe {
            std::ptr::write(ffi_stream_addr as *mut FFI_ArrowArrayStream, ffi);
        }
        Ok(())
    })
}
```

The schema is captured **before** `df.collect()` consumes the DataFrame, so
empty result sets still get a correct schema.

`std::ptr::write` is the right primitive: it `memcpy`s the new value into the
destination without dropping the old contents (which is correct because
`ArrowArrayStream.allocateNew` zeros the struct).

The `FFI_ArrowArrayStream` retains ownership of the boxed
`RecordBatchIterator` until the consumer (Java) calls the C `release`
callback; that's wired through arrow-rs's standard implementation.

## Tests

### `canExecuteSelect1`

Update to consume the reader:

```java
@Test
void canExecuteSelect1() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
         SessionContext ctx = new SessionContext();
         ArrowReader reader = ctx.sql("SELECT 1", allocator)) {
        assertTrue(reader.loadNextBatch());
        assertEquals(1, reader.getVectorSchemaRoot().getRowCount());
    }
}
```

### `selectCountStarLineitem`

Replaces `registerAndQueryLineitem` with a real assertion:

```java
@Test
void selectCountStarLineitem() throws Exception {
    Path lineitem = Path.of("tpch-data/sf1/lineitem.parquet");
    Assumptions.assumeTrue(Files.exists(lineitem),
            "TPC-H SF1 data not found; run `make tpch-data` first");

    try (BufferAllocator allocator = new RootAllocator();
         SessionContext ctx = new SessionContext()) {
        ctx.registerParquet("lineitem", lineitem.toAbsolutePath().toString());
        try (ArrowReader reader = ctx.sql("SELECT COUNT(*) FROM lineitem", allocator)) {
            assertTrue(reader.loadNextBatch());
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertEquals(1, root.getRowCount());
            BigIntVector count = (BigIntVector) root.getVector(0);
            assertEquals(6_001_215L, count.get(0));
        }
    }
}
```

`6_001_215` is the known lineitem cardinality at SF1.

New imports in the test:
- `org.apache.arrow.memory.BufferAllocator`
- `org.apache.arrow.memory.RootAllocator`
- `org.apache.arrow.vector.BigIntVector`
- `org.apache.arrow.vector.VectorSchemaRoot`
- `org.apache.arrow.vector.ipc.ArrowReader`
- `static org.junit.jupiter.api.Assertions.assertEquals`
- `static org.junit.jupiter.api.Assertions.assertTrue`

## Memory contract / ownership

| Resource | Owner | Released by |
| --- | --- | --- |
| `ArrowArrayStream` (80-byte struct) | Java `BufferAllocator` | `ArrowReader.close()` (transferred via `Data.importArrayStream`) |
| `RecordBatch`es | Rust (initially) | Each batch's release callback, invoked when Java drops the imported `VectorSchemaRoot` |
| `Box<RecordBatchIterator>` | `FFI_ArrowArrayStream` | Stream-level release callback when reader is closed |
| `BufferAllocator` | Test (`try-with-resources`) | Allocator close — verifies no leaks |

Closing in this order in the test (innermost-first) ensures clean shutdown:
`ArrowReader` → `SessionContext` → `BufferAllocator`.

## Files touched

| Path | Change |
| --- | --- |
| `pom.xml` | Add 3 arrow-java deps; extend Surefire `argLine` for `--add-opens` |
| `src/main/java/org/apache/datafusion/SessionContext.java` | Replace void `sql(String)` with `sql(String, BufferAllocator) -> ArrowReader`; replace native method |
| `src/test/java/org/apache/datafusion/SessionContextTest.java` | Update both tests; add real assertion on lineitem count |
| `native/Cargo.toml` | Add `arrow = "58"` dep |
| `native/src/lib.rs` | Replace `executeSql` JNI with `executeQuery`; new imports |

## Definition of done

- `make test` exits 0 with both tests passing (or 1 passing + 1 skipped if SF1
  data is absent).
- `selectCountStarLineitem` (when run) asserts the lineitem count is
  6,001,215.
- No memory leaks reported by the `RootAllocator` close in either test.
- All ASF license headers preserved.
