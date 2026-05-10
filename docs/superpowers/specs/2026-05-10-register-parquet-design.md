# Register Parquet Files

**Date:** 2026-05-10
**Status:** Approved

## Goal

Expose `SessionContext.registerParquet(String name, String path)` on the Java
side so users can register a Parquet file as a SQL table, then query it via the
existing `sql(String)` method.

Success criterion: a JUnit test writes a small Parquet file to a temp dir,
registers it under a name, runs a `SELECT * FROM <name>` against it, and the
test passes without exceptions.

## Non-goals

- Returning data from `sql()`. That remains deferred.
- Exposing `ParquetReadOptions`. Always uses `ParquetReadOptions::default()`
  on the Rust side.
- A `registerCsv` / `registerJson` companion. Out of scope; this spec is
  Parquet-only.
- A way to unregister a table or enumerate registered tables.
- Cloud storage URLs (`s3://`, `hdfs://`, etc.). DataFusion supports these
  but they require additional object-store wiring; deferred.
- Schema inspection. The user gets back nothing; failures bubble as
  `RuntimeException`.

## Java API

One additional public method on `SessionContext`:

```java
public void registerParquet(String name, String path) {
    if (nativeHandle == 0) {
        throw new IllegalStateException("SessionContext is closed");
    }
    registerParquet(nativeHandle, name, path);
}

private static native void registerParquet(long handle, String name, String path);
```

Same use-after-close guard as `sql()`. `void` return; failures (file not
found, malformed file, unsupported schema, etc.) propagate as
`RuntimeException` via the existing `try_unwrap_or_throw` helper.

## Rust JNI

New entry point in `native/src/lib.rs`:

```rust
#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_registerParquet<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    name: JString<'local>,
    path: JString<'local>,
) {
    try_unwrap_or_throw(&mut env, (), |env| -> JniResult<()> {
        if handle == 0 {
            return Err("SessionContext handle is null".into());
        }
        let ctx = unsafe { &*(handle as *const SessionContext) };
        let name: String = env.get_string(&name)?.into();
        let path: String = env.get_string(&path)?.into();
        runtime().block_on(async {
            ctx.register_parquet(&name, &path, ParquetReadOptions::default()).await?;
            Ok::<(), DataFusionError>(())
        })?;
        Ok(())
    })
}
```

Imports added:

```rust
use datafusion::prelude::ParquetReadOptions;
```

Same Tokio runtime singleton, same null-handle guard, same error funnel as
`executeSql`.

## Test

A new test class member in `SessionContextTest`:

```java
@Test
void registerAndQueryParquet(@TempDir Path tmp) throws Exception {
    Path file = tmp.resolve("people.parquet");
    TestParquet.writeTinyParquet(file);

    try (SessionContext ctx = new SessionContext()) {
        ctx.registerParquet("people", file.toString());
        ctx.sql("SELECT * FROM people");
    }
}
```

A new helper `src/test/java/org/apache/datafusion/TestParquet.java` writes a
3-row Parquet file with two columns:

| column | parquet type        |
| ------ | ------------------- |
| `id`   | `INT32`             |
| `name` | `BINARY (UTF8)`     |

Implementation uses `parquet-hadoop` 1.14.x with `GroupWriteSupport` and
`ExampleParquetWriter`. ~30 lines.

## Build changes

`pom.xml` gains one test-scope dependency:

```xml
<dependency>
    <groupId>org.apache.parquet</groupId>
    <artifactId>parquet-hadoop</artifactId>
    <version>1.14.4</version>
    <scope>test</scope>
</dependency>
```

This pulls in `parquet-column`, `parquet-common`, and a Hadoop core
dependency tree at test scope only — no impact on the runtime classpath.

## Files touched

| Path | Change |
| --- | --- |
| `src/main/java/org/apache/datafusion/SessionContext.java` | Add `registerParquet(String, String)` + native declaration |
| `native/src/lib.rs` | Add `Java_org_apache_datafusion_SessionContext_registerParquet` |
| `src/test/java/org/apache/datafusion/TestParquet.java` | New helper to write a tiny Parquet file |
| `src/test/java/org/apache/datafusion/SessionContextTest.java` | Add `registerAndQueryParquet` test |
| `pom.xml` | Add `parquet-hadoop` test dependency |

## Definition of done

- `make test` exits 0.
- Both tests pass: `canExecuteSelect1` and `registerAndQueryParquet`.
- ASF license headers on every new file.
