# Initial Infrastructure: Java Bindings for DataFusion

**Date:** 2026-05-10
**Status:** Approved

## Goal

Stand up the smallest possible end-to-end skeleton for Java bindings to Apache
DataFusion: a Maven project, a Rust crate that depends on the latest DataFusion
release, and a Java `SessionContext` class whose `sql(String)` method calls
through to `datafusion::SessionContext::sql` over JNI.

Returning data is out of scope. The success criterion is that a JUnit test can
construct a `SessionContext`, call `sql("SELECT 1")` without an exception, and
close the context.

## Non-goals

- Returning result data (Arrow record batches, row iteration, etc.).
- Arrow C Data Interface integration.
- Bundling the native library inside the JAR. Tests load it from
  `native/target/debug` via `java.library.path`.
- Multi-module Maven layout. One module is enough now.
- Spark-version shims, plan serialization, expression serdes — none of that
  applies here; this project is independent of Comet.

## Coordinates

- **groupId:** `org.apache.datafusion`
- **artifactId:** `datafusion-java-bindings`
- **Java package:** `org.apache.datafusion`
- **JDK:** 17
- **Rust crate name:** `datafusion-jni` (produces `libdatafusion_jni.{so,dylib,dll}`)

## Repository layout

```
datafusion-java-bindings/
├── README.md
├── Makefile
├── pom.xml
├── mvnw, mvnw.cmd, .mvn/          # Maven wrapper
├── src/
│   ├── main/java/org/apache/datafusion/
│   │   ├── NativeLibraryLoader.java
│   │   └── SessionContext.java
│   └── test/java/org/apache/datafusion/
│       └── SessionContextTest.java
└── native/
    ├── Cargo.toml
    └── src/
        ├── lib.rs
        └── errors.rs
```

## Rust crate (`native/`)

### Dependencies

- `datafusion` — latest release at implementation time (e.g. 50.x).
- `jni = "0.21"` — same major version Comet uses, but the upstream crate, not
  Comet's fork. Use the standard `JNIEnv` API (no `EnvUnowned`).
- `tokio = { version = "1", features = ["rt-multi-thread"] }` — needed because
  `SessionContext::sql` and `DataFrame::collect` are async.

`crate-type = ["cdylib"]`.

### `lib.rs` — JNI entry points

Three `#[no_mangle] pub extern "system"` functions, named to match the JNI
mangling rules for `org.apache.datafusion.SessionContext`:

1. `Java_org_apache_datafusion_SessionContext_createSessionContext(env, class) -> jlong`
   - `Box::into_raw(Box::new(datafusion::prelude::SessionContext::new())) as jlong`.
2. `Java_org_apache_datafusion_SessionContext_executeSql(env, class, handle: jlong, sql: JString)`
   - Cast handle back to `&SessionContext` (do NOT take ownership).
   - Convert `JString` to a Rust `String`.
   - On a process-wide Tokio runtime: `ctx.sql(&sql).await?.collect().await?`.
   - Drop the result. Throw a Java exception on error.
3. `Java_org_apache_datafusion_SessionContext_closeSessionContext(env, class, handle: jlong)`
   - `drop(Box::from_raw(handle as *mut SessionContext))`. Idempotent caller-side
     guard lives on the Java side.

A single `tokio::runtime::Runtime` is created lazily and held in a `OnceLock`.
The JNI methods use `runtime.block_on(...)` to drive the async calls.

### `errors.rs`

A `try_unwrap_or_throw` helper modeled on Comet's. The shape is the same —
take a closure returning `Result<T, E>`, catch panics with
`std::panic::catch_unwind`, and on any `Err` or panic throw a Java exception
and return a default value. Implementation uses the upstream `jni` crate's
`JNIEnv::throw_new`, not Comet's fork-specific helpers, so the code is adapted
rather than copied verbatim. Throws `java.lang.RuntimeException` for now; a
dedicated `DataFusionException` is deferred.

This pattern is the only piece informed by Comet. The new project has no
dependency on Comet, source or binary.

## Java side (`org.apache.datafusion`)

### `NativeLibraryLoader`

```java
public final class NativeLibraryLoader {
    private static volatile boolean loaded = false;

    public static synchronized void loadLibrary() {
        if (!loaded) {
            System.loadLibrary("datafusion_jni");
            loaded = true;
        }
    }
}
```

Kept separate from `SessionContext` so we can later swap in a
JAR-resource-extraction loader (Comet-style) without touching the user-facing
class.

### `SessionContext`

```java
public final class SessionContext implements AutoCloseable {
    static { NativeLibraryLoader.loadLibrary(); }

    private long nativeHandle;

    public SessionContext() {
        this.nativeHandle = createSessionContext();
    }

    public void sql(String query) {
        executeSql(nativeHandle, query);
    }

    @Override
    public void close() {
        if (nativeHandle != 0) {
            closeSessionContext(nativeHandle);
            nativeHandle = 0;
        }
    }

    private static native long createSessionContext();
    private static native void executeSql(long handle, String sql);
    private static native void closeSessionContext(long handle);
}
```

`sql` returns `void` for now. It throws a `RuntimeException` on planning or
execution failure.

## Build orchestration

### `pom.xml`

- `maven.compiler.source = 17`, `maven.compiler.target = 17`.
- JUnit Jupiter 5 (`junit-jupiter` 5.x) on the test classpath.
- `maven-surefire-plugin` configured with
  `argLine = -Djava.library.path=${project.basedir}/native/target/debug`.
- No `exec-maven-plugin` for cargo. `make` orchestrates the two builds.

### `Makefile`

```
.PHONY: all native jvm test clean

all: native jvm

native:
	cd native && cargo build

jvm:
	./mvnw package -DskipTests

test: native
	./mvnw test

clean:
	cd native && cargo clean
	./mvnw clean
```

### Maven wrapper

Add `mvnw`, `mvnw.cmd`, and `.mvn/wrapper/` so contributors don't need a
system Maven install. Use whatever the current Maven 3.9.x wrapper bundles.

## Test

`SessionContextTest.java`:

```java
class SessionContextTest {
    @Test
    void canExecuteSelect1() {
        try (SessionContext ctx = new SessionContext()) {
            ctx.sql("SELECT 1");
        }
    }
}
```

**Success criterion:** `make test` exits 0. This proves the whole stack: the
native library loads, the JNI bridge works, DataFusion plans and executes the
query, and the native handle is freed.

## Future work (explicitly deferred)

- Returning record batches via Arrow C Data Interface.
- A `DataFusionException` type plus structured error mapping.
- JAR-bundled native library with runtime extraction (Comet's `NativeBase`
  pattern).
- Additional `SessionContext` methods: `registerCsv`, `registerParquet`,
  `registerTable`, etc.
- Cross-platform CI builds.
