# Fetch Query Batches Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `SessionContext.sql(String)` (void) with `sql(String, BufferAllocator) -> ArrowReader` so a Java caller can read query results. The lineitem test asserts `SELECT COUNT(*) FROM lineitem` returns `6_001_215`.

**Architecture:** Apache Arrow C Stream Interface bridges Rust → Java. Java allocates an `ArrowArrayStream` struct (80 bytes), passes its address to Rust, Rust populates it via `std::ptr::write` of an `FFI_ArrowArrayStream`, Java imports it into an `ArrowReader`. Rust eager-collects all batches inside one `block_on`; per-batch async streaming is a follow-up.

**Tech Stack:** Java 17, JUnit 5, arrow-java 19.0.1 (`arrow-vector`, `arrow-c-data`, `arrow-memory-netty`), Rust + DataFusion 53 (which already pulls arrow-rs 58.2 transitively — we use the `datafusion::arrow::*` re-exports).

**Spec:** `docs/superpowers/specs/2026-05-10-fetch-query-batches-design.md`.

---

## Decomposition note

Two tasks. The Java and Rust changes are coupled (the new public Java method has no native symbol until Rust ships it), so we can't ship them independently. Task 1 sets up everything Java-side; Task 2 brings up the JNI symbol so the test passes. Adding `arrow` as a direct Rust crate is **not** needed: DataFusion re-exports it as `datafusion::arrow`, which avoids version-skew between our explicit pin and DataFusion's transitive resolution.

## File Structure

| Path | Change |
| --- | --- |
| `pom.xml` | Add three arrow-java compile/runtime deps; extend Surefire `argLine` for `--add-opens` |
| `src/main/java/org/apache/datafusion/SessionContext.java` | Replace void `sql(String)` with `sql(String, BufferAllocator) -> ArrowReader`; replace native method `executeSql` with `executeQuery` |
| `src/test/java/org/apache/datafusion/SessionContextTest.java` | Update `canExecuteSelect1` to consume the reader; replace `registerAndQueryLineitem` with `selectCountStarLineitem` asserting count = 6,001,215 |
| `native/src/lib.rs` | Replace `Java_..._executeSql` with `Java_..._executeQuery`; new imports from `datafusion::arrow` |

`native/Cargo.toml` is **not** modified — we use `datafusion::arrow::...` re-exports.

---

## Prerequisites

- Earlier work merged (parquet registration + TPC-H test fixture).
- `./mvnw test` currently passes 2/2 with TPC-H SF1 data, or skips 1 if absent.
- TPC-H SF1 data is recommended for executing this plan (so Task 2 verification asserts the count). If SF1 isn't present, the lineitem test will skip; you can still verify `canExecuteSelect1` runs end-to-end.

```bash
cd /Users/andy/git/apache/datafusion-java-bindings
./mvnw test               # confirm baseline 2/2 (or 1 + 1 skipped)
ls -lh tpch-data/sf1/lineitem.parquet  # exists, ~221MB
```

Project conventions:
- ASF license headers preserved on all source files.
- Conventional Commits in imperative mood. NO `Co-Authored-By:` trailers.

---

## Task 1: Java + Maven changes (test compiles, fails at runtime)

Goal: add arrow-java deps, rewrite `SessionContext.sql` to return an `ArrowReader`, update both tests. The suite compiles but fails at runtime with `UnsatisfiedLinkError` on `executeQuery` — Task 2 fixes it.

**Files:**
- Modify: `pom.xml`
- Modify: `src/main/java/org/apache/datafusion/SessionContext.java`
- Modify: `src/test/java/org/apache/datafusion/SessionContextTest.java`

- [ ] **Step 1: Add arrow-java deps to `pom.xml`**

Inside the existing `<dependencies>` block (the `junit-jupiter` dep is currently the only one), add three more so the block reads:

```xml
    <dependencies>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>${junit.version}</version>
            <scope>test</scope>
        </dependency>
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
    </dependencies>
```

- [ ] **Step 2: Update Surefire `argLine` in `pom.xml`**

The existing line:

```xml
                    <argLine>-Djava.library.path=${project.basedir}/native/target/debug</argLine>
```

becomes:

```xml
                    <argLine>-Djava.library.path=${project.basedir}/native/target/debug --add-opens=java.base/java.nio=ALL-UNNAMED</argLine>
```

(Arrow Java's netty allocator backend reflects into `java.nio.DirectByteBuffer` on JDK 17. Without `--add-opens` you get an `IllegalAccessError` at runtime.)

- [ ] **Step 3: Verify deps resolve**

```bash
./mvnw dependency:resolve -q
```

Expected: exits 0. (First run will download ~6 jars from Maven Central.)

- [ ] **Step 4: Rewrite `SessionContext.java`**

Replace `src/main/java/org/apache/datafusion/SessionContext.java` with:

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datafusion;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

/**
 * A DataFusion session context.
 *
 * <p>Instances are <strong>not thread-safe</strong>. Concurrent calls to any of
 * {@link #sql}, {@link #registerParquet}, or {@link #close} from different threads can
 * produce a use-after-free on the native side. Callers must externally synchronize, or
 * confine each context to a single thread.
 */
public final class SessionContext implements AutoCloseable {
    static {
        NativeLibraryLoader.loadLibrary();
    }

    private long nativeHandle;

    public SessionContext() {
        this.nativeHandle = createSessionContext();
        if (this.nativeHandle == 0) {
            throw new RuntimeException("Failed to create native SessionContext");
        }
    }

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

    public void registerParquet(String name, String path) {
        if (nativeHandle == 0) {
            throw new IllegalStateException("SessionContext is closed");
        }
        registerParquet(nativeHandle, name, path);
    }

    @Override
    public void close() {
        if (nativeHandle != 0) {
            closeSessionContext(nativeHandle);
            nativeHandle = 0;
        }
    }

    private static native long createSessionContext();
    private static native void executeQuery(long handle, String sql, long ffiStreamAddr);
    private static native void registerParquet(long handle, String name, String path);
    private static native void closeSessionContext(long handle);
}
```

What changed vs. the previous version:
- Imports: added `arrow-c-data` and `arrow-vector` types.
- `sql(String)` (void) is gone; `sql(String, BufferAllocator)` returns `ArrowReader`.
- Native declaration `executeSql` replaced with `executeQuery(long, String, long)`.
- Class Javadoc and registerParquet/close are unchanged.

- [ ] **Step 5: Rewrite `SessionContextTest.java`**

Replace `src/test/java/org/apache/datafusion/SessionContextTest.java` with:

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datafusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

class SessionContextTest {
    @Test
    void canExecuteSelect1() throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
             SessionContext ctx = new SessionContext();
             ArrowReader reader = ctx.sql("SELECT 1", allocator)) {
            assertTrue(reader.loadNextBatch());
            assertEquals(1, reader.getVectorSchemaRoot().getRowCount());
        }
    }

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
}
```

Renames: the previous `registerAndQueryLineitem` becomes `selectCountStarLineitem`.

- [ ] **Step 6: Verify compilation succeeds**

```bash
./mvnw test-compile
```

Expected: `BUILD SUCCESS`. (If imports for arrow-java types fail, re-check Step 1.)

- [ ] **Step 7: Run the suite, expect runtime failure**

```bash
./mvnw test
```

Expected outcome — at least one test errors with `java.lang.UnsatisfiedLinkError` for `org.apache.datafusion.SessionContext.executeQuery`. The exact summary depends on whether SF1 data is present:

- With SF1 data: `Tests run: 2, Failures: 0, Errors: 2, Skipped: 0` (both tests link-error on `executeQuery`).
- Without SF1 data: `Tests run: 2, Failures: 0, Errors: 1, Skipped: 1` (`canExecuteSelect1` errors; lineitem skips).

If you instead see `IllegalAccessError` mentioning `java.nio` or `DirectByteBuffer`, the `--add-opens` in Step 2 didn't take effect — check the argLine.

- [ ] **Step 8: Commit**

```bash
git add pom.xml \
        src/main/java/org/apache/datafusion/SessionContext.java \
        src/test/java/org/apache/datafusion/SessionContextTest.java
git commit -m "feat: switch SessionContext.sql to return ArrowReader"
```

---

## Task 2: Rust JNI implementation

Goal: implement `Java_org_apache_datafusion_SessionContext_executeQuery`. After this, both tests pass (or pass + skip, depending on SF1 data presence).

**Files:**
- Modify: `native/src/lib.rs`

- [ ] **Step 1: Replace `executeSql` with `executeQuery`**

Open `native/src/lib.rs`. Find the existing `Java_org_apache_datafusion_SessionContext_executeSql` function (and its entire body, including the `try_unwrap_or_throw` block). Replace it with the new function below.

Also update the `use` declarations near the top of the file. Find:

```rust
use datafusion::error::DataFusionError;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use jni::objects::{JClass, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use tokio::runtime::Runtime;
```

Add three new lines so the block becomes:

```rust
use std::sync::Arc;

use datafusion::arrow::array::ffi_stream::FFI_ArrowArrayStream;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatchIterator;
use datafusion::error::DataFusionError;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use jni::objects::{JClass, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use tokio::runtime::Runtime;
```

(Keep the existing `use crate::errors::{try_unwrap_or_throw, JniResult};` line below.)

Then replace the `executeSql` JNI function with:

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

What changed vs. `executeSql`:
- Function name and JNI symbol changed.
- New parameter `ffi_stream_addr: jlong` — the address of a Java-allocated `ArrowArrayStream` C struct (80 bytes) that the native side will populate.
- Body now plans + collects + wraps in `RecordBatchIterator` + builds an `FFI_ArrowArrayStream` + `std::ptr::write`s it into the foreign address.
- Guards both `handle == 0` and `ffi_stream_addr == 0`.

The other JNI functions (`createSessionContext`, `registerParquet`, `closeSessionContext`) are unchanged.

- [ ] **Step 2: Build the Rust crate**

```bash
cd native && cargo build && cd ..
```

Expected: `Finished dev profile`. No warnings about unused imports.

If you get "no method `as_arrow` for `DFSchema`", check the import — `df.schema()` returns `&DFSchema`, which has `as_arrow() -> &Schema` in DataFusion 53. Should compile out of the box.

If you get "unresolved import `datafusion::arrow::array::ffi_stream`", the FFI stream module path may differ — try `datafusion::arrow::ffi_stream::FFI_ArrowArrayStream` (drop the inner `array::`).

- [ ] **Step 3: Run the JUnit suite**

```bash
./mvnw test
```

Expected outcomes:
- With SF1 data present: `Tests run: 2, Failures: 0, Errors: 0, Skipped: 0`. Both `canExecuteSelect1` and `selectCountStarLineitem` pass. The lineitem test asserts the count is exactly 6,001,215.
- Without SF1 data: `Tests run: 2, Failures: 0, Errors: 0, Skipped: 1`. Only `canExecuteSelect1` runs (and passes); lineitem skips.

If `selectCountStarLineitem` fails with an `assertEquals(6001215, ...)` mismatch, the SF1 data may have been generated with a different scale factor — re-run `make tpch-data`.

If you see "memory leaked" from `RootAllocator.close()`, a buffer wasn't released — usually a missing close on the reader or a bug in the Rust release callback path.

- [ ] **Step 4: Run the full pipeline from a clean state**

```bash
make clean && make test
```

Expected: native rebuilds (~1 min), then both tests pass (or pass + skip per SF1 data).

- [ ] **Step 5: Commit**

```bash
git add native/src/lib.rs
git commit -m "feat: implement JNI executeQuery returning ArrowArrayStream"
```

---

## Definition of Done

- `make clean && make test` exits 0.
- With SF1 data, `./mvnw test` reports `Tests run: 2, Failures: 0, Errors: 0, Skipped: 0`.
- `selectCountStarLineitem` asserts the lineitem count equals `6_001_215`.
- The `RootAllocator` close in each test does not report leaked buffers.
- Two new commits on `main`:
  1. `feat: switch SessionContext.sql to return ArrowReader`
  2. `feat: implement JNI executeQuery returning ArrowArrayStream`
- ASF license headers preserved on all source files.
