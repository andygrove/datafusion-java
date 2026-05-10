# Register Parquet Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose `SessionContext.registerParquet(String name, String path)` so a JUnit test can write a tiny Parquet file, register it as a table, and run `SELECT * FROM <name>` against it.

**Architecture:** Add one Java method on `SessionContext` (with the same use-after-close guard as `sql`), one matching JNI entry point in Rust that calls `datafusion::prelude::SessionContext::register_parquet` with `ParquetReadOptions::default()`. Test fixtures use `parquet-hadoop` 1.14.x at test scope to write a 3-row Parquet file into a JUnit `@TempDir`.

**Tech Stack:** Java 17, JUnit 5 (with `@TempDir`), `parquet-hadoop` 1.14.4 + `hadoop-common` 3.3.6 (test scope), Rust + DataFusion 53.

**Spec:** `docs/superpowers/specs/2026-05-10-register-parquet-design.md`.

---

## File Structure

| Path | Change |
| --- | --- |
| `pom.xml` | Add `parquet-hadoop` and `hadoop-common` test deps |
| `src/test/java/org/apache/datafusion/TestParquet.java` | NEW — helper to write a tiny Parquet file |
| `src/test/java/org/apache/datafusion/SessionContextTest.java` | Add `registerAndQueryParquet` test |
| `src/main/java/org/apache/datafusion/SessionContext.java` | Add `registerParquet(String, String)` + native declaration |
| `native/src/lib.rs` | Add `Java_org_apache_datafusion_SessionContext_registerParquet` |

---

## Prerequisites

- All Task 5 / earlier tasks complete (the existing skeleton + `sql()` works end-to-end).
- `./mvnw test` currently passes 1/1 test (`canExecuteSelect1`).
- The Rust crate at `native/` is using `datafusion = "53"`.

Verify before starting:

```bash
cd /Users/andy/git/apache/datafusion-java-bindings
./mvnw test                      # passes 1/1
ls native/target/debug/libdatafusion_jni.dylib   # exists
```

(macOS path; adjust extension on other platforms.)

Project conventions reminder:
- ASF license header on every new source file (Java `/* */`, Rust `//`).
- Conventional Commits in imperative mood. **No** `Co-Authored-By:` trailers.

---

## Task 1: Add Parquet test dependencies and `TestParquet` helper

Goal: bring in the libraries needed to write a Parquet file from a test, and ship a small helper that does exactly that. Helper is unused so far — Task 2 wires it into a real test.

**Files:**
- Modify: `pom.xml`
- Create: `src/test/java/org/apache/datafusion/TestParquet.java`

- [ ] **Step 1: Add the two test-scope dependencies to `pom.xml`**

Open `pom.xml` and inside the existing `<dependencies>` block (right after the `junit-jupiter` dependency), add:

```xml
        <dependency>
            <groupId>org.apache.parquet</groupId>
            <artifactId>parquet-hadoop</artifactId>
            <version>1.14.4</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-common</artifactId>
            <version>3.3.6</version>
            <scope>test</scope>
        </dependency>
```

(Why `hadoop-common`: `parquet-hadoop` 1.14.x marks Hadoop as `provided` scope, so callers must supply it. `hadoop-common` provides `Path`, `Configuration`, and `LocalFileSystem`, which is everything we need.)

- [ ] **Step 2: Resolve dependencies (sanity check)**

```bash
./mvnw dependency:resolve -q
```

Expected: exits 0 (artifacts download to `~/.m2/repository`). If you get "Could not resolve dependencies", the version numbers above may be wrong — check Maven Central for the latest 1.14.x of parquet-hadoop and 3.3.x of hadoop-common.

- [ ] **Step 3: Create `TestParquet.java`**

Path: `src/test/java/org/apache/datafusion/TestParquet.java`. Full file:

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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

final class TestParquet {

    private static final String SCHEMA_STR =
            "message People { required int32 id; required binary name (UTF8); }";

    private TestParquet() {}

    /** Writes a 3-row Parquet file ({@code (id INT32, name UTF8)}) at {@code file}. */
    static void writeTinyParquet(java.nio.file.Path file) throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType(SCHEMA_STR);
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);

        try (ParquetWriter<Group> writer = ExampleParquetWriter
                .builder(new Path(file.toUri()))
                .withConf(conf)
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .build()) {
            writer.write(factory.newGroup().append("id", 1).append("name", "alice"));
            writer.write(factory.newGroup().append("id", 2).append("name", "bob"));
            writer.write(factory.newGroup().append("id", 3).append("name", "carol"));
        }
    }
}
```

- [ ] **Step 4: Verify it compiles**

```bash
./mvnw test-compile
```

Expected: `BUILD SUCCESS`. If compilation fails on `org.apache.parquet.*` imports, the parquet-hadoop dep didn't come in correctly — re-check Step 1.

- [ ] **Step 5: Run existing test to confirm nothing regressed**

```bash
./mvnw test
```

Expected: `Tests run: 1, Failures: 0, Errors: 0` — only `canExecuteSelect1` runs. `TestParquet` is a helper class, not a test class (no `@Test` methods), so JUnit ignores it.

- [ ] **Step 6: Commit**

```bash
git add pom.xml src/test/java/org/apache/datafusion/TestParquet.java
git commit -m "test: add parquet-hadoop dep and TestParquet helper"
```

---

## Task 2: Add the failing test and Java `registerParquet` declaration

Goal: write the integration test that exercises the new method, then add the Java `registerParquet` method (with native declaration) so the test compiles. The test will fail at runtime with `UnsatisfiedLinkError` because the Rust side hasn't implemented the symbol yet — Task 3 fixes that.

**Files:**
- Modify: `src/test/java/org/apache/datafusion/SessionContextTest.java`
- Modify: `src/main/java/org/apache/datafusion/SessionContext.java`

- [ ] **Step 1: Add the new test method to `SessionContextTest.java`**

Open `src/test/java/org/apache/datafusion/SessionContextTest.java`. Add the import for `@TempDir` and `java.nio.file.Path`, and add the new test next to `canExecuteSelect1`. Final file content:

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

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SessionContextTest {
    @Test
    void canExecuteSelect1() {
        try (SessionContext ctx = new SessionContext()) {
            ctx.sql("SELECT 1");
        }
    }

    @Test
    void registerAndQueryParquet(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("people.parquet");
        TestParquet.writeTinyParquet(file);

        try (SessionContext ctx = new SessionContext()) {
            ctx.registerParquet("people", file.toString());
            ctx.sql("SELECT * FROM people");
        }
    }
}
```

- [ ] **Step 2: Verify the test fails to compile (intentional)**

```bash
./mvnw test-compile
```

Expected: compilation FAILS with something like `cannot find symbol: method registerParquet(String,String)` in `SessionContextTest`. This is the failing-test step of TDD — `registerParquet` doesn't exist on `SessionContext` yet.

- [ ] **Step 3: Add `registerParquet` to `SessionContext.java`**

Open `src/main/java/org/apache/datafusion/SessionContext.java`. Add a new public method between `sql(...)` and `close(...)`, plus a new `private static native` declaration alongside the existing three.

The Javadoc class header and existing methods stay. Add:

After `sql(String query)` (around the existing closing `}` of `sql`), add:

```java
    public void registerParquet(String name, String path) {
        if (nativeHandle == 0) {
            throw new IllegalStateException("SessionContext is closed");
        }
        registerParquet(nativeHandle, name, path);
    }
```

In the block of `private static native` declarations at the bottom of the class, add a fourth line:

```java
    private static native void registerParquet(long handle, String name, String path);
```

After your changes, the lower portion of `SessionContext.java` should read:

```java
    public void sql(String query) {
        if (nativeHandle == 0) {
            throw new IllegalStateException("SessionContext is closed");
        }
        executeSql(nativeHandle, query);
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
    private static native void executeSql(long handle, String sql);
    private static native void registerParquet(long handle, String name, String path);
    private static native void closeSessionContext(long handle);
}
```

- [ ] **Step 4: Verify compilation now succeeds**

```bash
./mvnw test-compile
```

Expected: `BUILD SUCCESS`. The test class now compiles because `SessionContext.registerParquet` exists.

- [ ] **Step 5: Run the tests, expect the new one to fail at runtime**

```bash
./mvnw test
```

Expected output should look like:

```
Tests run: 2, Failures: 0, Errors: 1, Skipped: 0
```

The error should be on `registerAndQueryParquet`, with a `java.lang.UnsatisfiedLinkError` for `org.apache.datafusion.SessionContext.registerParquet` — because the Rust JNI symbol doesn't exist yet. `canExecuteSelect1` should still pass.

If the failure is anything OTHER than `UnsatisfiedLinkError` for `registerParquet` (e.g., the parquet writer threw, or the test is failing to find `TestParquet.writeTinyParquet`), stop and investigate.

- [ ] **Step 6: Commit**

```bash
git add src/test/java/org/apache/datafusion/SessionContextTest.java \
        src/main/java/org/apache/datafusion/SessionContext.java
git commit -m "feat: add SessionContext.registerParquet Java API"
```

---

## Task 3: Implement the Rust JNI side

Goal: implement `Java_org_apache_datafusion_SessionContext_registerParquet`. After this task, both tests pass.

**Files:**
- Modify: `native/src/lib.rs`

- [ ] **Step 1: Add the `ParquetReadOptions` import**

Open `native/src/lib.rs`. Find the existing line:

```rust
use datafusion::prelude::SessionContext;
```

Replace it with:

```rust
use datafusion::prelude::{ParquetReadOptions, SessionContext};
```

- [ ] **Step 2: Add the new JNI entry point**

Append the following function to `native/src/lib.rs` (just before the closing of the file, after the existing `closeSessionContext` function):

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
            ctx.register_parquet(&name, &path, ParquetReadOptions::default())
                .await?;
            Ok::<(), datafusion::error::DataFusionError>(())
        })?;
        Ok(())
    })
}
```

(The error type is fully-qualified here because the existing `executeSql` uses `DataFusionError` from the top-level `use`. Either spelling compiles; using the fully-qualified path here keeps this hunk self-contained.)

- [ ] **Step 3: Build the Rust crate**

```bash
cd native && cargo build && cd ..
```

Expected: `Finished dev profile`. No warnings about unused imports.

If you see a complaint about `ParquetReadOptions` not being found in `datafusion::prelude`, it likely moved between DataFusion versions. Check `cd native && cargo doc --open -p datafusion` and grep for `ParquetReadOptions` — alternative paths in older versions: `datafusion::execution::options::ParquetReadOptions`.

- [ ] **Step 4: Run the JUnit suite**

```bash
./mvnw test
```

Expected: `Tests run: 2, Failures: 0, Errors: 0, Skipped: 0`. Both tests pass.

If `registerAndQueryParquet` still fails with an `UnsatisfiedLinkError`, the cargo build didn't produce an updated `libdatafusion_jni.dylib` — re-run `cargo build`. If it fails with a `RuntimeException`, read the message — most commonly the SQL `SELECT * FROM people` failed because the table wasn't actually registered (check the Rust code) or the Parquet file path didn't exist (check `TestParquet`).

- [ ] **Step 5: Run the full pipeline from a clean state**

```bash
make clean && make test
```

Expected: native rebuilds (~1m on first run after clean), then both tests pass.

- [ ] **Step 6: Commit**

```bash
git add native/src/lib.rs
git commit -m "feat: implement JNI bridge for SessionContext.registerParquet"
```

---

## Definition of Done

- `make clean && make test` exits 0.
- `./mvnw test` reports `Tests run: 2, Failures: 0, Errors: 0`.
- Three new commits on `main`:
  1. `test: add parquet-hadoop dep and TestParquet helper`
  2. `feat: add SessionContext.registerParquet Java API`
  3. `feat: implement JNI bridge for SessionContext.registerParquet`
- All new source files have ASF license headers.
- `git status` is clean.
