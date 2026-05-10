# Initial Infrastructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up a Maven project + Rust crate skeleton so a Java `SessionContext.sql("SELECT 1")` call drives `datafusion::SessionContext::sql` over JNI without throwing.

**Architecture:** Single-module Maven project (Java 17, JUnit 5). Rust `cdylib` crate at `native/` depending on the latest DataFusion. JNI bridge stores a raw `Box<SessionContext>` pointer in a `long` handle on the Java side. Three JNI entry points: create, execute, close. Native library loaded at test time via `java.library.path` set by Surefire.

**Tech Stack:** Java 17, JUnit 5, Maven, Rust 2021, `datafusion = "50"`, `jni = "0.21"`, `tokio = "1"`.

**Spec:** `docs/superpowers/specs/2026-05-10-initial-infrastructure-design.md`.

---

## File Structure

Files created across the plan:

| Path | Responsibility |
| --- | --- |
| `.gitignore` | Ignore `target/`, IDE files |
| `pom.xml` | Maven build, JUnit 5, Surefire `java.library.path` |
| `Makefile` | Orchestrate `cargo build` and `./mvnw test` |
| `mvnw`, `mvnw.cmd`, `.mvn/wrapper/maven-wrapper.properties` | Maven Wrapper |
| `native/Cargo.toml` | Rust crate manifest, `cdylib`, deps |
| `native/src/lib.rs` | Three JNI entry points + Tokio runtime singleton |
| `native/src/errors.rs` | `try_unwrap_or_throw` helper |
| `src/main/java/org/apache/datafusion/NativeLibraryLoader.java` | One-shot `System.loadLibrary("datafusion_jni")` |
| `src/main/java/org/apache/datafusion/SessionContext.java` | User-facing class, `AutoCloseable`, holds `nativeHandle` |
| `src/test/java/org/apache/datafusion/SessionContextTest.java` | One JUnit test: construct, run `SELECT 1`, close |

---

## Prerequisites

- JDK 17 on `JAVA_HOME` and `PATH`.
- Rust toolchain via `rustup` (stable channel).
- Maven 3.9.x on `PATH` (`mvn -v` should work).

Verify before starting:

```bash
java -version    # 17.x
mvn -v           # 3.9.x, picks up Java 17
cargo --version  # any recent stable
```

---

## Task 1: Project bootstrap (`.gitignore`)

**Files:**
- Create: `.gitignore`

- [ ] **Step 1: Create `.gitignore`**

```
target/
*.class
.idea/
.vscode/
*.iml
.DS_Store
```

- [ ] **Step 2: Commit**

```bash
git add .gitignore
git commit -m "chore: add .gitignore"
```

---

## Task 2: Maven project skeleton with sanity test

Goal: prove `mvn test` runs end-to-end with a trivial test, before introducing native code.

**Files:**
- Create: `pom.xml`
- Create: `src/test/java/org/apache/datafusion/SanityTest.java`

- [ ] **Step 1: Create `pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
             http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.apache.datafusion</groupId>
    <artifactId>datafusion-java-bindings</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <packaging>jar</packaging>

    <properties>
        <maven.compiler.source>17</maven.compiler.source>
        <maven.compiler.target>17</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <junit.version>5.11.3</junit.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>${junit.version}</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>3.5.2</version>
                <configuration>
                    <argLine>-Djava.library.path=${project.basedir}/native/target/debug</argLine>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 2: Write sanity test**

`src/test/java/org/apache/datafusion/SanityTest.java`:

```java
package org.apache.datafusion;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SanityTest {
    @Test
    void mavenBuildWorks() {
        assertEquals(2, 1 + 1);
    }
}
```

- [ ] **Step 3: Run the sanity test**

```bash
mvn test
```

Expected: `BUILD SUCCESS`, `Tests run: 1, Failures: 0, Errors: 0`. (You'll see a warning that `java.library.path` points to a directory that doesn't exist yet — ignore it; the dir is created in Task 3.)

- [ ] **Step 4: Commit**

```bash
git add pom.xml src/test/java/org/apache/datafusion/SanityTest.java
git commit -m "build: add Maven project skeleton with JUnit 5"
```

---

## Task 3: Rust crate skeleton

Goal: produce `native/target/debug/libdatafusion_jni.{dylib,so,dll}` with no JNI functions yet. Confirms toolchain + DataFusion version resolves.

**Files:**
- Create: `native/Cargo.toml`
- Create: `native/src/lib.rs`

- [ ] **Step 1: Create `native/Cargo.toml`**

```toml
[package]
name = "datafusion-jni"
version = "0.1.0"
edition = "2021"
publish = false

[lib]
crate-type = ["cdylib"]

[dependencies]
datafusion = "50"
jni = "0.21"
tokio = { version = "1", features = ["rt-multi-thread"] }
```

(If a newer DataFusion major has been released by the time you read this, prefer it. Check `cargo search datafusion --limit 1` and update.)

- [ ] **Step 2: Create `native/src/lib.rs` (placeholder)**

```rust
// JNI entry points are added in Task 5.
```

- [ ] **Step 3: Build the crate**

```bash
cd native && cargo build
```

Expected: `Compiling datafusion v50.x.y` ... `Finished dev profile`. Verify the artifact exists:

```bash
ls native/target/debug/libdatafusion_jni.*
```

(macOS: `.dylib`; Linux: `.so`; Windows: `datafusion_jni.dll` directly under `target/debug/`.)

- [ ] **Step 4: Commit**

```bash
git add native/Cargo.toml native/Cargo.lock native/src/lib.rs
git commit -m "build: add Rust crate skeleton depending on DataFusion"
```

---

## Task 4: Java `SessionContext` + failing test

Goal: write the user-facing Java types and a JUnit test that drives them. The test will compile but fail at runtime with `UnsatisfiedLinkError` because the Rust JNI methods don't exist yet. Task 5 makes it pass.

**Files:**
- Create: `src/main/java/org/apache/datafusion/NativeLibraryLoader.java`
- Create: `src/main/java/org/apache/datafusion/SessionContext.java`
- Create: `src/test/java/org/apache/datafusion/SessionContextTest.java`
- Delete: `src/test/java/org/apache/datafusion/SanityTest.java`

- [ ] **Step 1: Write `NativeLibraryLoader`**

`src/main/java/org/apache/datafusion/NativeLibraryLoader.java`:

```java
package org.apache.datafusion;

public final class NativeLibraryLoader {
    private static boolean loaded = false;

    private NativeLibraryLoader() {}

    public static synchronized void loadLibrary() {
        if (!loaded) {
            System.loadLibrary("datafusion_jni");
            loaded = true;
        }
    }
}
```

- [ ] **Step 2: Write `SessionContext`**

`src/main/java/org/apache/datafusion/SessionContext.java`:

```java
package org.apache.datafusion;

public final class SessionContext implements AutoCloseable {
    static {
        NativeLibraryLoader.loadLibrary();
    }

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

- [ ] **Step 3: Write the failing test**

`src/test/java/org/apache/datafusion/SessionContextTest.java`:

```java
package org.apache.datafusion;

import org.junit.jupiter.api.Test;

class SessionContextTest {
    @Test
    void canExecuteSelect1() {
        try (SessionContext ctx = new SessionContext()) {
            ctx.sql("SELECT 1");
        }
    }
}
```

- [ ] **Step 4: Delete the sanity test**

```bash
rm src/test/java/org/apache/datafusion/SanityTest.java
```

- [ ] **Step 5: Run the test, expect failure**

```bash
mvn test
```

Expected: `Tests run: 1, Failures: 0, Errors: 1`. The error should be `java.lang.UnsatisfiedLinkError` — either because the library can't be loaded (file not yet rebuilt with native methods) or because `createSessionContext` is unimplemented. Either is fine; both are fixed by Task 5.

- [ ] **Step 6: Commit**

```bash
git add src/main/java src/test/java
git commit -m "feat: add SessionContext and NativeLibraryLoader Java classes"
```

---

## Task 5: Rust JNI implementations

Goal: implement the three JNI entry points + the error-throwing helper, then `mvn test` passes.

**Files:**
- Create: `native/src/errors.rs`
- Modify: `native/src/lib.rs` (replace placeholder)

- [ ] **Step 1: Write `errors.rs`**

`native/src/errors.rs`:

```rust
use std::any::Any;
use std::error::Error;
use std::panic::{catch_unwind, AssertUnwindSafe};

use jni::JNIEnv;

pub type JniResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

pub fn try_unwrap_or_throw<T, F>(env: &mut JNIEnv, default: T, f: F) -> T
where
    F: FnOnce(&mut JNIEnv) -> JniResult<T>,
{
    match catch_unwind(AssertUnwindSafe(|| f(env))) {
        Ok(Ok(value)) => value,
        Ok(Err(err)) => {
            throw_runtime_exception(env, &err.to_string());
            default
        }
        Err(panic) => {
            throw_runtime_exception(env, &panic_message(&panic));
            default
        }
    }
}

fn throw_runtime_exception(env: &mut JNIEnv, message: &str) {
    if env.exception_check().unwrap_or(false) {
        return;
    }
    let _ = env.throw_new("java/lang/RuntimeException", message);
}

fn panic_message(panic: &Box<dyn Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else {
        "rust panic with non-string payload".to_string()
    }
}
```

- [ ] **Step 2: Replace `native/src/lib.rs` with the JNI bridge**

`native/src/lib.rs`:

```rust
mod errors;

use std::sync::OnceLock;

use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use jni::objects::{JClass, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use tokio::runtime::Runtime;

use crate::errors::{try_unwrap_or_throw, JniResult};

fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| Runtime::new().expect("failed to create Tokio runtime"))
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_createSessionContext<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    try_unwrap_or_throw(&mut env, 0, |_env| -> JniResult<jlong> {
        let ctx = SessionContext::new();
        Ok(Box::into_raw(Box::new(ctx)) as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_executeSql<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    sql: JString<'local>,
) {
    try_unwrap_or_throw(&mut env, (), |env| -> JniResult<()> {
        if handle == 0 {
            return Err("SessionContext handle is null".into());
        }
        let ctx = unsafe { &*(handle as *const SessionContext) };
        let sql_str: String = env.get_string(&sql)?.into();
        runtime().block_on(async {
            let df = ctx.sql(&sql_str).await?;
            df.collect().await?;
            Ok::<(), DataFusionError>(())
        })?;
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_datafusion_SessionContext_closeSessionContext<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    try_unwrap_or_throw(&mut env, (), |_env| -> JniResult<()> {
        if handle != 0 {
            unsafe {
                drop(Box::from_raw(handle as *mut SessionContext));
            }
        }
        Ok(())
    })
}
```

- [ ] **Step 3: Build the native library**

```bash
cd native && cargo build
```

Expected: `Finished dev profile`. No warnings about unused code in `lib.rs`.

- [ ] **Step 4: Run the JUnit test**

From the project root:

```bash
mvn test
```

Expected: `Tests run: 1, Failures: 0, Errors: 0`.

If you get `UnsatisfiedLinkError: no datafusion_jni in java.library.path`, double-check that `native/target/debug/libdatafusion_jni.{dylib,so}` exists and that Surefire's `argLine` is being applied (`mvn test -X | grep argLine`).

If you get a `RuntimeException` thrown from native code, the SQL call failed — read the message; most commonly Tokio runtime issues or a DataFusion API change between versions.

- [ ] **Step 5: Commit**

```bash
git add native/src/lib.rs native/src/errors.rs native/Cargo.lock
git commit -m "feat: implement JNI bridge for SessionContext.sql"
```

---

## Task 6: Maven Wrapper

Goal: ship `./mvnw` so contributors don't need a system Maven. The plan still uses system `mvn` up to this point because we need it to *generate* the wrapper.

**Files:**
- Create (via plugin): `mvnw`, `mvnw.cmd`, `.mvn/wrapper/maven-wrapper.properties`

- [ ] **Step 1: Generate the wrapper**

```bash
mvn -N wrapper:wrapper -Dmaven=3.9.9
```

Expected: `BUILD SUCCESS`. New files appear: `mvnw`, `mvnw.cmd`, `.mvn/wrapper/maven-wrapper.properties`.

- [ ] **Step 2: Verify the wrapper runs**

```bash
./mvnw -v
```

Expected: prints Maven 3.9.9 and the JDK 17 it detects.

- [ ] **Step 3: Re-run the test through the wrapper**

```bash
./mvnw test
```

Expected: same green result as before.

- [ ] **Step 4: Commit**

```bash
git add mvnw mvnw.cmd .mvn
git commit -m "build: add Maven Wrapper"
```

---

## Task 7: Makefile + README

Goal: one-command build (`make test`) and a README that explains how to use the project.

**Files:**
- Create: `Makefile`
- Modify: `README.md`

- [ ] **Step 1: Create `Makefile`**

Use literal tabs for the recipe lines, not spaces — `make` requires tabs.

```makefile
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

- [ ] **Step 2: Update `README.md`**

Replace the file's contents with the text below (write it directly — no surrounding fences):

    # datafusion-java-bindings

    DataFusion Java Bindings (Experimental).

    ## Prerequisites

    - JDK 17
    - Rust toolchain (stable, via rustup)

    Maven is bundled via the `./mvnw` wrapper.

    ## Build & Test

        make test

    This builds the native crate and runs the JUnit tests. Run the steps individually with:

        cd native && cargo build
        ./mvnw test

    ## Status

    Currently exposes a single Java entry point: `org.apache.datafusion.SessionContext.sql(String)`. The method runs the query through DataFusion but does not return data yet.

- [ ] **Step 3: Verify `make test` works from a clean state**

```bash
make clean && make test
```

Expected: native build runs, then `./mvnw test` runs and passes.

- [ ] **Step 4: Commit**

```bash
git add Makefile README.md
git commit -m "build: add Makefile and update README"
```

---

## Definition of Done

- `make clean && make test` exits 0.
- `git status` is clean.
- The committed history reads as 7 small, logical commits.
- The plan's success criterion holds: a Java `SessionContext` constructed, `sql("SELECT 1")` invoked, and the context closed — all without exceptions.
