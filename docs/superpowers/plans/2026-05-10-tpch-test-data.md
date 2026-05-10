# TPC-H Test Data Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the in-process Parquet writer (`parquet-hadoop` + `hadoop-common` test deps + `ParquetTestHelper`) with an externally-generated TPC-H SF1 dataset produced by `tpchgen-cli`. The integration test reads `tpch-data/sf1/lineitem.parquet` and skips cleanly when the file is absent.

**Architecture:** Three logical changes. (1) Rewrite the test to point at a fixed local path with `Assumptions.assumeTrue` so it's a clean skip when data is missing. (2) Drop the parquet/hadoop test deps now that nothing in-process generates parquet. (3) Add a `make tpch-data` Make target plus `.gitignore` and README updates so contributors know how to materialize the data once.

**Tech Stack:** Java 17, JUnit 5 (`Assumptions`), Maven, GNU Make, `tpchgen-cli` (Rust binary, installed via `cargo install tpchgen-cli`).

**Spec:** `docs/superpowers/specs/2026-05-10-tpch-test-data-design.md`.

---

## File Structure

| Path | Change |
| --- | --- |
| `src/test/java/org/apache/datafusion/SessionContextTest.java` | Rewrite the parquet test to use `tpch-data/sf1/lineitem.parquet` with `Assumptions.assumeTrue` |
| `src/test/java/org/apache/datafusion/ParquetTestHelper.java` | DELETE |
| `pom.xml` | Remove `parquet-hadoop` and `hadoop-common` test deps |
| `.gitignore` | Append `tpch-data/` |
| `Makefile` | Add `tpch-data` target; declare it in `.PHONY` |
| `README.md` | Add `tpchgen-cli` prereq + a "Test data" section |

---

## Prerequisites

- All earlier work merged (the existing `SessionContext.registerParquet` feature works end-to-end).
- `./mvnw test` currently passes 2/2.
- `tpchgen-cli` is on `PATH` (`cargo install tpchgen-cli` if not — check with `which tpchgen-cli`).

Verify before starting:

```bash
cd /Users/andy/git/apache/datafusion-java-bindings
./mvnw test                 # 2/2 passing
which tpchgen-cli           # expected: ~/.cargo/bin/tpchgen-cli
```

Project conventions: Conventional Commits in imperative mood. **No** `Co-Authored-By:` trailers. ASF header on all source files.

---

## Task 1: Rewrite the test, delete the in-process helper

Goal: switch the integration test to read a fixed lineitem path and skip when absent. After this task, `tpch-data/` doesn't exist yet, so the test is expected to **skip** — but the suite still passes.

**Files:**
- Modify: `src/test/java/org/apache/datafusion/SessionContextTest.java`
- Delete: `src/test/java/org/apache/datafusion/ParquetTestHelper.java`

- [ ] **Step 1: Rewrite `SessionContextTest.java`**

Replace the entire contents of `src/test/java/org/apache/datafusion/SessionContextTest.java` with:

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

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

class SessionContextTest {
    @Test
    void canExecuteSelect1() {
        try (SessionContext ctx = new SessionContext()) {
            ctx.sql("SELECT 1");
        }
    }

    @Test
    void registerAndQueryLineitem() {
        Path lineitem = Path.of("tpch-data/sf1/lineitem.parquet");
        Assumptions.assumeTrue(Files.exists(lineitem),
                "TPC-H SF1 data not found; run `make tpch-data` first");

        try (SessionContext ctx = new SessionContext()) {
            ctx.registerParquet("lineitem", lineitem.toAbsolutePath().toString());
            ctx.sql("SELECT COUNT(*) FROM lineitem");
        }
    }
}
```

Notes on what changed vs. the previous version:
- `@TempDir` parameter and `org.junit.jupiter.api.io.TempDir` import are gone.
- `org.apache.datafusion.ParquetTestHelper` is gone.
- New imports: `java.nio.file.Files`, `org.junit.jupiter.api.Assumptions`.
- `throws Exception` is gone (no checked-exception calls remain).

- [ ] **Step 2: Delete the in-process helper**

```bash
git rm src/test/java/org/apache/datafusion/ParquetTestHelper.java
```

- [ ] **Step 3: Verify the suite still compiles**

```bash
./mvnw test-compile
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 4: Run the suite, expect a skip**

```bash
./mvnw test
```

Expected: `canExecuteSelect1` passes. `registerAndQueryLineitem` is reported as **skipped** with the message `TPC-H SF1 data not found; run \`make tpch-data\` first`. Surefire's summary line should look like:

```
Tests run: 2, Failures: 0, Errors: 0, Skipped: 1
```

(JUnit 5's `Assumptions.assumeTrue` throws `TestAbortedException` which Surefire reports as Skipped.)

If the test errors instead of skipping (e.g., `NoSuchFileException`), check the path string and that `Files.exists` is what's being called.

- [ ] **Step 5: Commit**

```bash
git add src/test/java/org/apache/datafusion/SessionContextTest.java
git commit -m "test: read TPC-H lineitem.parquet, skip when missing"
```

(The `git rm` from Step 2 already staged the deletion.)

---

## Task 2: Remove `parquet-hadoop` and `hadoop-common` test deps

Goal: now that nothing in-process generates Parquet, drop the two heavyweight test deps. Verify the dependency tree contains no `parquet-*` or `hadoop-*` artifacts and that the suite still runs (skipping the lineitem test).

**Files:**
- Modify: `pom.xml`

- [ ] **Step 1: Remove the two `<dependency>` blocks from `pom.xml`**

Open `pom.xml` and delete this block (currently between the `junit-jupiter` dependency and the closing `</dependencies>`):

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

After the edit, the `<dependencies>` block should contain only the `junit-jupiter` test dep.

- [ ] **Step 2: Verify the dep tree is clean**

```bash
./mvnw dependency:tree -q | grep -E 'parquet|hadoop' || echo "CLEAN"
```

Expected: `CLEAN` (no matches).

- [ ] **Step 3: Run the suite (still skipping)**

```bash
./mvnw test
```

Expected: `Tests run: 2, Failures: 0, Errors: 0, Skipped: 1`. Same as Task 1's outcome — removing the deps doesn't change behavior because nothing on the test classpath was using them anymore.

- [ ] **Step 4: Commit**

```bash
git add pom.xml
git commit -m "build: drop parquet-hadoop and hadoop-common test deps"
```

---

## Task 3: Add `make tpch-data`, `.gitignore`, README updates; verify end-to-end

Goal: provide the canonical way to generate the test data, document it, and confirm the test goes from skip to pass once data exists.

**Files:**
- Modify: `Makefile`
- Modify: `.gitignore`
- Modify: `README.md`

- [ ] **Step 1: Add `tpch-data/` to `.gitignore`**

Append a single line to `.gitignore`:

```
tpch-data/
```

After the edit, the file should look like:

```
target/
*.class
.idea/
.vscode/
*.iml
.DS_Store
tpch-data/
```

- [ ] **Step 2: Add the `tpch-data` target to `Makefile`**

Open `Makefile`. Update the `.PHONY` line and add the new target at the end of the file. The recipe lines must use literal **tabs** (not spaces).

Change the `.PHONY` line from:

```makefile
.PHONY: all native jvm test clean
```

to:

```makefile
.PHONY: all native jvm test clean tpch-data
```

And append at the end of the file:

```makefile

tpch-data:
	@command -v tpchgen-cli >/dev/null || \
		(echo "Install: cargo install tpchgen-cli" && exit 1)
	mkdir -p tpch-data/sf1
	tpchgen-cli -s 1 -f parquet -o tpch-data/sf1
```

(Two tabs, one for the `@command -v` line and one for the `(echo ...)` continuation; the `\` at end of line lets Make see it as a single recipe line. The `mkdir` and `tpchgen-cli` lines are tab-indented too.)

- [ ] **Step 3: Sanity-check the Make target's syntax**

```bash
make -n tpch-data
```

Expected: prints what the target *would* run — something like:

```
command -v tpchgen-cli >/dev/null || 		(echo "Install: cargo install tpchgen-cli" && exit 1)
mkdir -p tpch-data/sf1
tpchgen-cli -s 1 -f parquet -o tpch-data/sf1
```

If `make` complains about tabs vs spaces (`*** missing separator. Stop.`), the indentation got mangled — re-edit with literal tabs.

- [ ] **Step 4: Update `README.md`**

Open `README.md`. After the existing Prerequisites bullets, append a third bullet (after "Rust toolchain"):

```
- [`tpchgen-cli`](https://github.com/clflushopt/tpchgen-rs) — install with `cargo install tpchgen-cli`. Only needed if you want to run the Parquet integration test.
```

Then add a new `## Test data` section between the existing `## Build & Test` and `## Status` sections:

```
## Test data

The Parquet integration test reads TPC-H SF1 data (~1GB across 8 tables). Generate it once with:

    make tpch-data

Tests that need this data skip cleanly if it's missing.
```

After your edits, the full README should read:

```markdown
# datafusion-java-bindings

DataFusion Java Bindings (Experimental).

## Prerequisites

- JDK 17
- Rust toolchain (stable, via rustup)
- [`tpchgen-cli`](https://github.com/clflushopt/tpchgen-rs) — install with `cargo install tpchgen-cli`. Only needed if you want to run the Parquet integration test.

Maven is bundled via the `./mvnw` wrapper.

## Build & Test

    make test

This builds the native crate and runs the JUnit tests. Run the steps individually with:

    cd native && cargo build
    ./mvnw test

## Test data

The Parquet integration test reads TPC-H SF1 data (~1GB across 8 tables). Generate it once with:

    make tpch-data

Tests that need this data skip cleanly if it's missing.

## Status

Currently exposes two Java entry points on `org.apache.datafusion.SessionContext`:

- `sql(String)` — execute a SQL query through DataFusion. Does not return data yet.
- `registerParquet(String name, String path)` — register a local Parquet file as a SQL table named `name`.
```

- [ ] **Step 5: Generate the data and verify the test now passes**

```bash
make tpch-data
```

Expected: `tpchgen-cli` runs, prints progress, and writes 8 parquet files under `tpch-data/sf1/`. Total runtime ~30s on a modern machine. Verify:

```bash
ls -lh tpch-data/sf1/lineitem.parquet
```

Expected: a file in the hundreds of megabytes range.

- [ ] **Step 6: Run the full suite — both tests should now pass**

```bash
./mvnw test
```

Expected:

```
Tests run: 2, Failures: 0, Errors: 0, Skipped: 0
```

Both `canExecuteSelect1` and `registerAndQueryLineitem` pass. The lineitem test internally does `df.collect()` on `SELECT COUNT(*) FROM lineitem` — DataFusion only collects 1 row (the count), so the test runs in well under a second despite the 6M-row table.

- [ ] **Step 7: Confirm `tpch-data/` is correctly ignored by git**

```bash
git status
```

Expected: only `Makefile`, `.gitignore`, and `README.md` show as modified — `tpch-data/` should NOT appear.

- [ ] **Step 8: Commit**

```bash
git add Makefile .gitignore README.md
git commit -m "build: add tpch-data target and document SF1 generation"
```

---

## Definition of Done

- `git status` is clean.
- `./mvnw dependency:tree -q | grep -E 'parquet|hadoop'` returns no matches.
- `./mvnw test` reports `Tests run: 2, Failures: 0, Errors: 0, Skipped: 0` after `make tpch-data` has been run.
- `./mvnw test` (in a separate clone or after `rm -rf tpch-data`) reports `Skipped: 1` and still exits 0.
- Three new commits on `main`:
  1. `test: read TPC-H lineitem.parquet, skip when missing`
  2. `build: drop parquet-hadoop and hadoop-common test deps`
  3. `build: add tpch-data target and document SF1 generation`
