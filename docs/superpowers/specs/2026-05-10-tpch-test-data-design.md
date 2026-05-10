# Replace Parquet Test Helper with TPC-H SF1

**Date:** 2026-05-10
**Status:** Approved

## Goal

Drop the heavyweight `parquet-hadoop` + `hadoop-common` test dependencies and
the in-process Parquet writer (`ParquetTestHelper`). Replace them with an
external data-generation step using
[`tpchgen-cli`](https://github.com/clflushopt/tpchgen-rs), and rewrite the
existing `registerAndQueryParquet` test to query a real TPC-H `lineitem` file
at scale factor 1.

Success criterion: `make test` passes with neither `parquet-hadoop` nor
`hadoop-common` on any classpath. The Parquet integration test runs when
SF1 data is present, and skips cleanly with an instructive message when it
isn't.

## Non-goals

- Parameterising scale factor. SF1 only.
- Auto-installing `tpchgen-cli`. The user installs it once via
  `cargo install tpchgen-cli`.
- Auto-running `make tpch-data` from `make test`. The data target is
  manual; the test is robust to its absence.
- Caching the data anywhere outside the repo. Single canonical path.
- Multiple scale-factor directories, env-var overrides, or alternative
  data sources.
- Returning data from `sql()` — still deferred.

## Data location

`tpch-data/sf1/` at the repo root. Files end up as
`tpch-data/sf1/customer.parquet`, `tpch-data/sf1/lineitem.parquet`, etc.

Deliberately NOT under `target/`: Maven's `mvn clean` and our `make clean`
both remove `target/`, and we don't want a 1GB regenerate every clean.

`.gitignore` gains the line `tpch-data/`.

## Removals

### `pom.xml`

Drop these two dependency blocks (both currently at test scope):

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

### `src/test/java/org/apache/datafusion/ParquetTestHelper.java`

Delete the file. Its only caller is the test method we're rewriting.

## Additions

### `Makefile`

New PHONY target `tpch-data`:

```makefile
tpch-data:
	@command -v tpchgen-cli >/dev/null || \
		(echo "Install: cargo install tpchgen-cli" && exit 1)
	mkdir -p tpch-data/sf1
	tpchgen-cli -s 1 -f parquet -o tpch-data/sf1
```

Notes:
- The existence check makes the failure mode obvious; `tpchgen-cli` is
  not bundled.
- `make clean` is NOT updated — it must NOT remove `tpch-data/`, or every
  clean costs ~30 seconds of regeneration.
- Add `tpch-data` to `.PHONY`.

### `.gitignore`

Append:

```
tpch-data/
```

### `SessionContextTest.java`

Replace the existing `registerAndQueryParquet` method with:

```java
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
```

Imports added:
- `java.nio.file.Files`
- `org.junit.jupiter.api.Assumptions`

Imports removed:
- `org.junit.jupiter.api.io.TempDir` (no longer needed; `@TempDir` parameter
  goes away with the old test)

The existing `canExecuteSelect1` test is unchanged.

Why `COUNT(*)` not `SELECT *`: the Rust side does
`ctx.sql(...).await?.collect().await?`. A bare `SELECT * FROM lineitem`
would materialise all 6M rows just to discard them. `COUNT(*)` is the
idiomatic smoke test for "table is registered and readable".

Path resolution: Surefire runs the JVM with cwd at the project root, so
`Path.of("tpch-data/sf1/lineitem.parquet")` resolves correctly.
`toAbsolutePath()` is then passed to native code so DataFusion doesn't
have to guess about cwd.

### `README.md`

Add to Prerequisites:

> - [`tpchgen-cli`](https://github.com/clflushopt/tpchgen-rs) — install with
>   `cargo install tpchgen-cli`. Only needed if you want to run the Parquet
>   integration test.

Add a new section before Status:

> ## Test data
>
> The Parquet integration test reads TPC-H SF1 data (~1GB across 8 tables).
> Generate it once with:
>
>     make tpch-data
>
> Tests that need this data skip cleanly if it's missing.

## Files touched

| File | Change |
| --- | --- |
| `pom.xml` | Remove `parquet-hadoop` and `hadoop-common` deps |
| `src/test/java/org/apache/datafusion/ParquetTestHelper.java` | Delete |
| `src/test/java/org/apache/datafusion/SessionContextTest.java` | Replace `registerAndQueryParquet` with `registerAndQueryLineitem` |
| `.gitignore` | Append `tpch-data/` |
| `Makefile` | Add `tpch-data` target |
| `README.md` | Add `tpchgen-cli` prereq and a Test data section |

## Definition of done

- `./mvnw dependency:tree` shows neither `parquet-*` nor `hadoop-*` artifacts.
- `make test` passes either way. With SF1 data absent, the Parquet test
  reports as skipped (Surefire `Skipped: 1`); `canExecuteSelect1` still
  passes. With SF1 data present, both tests pass and `Skipped: 0`.
- ASF license headers preserved on every modified source file.
