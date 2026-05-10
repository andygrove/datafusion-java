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
