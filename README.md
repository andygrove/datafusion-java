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

Currently exposes two Java entry points on `org.apache.datafusion.SessionContext`:

- `sql(String)` — execute a SQL query through DataFusion. Does not return data yet.
- `registerParquet(String name, String path)` — register a local Parquet file as a SQL table named `name`.
