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
