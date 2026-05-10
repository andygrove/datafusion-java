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
