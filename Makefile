# Rust DuckDB extension based on the C Extension API.
PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXTENSION_NAME=qvd

# Set to 1 to enable the unstable C API (binaries only work on TARGET_DUCKDB_VERSION,
# forward compatibility is broken). Required: duckdb-rs relies on the unstable C API.
USE_UNSTABLE_C_API=1

# Target DuckDB version (matches the community-extensions build version).
TARGET_DUCKDB_VERSION=v1.5.3

all: configure debug

# Fetch the shared CI tooling (venv DuckDB + SQLLogicTest runner).
# Run once before `make configure`.
.PHONY: bootstrap
bootstrap:
	git clone --depth 1 https://github.com/duckdb/extension-ci-tools.git

# Reusable makefiles from extension-ci-tools (the community build template).
include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/rust.Makefile

configure: venv platform extension_version

debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release

test: test_debug
test_debug: test_extension_debug
test_release: test_extension_release

clean: clean_build clean_rust
clean_all: clean_configure clean

# --- Fast local iteration (without the CI tooling) ------------------------
# Builds the native library. The DuckDB footer is NOT appended here: to produce
# a real loadable .duckdb_extension, use `make debug` after
# `make bootstrap && make configure`.
.PHONY: cargo-build cargo-build-release cargo-clean
cargo-build:
	cargo build

cargo-build-release:
	cargo build --release

cargo-clean:
	cargo clean
