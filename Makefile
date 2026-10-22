# Rust DuckDB extension based on the C Extension API.
PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXTENSION_NAME=qvd

# Stable C ABI (C_STRUCT): since DuckDB 2.0 the unstable ABI is reserved to the
# V2 C API, so a V1 extension (duckdb-rs `duckdb` crate) must be stable. The
# binary then loads on every DuckDB whose V1 C API is >= TARGET_DUCKDB_VERSION.
USE_UNSTABLE_C_API=0

# Minimum V1 C API version requested at load (stamped in the footer). The V1 API
# is frozen at v1.5.6, which holds every function we call (copy functions included).
TARGET_DUCKDB_VERSION := v1.5.6

# Test against the DuckDB the pinned `duckdb` crate in Cargo.toml was built for:
# its version encodes it (e.g. 1.20000.0 -> 2.0.0). Without this, ci-tools
# pip-installs the latest DuckDB release.
DUCKDB_RS_ENC := $(shell sed -n 's/^duckdb = .*version = "=1\.\([0-9]\{5\}\)\.[0-9]*".*/\1/p' Cargo.toml)
DUCKDB_TEST_VERSION := $(shell v=$(DUCKDB_RS_ENC); echo "$$((v/10000)).$$((v/100%100)).$$((v%100))")

all: configure debug

# Initialise the extension-ci-tools submodule (the community build template).
# Already present after `git clone --recurse-submodules`; this fetches/refreshes
# it otherwise (e.g. a non-recursive clone). Run once before `make configure`.
.PHONY: bootstrap
bootstrap:
	git submodule update --init --recursive

# Reusable makefiles from extension-ci-tools (pinned submodule). Guarded: if the
# submodule isn't checked out yet, print a clear hint instead of GNU make's
# cryptic "No rule to make target ...Makefile" error from the `include`. The
# `bootstrap` target above stays reachable so a single command bootstraps it.
CI_TOOLS_MK := extension-ci-tools/makefiles/c_api_extensions/rust.Makefile
ifeq (,$(wildcard $(CI_TOOLS_MK)))
$(warning extension-ci-tools submodule missing — run: make bootstrap)
else
include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/rust.Makefile
endif

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
