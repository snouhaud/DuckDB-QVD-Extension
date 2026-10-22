# Stable C ABI migration — notes

> This note concerns the **`qvd`** extension (branch `main`). `qvd_next` (this
> branch) deliberately uses the C API v2 on the unstable ABI, pinned to DuckDB
> 2.0.0: see the README.

**Status: done in v1.0.5 (2026-10). The extension is built on the stable C ABI
(`C_STRUCT`), V1 C API pinned at `v1.5.6`.**

## Why it happened

DuckDB 2.0.0 reserves the unstable ABI (`C_STRUCT_UNSTABLE`) for the new **V2 C
API**: an unstable binary must export `<name>_init_c_api_v2`. A V1 extension —
which is what the `duckdb` crate builds (`<name>_init_c_api`) — is only accepted
on the stable ABI, with a V1 API version of at most `v1.5.6`. Staying on the
unstable ABI was therefore no longer possible.

## Why it was possible

The two blockers recorded here in 2026-07 both went away with DuckDB 1.5.6:

1. **Copy functions outside the frozen set** — DuckDB 1.5.6 regenerated the C API
   from a spec (duckdb/duckdb#24131) and moved every post-`v1.2.0` function,
   copy functions included, into a stable `AT_LEAST(1, 5, 6)` band. The unstable
   tail of the V1 struct is empty. All functions we call are in the stable set.
2. **`duckdb-rs` is unstable-only** — its bindgen still passes
   `-DDUCKDB_EXTENSION_API_VERSION_UNSTABLE`, but with an empty unstable tail the
   generated struct is exactly the stable one. The requested API version comes
   from `DUCKDB_EXTENSION_MIN_DUCKDB_VERSION` (`src/lib.rs`), set by the Makefile.

No source change was needed: only `USE_UNSTABLE_C_API=0` and
`TARGET_DUCKDB_VERSION := v1.5.6` in the `Makefile`.

## What it buys

One binary loads on every DuckDB providing the V1 C API `v1.5.6`: verified on
**1.5.6 and 2.0.0** with the same build (`duckdb` crate `1.20000.0`). A DuckDB
release no longer forces a rebuild; bumping the crate is now optional (tests run
against the DuckDB version the crate encodes, via `DUCKDB_TEST_VERSION`).

## What to watch

- **Never call a function outside the stable V1 set.** If a later `duckdb` crate
  grows an unstable tail again, its symbols compile fine but are not guaranteed
  at load time.
- **The V2 C API** (`duckdb-neo` crate) brings COPY column names, filter
  pushdown, progress, etc., but is unstable (tied to one DuckDB version) as of
  2.0.0. Revisit once it has a stable surface.
