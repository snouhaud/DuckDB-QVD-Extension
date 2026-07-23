# Stable C ABI migration — notes

**Status: not pursued (2026-07). Staying on the unstable C ABI.** This file records
why, and what would have to change for the migration to become worthwhile.

## The problem it would solve

The extension is built with `--abi-type C_STRUCT_UNSTABLE` (DuckDB's *unstable* C
extension ABI). A binary therefore loads on **exactly one** DuckDB version — every
DuckDB release requires a rebuild (bump the `duckdb` crate, move the
`extension-ci-tools` submodule branch, `make release`).

DuckDB's *stable* C ABI (`C_STRUCT`) is forward-compatible: build once, load on
multiple DuckDB versions. Moving to it is the only way to drop the per-version
rebuild — **not** dropping the C API itself (impossible for a Rust extension: the
C API is the only interface Rust has to DuckDB; the C++ internal API is not
callable from Rust).

## Why we don't do it today — two blockers

1. **`duckdb-rs` is unstable-only.** The whole safe layer we rely on (`read_qvd`'s
   `VTab`, the C-API entrypoint, Arrow/vector conversion) is built on the unstable
   API struct. There is no stable-ABI mode in the crate. Going stable means
   dropping duckdb-rs's extension layer and hand-rolling raw FFI against the frozen
   subset of `duckdb.h` — a substantial rewrite of `src/lib.rs` and part of
   `src/qvd.rs`.

2. **The COPY function API is outside the frozen set.** The stable baseline is
   frozen at `v1.2.0` (`DUCKDB_EXTENSION_API_VERSION_STRING "v1.2.0"` in
   `extension_api.hpp`). The copy functions (`duckdb_create_copy_function`,
   `duckdb_register_copy_function`, `…_set_copy_from_function`, …) sit in the
   post-freeze region of the `duckdb_ext_api_v1` struct. So `COPY … TO/FROM
   (FORMAT qvd)` (all of `src/copy.rs` + `src/copy_from.rs`) **cannot** be built on
   the stable ABI until DuckDB promotes copy functions into the frozen set — which
   is upstream's decision, not ours. (The read path may also touch post-1.2.0
   functions, e.g. table-function projection pushdown — verify before assuming the
   reader alone can go stable.)

## When to revisit

Reconsider if **all** of these hold:

- Forward compatibility becomes a hard requirement (e.g. distributing a single
  binary across DuckDB versions, or the per-release rebump becomes a real burden).
- DuckDB promotes the copy-function C API into the stable frozen set — check
  `extension_api.hpp` / release notes. **Blocker #2 is the gate; without it even a
  partial migration can't cover writing.**
- Either `duckdb-rs` gains stable-ABI support, or we accept a raw-FFI rewrite.

## If revisited, the only sensible shape

Split the extension: a **read-only `read_qvd`** built as raw FFI on the stable ABI
(forward-compatible reads), with the **write path staying on the unstable ABI**.
This doubles maintenance, so only do it on real need — not speculatively.

## Cost comparison (why staying put wins for now)

- Per-version rebump (current): bump `duckdb` crate + submodule branch +
  `make release`, ~2 min, **zero source changes**.
- Stable-ABI migration: weeks of work, partly **blocked upstream**, and only pays
  off once forward-compat is actually needed.
