# DuckDB `qvd` Extension — `read_qvd()` / `COPY TO (FORMAT qvd)`

**100% Rust** DuckDB extension to **read and write** Qlik **QVD** files.
**Reading** relies on [`qvd` (qvdrs)](https://github.com/bintocher/qvdrs)
(a **streaming** reader with bounded memory); **writing** relies on
[OpenQVD](https://github.com/Sigilweaver/OpenQVD) (fine-grained tag control).

```sql
SELECT * FROM read_qvd('sales.qvd');
COPY (SELECT * FROM sales) TO 'output.qvd' (FORMAT qvd);
COPY sales FROM 'output.qvd' (FORMAT qvd);
```

## Project status

**Reading is functional, verified in live DuckDB (v1.5.3).** The extension
registers itself, declares the `read_qvd(VARCHAR)` table function, and reads
QVD files in **streaming** mode via `qvdrs`: a DuckDB type is inferred per field
(at `bind`), then data is decoded **in chunks of 2048 rows** (`open_qvd_stream`/
`next_chunk`), one file open at a time. Memory is **bounded** (≈ symbol tables +
one chunk), regardless of the number of rows — no more materializing all rows.
The `DATE`/`BIGINT`/`VARCHAR` columns behave like real SQL types (filters,
`EXTRACT`, aggregates). The logic is isolated in [`src/qvd.rs`](src/qvd.rs) and
covered by tests (`cargo test --lib`).

```sql
SELECT id, price FROM read_qvd('sales.qvd') WHERE price > 100;
```

### Type mapping

The DuckDB type is inferred from the **Qlik tags** in the header (read at `bind`,
without decoding the data — which enables projection pushdown), with a fallback
on the `<Type>` element:

| Qlik tag (`<Type>` fallback) | DuckDB type |
|------------------------------|-------------|
| `$date`                      | `DATE`      |
| `$timestamp` (without `$date`)  | `TIMESTAMP` |
| `$time`                      | `TIME`      |
| `$interval`                  | `INTERVAL`  |
| `$text` / `$ascii`           | `VARCHAR`   |
| `$integer`                   | `BIGINT`    |
| `$numeric` (without `$integer`) | `DOUBLE`    |

Tags are a more reliable signal than `<Type>` (often `UNKNOWN` even for real
dates). The Qlik serial number (epoch 1899-12-30) is converted to a native
`DATE`/`TIMESTAMP`. Verified on real data: serial `33765` → `1992-06-10`.
`NULL`s are preserved.

### Projection pushdown

`supports_pushdown()` is enabled: DuckDB only requests the useful columns, and
only those are decoded via `Qvd::from_path_projected`. The `bind` reads only the
header; data decoding happens at `init`, restricted to the projected columns.
`SELECT count(*)` decodes no column.

### Multi-file glob

```sql
SELECT * FROM read_qvd('data/sales_*.qvd');
```

The pattern is expanded (sorted) and rows from all files are **concatenated**.
The schema is that of the **first file**; within each file fields are resolved
**by name** (robust to column-ordering differences), with a missing field coming
out as `NULL`. A pattern with no match raises an error.

### Writing: `COPY ... TO ... (FORMAT qvd)`

```sql
COPY (SELECT id, amount, sale_date FROM sales) TO 'export.qvd' (FORMAT qvd);
```

Implemented via a **copy function** of the C API (not wrapped by duckdb-rs,
driven in FFI in [`src/copy.rs`](src/copy.rs)): `bind` (types) →
`global_init` (path) → `sink` (accumulation) → `finalize` (OpenQVD write).
Types are preserved and tagged for reading back (`BIGINT`/`DOUBLE`/`VARCHAR`/
`DATE`/`TIMESTAMP`), `NULL` and UTF-8 are kept. Verified in a round-trip
`read_qvd` → `COPY` → `read_qvd`.

**Column names.** The C API of the copy function does not expose column names:
by default the fields are `field0`, `field1`, … They can be supplied explicitly
via the `FIELD_NAMES` option (both syntaxes are accepted):

```sql
COPY (SELECT id, amount, day FROM sales)
  TO 'export.qvd' (FORMAT qvd, FIELD_NAMES (id, amount, day));
-- or:  FIELD_NAMES ['id', 'amount', 'day']
```

The number of names must equal the number of columns (otherwise an error).

Write limitations:
- Types written: BOOLEAN/TINYINT/SMALLINT/INTEGER/BIGINT/FLOAT/DOUBLE/`DECIMAL`/
  VARCHAR/DATE/TIMESTAMP/TIME/INTERVAL (the `DECIMAL` is converted to `DOUBLE`,
  as the QVD format allows). The others (e.g. `HUGEINT`, unsigned types) require
  a `CAST`.
- `INTERVAL`s with a **month** component are approximated to 30 days/month
  (the QVD format has no notion of months).

### Import: `COPY table FROM 'file.qvd' (FORMAT qvd)`

```sql
CREATE TABLE sales(name VARCHAR, sale_date DATE, total BIGINT);
COPY sales FROM 'sales.qvd' (FORMAT qvd);
```

In the C API, `COPY ... FROM` delegates to a **table function**; since duckdb-rs
does not expose the raw `duckdb_table_function` of a `VTab`, it is built in FFI
in [`src/copy_from.rs`](src/copy_from.rs) and reuses the reading/typing of
`read_qvd`. All types (including `DATE`/`TIME`/`INTERVAL`) and `NULL`s are
restored; round-trip `COPY TO` → `COPY FROM` verified.

(Equivalent to `INSERT INTO sales SELECT * FROM read_qvd('sales.qvd')`.)

### Known limitations (future improvements)

- Typing driven by Qlik tags; a QVD without tags falls back to `<Type>` and then
  `VARCHAR` by default (files produced by Qlik are always tagged).
- **Streaming read with bounded memory** (≈ symbol tables + one chunk).
  However, `qvdrs` decodes **all** the symbol tables in the file (no projection
  at the symbol level); projection is applied at emission.
- **Writing** (`COPY TO`) still accumulates all rows in memory (OpenQVD writer).
- Local glob only (no DuckDB file system: neither httpfs nor S3).

## Performance (reading)

Measured on `flights.QVD` (**460 MB, 10 million rows, ~50 columns**),
DuckDB v1.5.3, **release builds**, peak RSS via `/usr/bin/time -l` (macOS,
Apple Silicon). Comparison between the current version (`qvdrs`, streaming) and
the previous one (OpenQVD, everything materialized):

| Query | OpenQVD (before) | `qvdrs` (streaming) |
|---|---|---|
| `SELECT count(*)` | 2 s · **1520 MB** | 7 s · **37 MB** |
| `SELECT count(DISTINCT Origin)` | 3 s · **1445 MB** | 8 s · **45 MB** |
| `SELECT max(Distance)` | 2 s · **1440 MB** | 8 s · **38 MB** |

**Memory: ~30–40× less (≈1.4 GB → ~40 MB, −97%).** The old version loaded the
entire file into RAM (OpenQVD by cloning the bytes) then materialized all rows —
a roughly constant cost ≈1.4 GB regardless of the query, **growing with the file
size**. Streaming stays **bounded** (symbol tables + one chunk), independent of
the number of rows; the gap widens further on larger files.

**Time: ~3× slower.** The accepted trade-off of streaming. The overhead comes
mainly from the *eager* decoding of **all** symbol tables by `qvdrs`
(traversing the index in chunks is itself fast) — a future optimization avenue.

## Structure

| File | Role |
|---|---|
| `src/lib.rs` | C-API entrypoint + `read_qvd` VTab (DuckDB plumbing, streaming) |
| `src/qvd.rs` | `qvdrs` streaming read: schema, typing, chunked scan, conversions + tests |
| `src/wasm_lib.rs` | Re-export for the Wasm target (staticlib) |
| `Cargo.toml` | Dependencies (`duckdb`, + `openqvd`/`arrow` to enable) |
| `Makefile` | C-API build (`make debug`/`release`) + fast cargo targets |
| `test/sql/read_qvd.test` | SQLLogicTest smoke test |

## Build

### Fast iteration (native library)

```sh
make cargo-build        # or: cargo build
```

> Building the `loadable-extension` feature of the `duckdb` crate generates
> bindings and may require **libclang** (bindgen) installed on the machine.

### Loadable extension (`.duckdb_extension`)

This project uses the official DuckDB **community build template** from
[`extension-ci-tools`](https://github.com/duckdb/extension-ci-tools), vendored as
a **git submodule pinned to the `v1.5.3` branch** (matching `TARGET_DUCKDB_VERSION`)
— the same flow the community-extensions infrastructure runs. Clone with
`git clone --recurse-submodules …`, or initialise the submodule after the fact:

```sh
make bootstrap          # git submodule update --init --recursive (once)
make configure          # creates a Python venv (duckdb + sqllogictest runner)
make debug              # → build/debug/qvd.duckdb_extension
# make release          # → build/release/qvd.duckdb_extension
```

`make debug` compiles the library and appends the metadata footer via
`append_extension_metadata.py`. For the C_STRUCT_UNSTABLE ABI, DuckDB requires
the stamped version to match the host exactly: `TARGET_DUCKDB_VERSION` is
**v1.5.3** (the current community version).

Loading (unsigned extension):

```sh
duckdb -unsigned -c "LOAD 'build/debug/qvd.duckdb_extension'; \
  SELECT * FROM read_qvd('QVD-Examples/Ventes.qvd');"
```

> A standalone script `./scripts/build-extension.sh` also produces a
> `build/qvd.duckdb_extension` without a venv (it stamps against the locally
> installed `duckdb` version), handy for quick local checks.

## Tests

```sh
make test                   # SQLLogicTest suite against the built extension
cargo +1.95.0 test --lib    # Rust unit test: generates a QVD then reads it back
```

`make test` runs the `test/sql/*.test` files through the community SQLLogicTest
runner (requires `make configure && make debug` first). The Rust test (in
[`src/qvd.rs`](src/qvd.rs)) covers integers, floats, text, `NULL` and a "dual"
field typed as DATE. `--lib` targets the lib (the Wasm "example" has a distinct
module constraint).

To test on real QVDs: drop files into `test/data/` and adapt
[`test/sql/read_qvd.test`](test/sql/read_qvd.test).

## Roadmap

- [x] Reading `read_qvd('file.qvd')` with BIGINT/DOUBLE/VARCHAR typing + NULL.
- [x] Native temporal types `DATE`/`TIMESTAMP`/`TIME`/`INTERVAL` (Qlik serials converted).
- [x] Projection pushdown via `from_path_projected` (only useful columns decoded).
- [x] Glob `read_qvd('data/*.qvd')` (concatenated rows, resolution by name).
- [x] Writing `COPY ... TO ... (FORMAT qvd)` (FFI copy function; round-trip verified).
- [x] Preserve column names on write (`FIELD_NAMES` option).
- [x] Writing `DECIMAL`s (all widths i16/i32/i64/i128 → DOUBLE).
- [x] Writing `TIME`/`INTERVAL` (full temporal round-trip verified).
- [x] Import `COPY table FROM 'x.qvd'` (FFI table function; round-trip verified).

## License

Apache-2.0, like OpenQVD.
