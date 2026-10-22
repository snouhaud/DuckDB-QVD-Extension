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

**Reading is functional, verified in live DuckDB (v2.0.0).** The extension
registers itself, declares the `read_qvd(VARCHAR)` table function, and reads QVD
files in **streaming projected** mode via `qvdrs`' low-level primitives: a DuckDB
type is inferred per field from the header alone (at `bind`), then — for the
**projected columns only** — their symbol tables (dictionary of distinct values)
are decoded and each symbol is converted to a typed value once. The bit-packed
index block is read **in chunks of 2048 rows** and, for each record, only the
**projected fields' bits** are extracted (`read_field_index`); rows are emitted by
index lookup, one file open at a time. Memory is **bounded** (≈ projected symbol
tables + one chunk of index bytes), regardless of the number of rows — no
materializing, no per-row reconversion, and **work scales with the number of
projected columns, not the total**.
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
`DATE`/`TIMESTAMP`. Verified on real data: serial `33765` → `1992-06-10`. For
`DATE`, the fractional part (time of day) is truncated, not rounded.
`NULL`s are preserved.

Headers are untrusted input: section sizes, symbol counts and bit positions are
checked against the file size before anything is allocated, so a corrupt or
crafted QVD fails with an error instead of exhausting memory or crashing the
host process.

### Projection pushdown

`supports_pushdown()` is enabled: DuckDB only requests the useful columns, and
only those are read and decoded. The `bind` reads only the header; data decoding
happens at scan time, restricted to the projected columns. `SELECT count(*)`
decodes a single column: DuckDB always requests at least one from a table
function.

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
`DATE`/`TIMESTAMP`/`TIME`/`INTERVAL`; `DECIMAL` becomes `DOUBLE`), `NULL` and
UTF-8 are kept. Verified in a round-trip
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

QVD fields map to the table's columns **by position**, and each column must have
the type `read_qvd` infers for its field (the C API cannot convert values here):
a mismatch is a bind error. To convert, use
`INSERT INTO sales SELECT * FROM read_qvd('sales.qvd')`, where DuckDB casts.

### Known limitations (future improvements)

- Typing driven by Qlik tags; a QVD without tags falls back to `<Type>` and then
  `VARCHAR` by default (files produced by Qlik are always tagged).
- **Streaming projected read with bounded memory** (≈ projected symbol tables +
  one chunk of index bytes). Only the **projected** columns' symbol tables are read
  and decoded, and only the projected fields' bits are extracted per record — work
  scales with the projection, not the total column count. The index block's
  **bytes** are still read in full (fields are bit-interleaved in each record — a
  QVD format trait).
- **Writing** (`COPY TO`) still accumulates all rows in memory (OpenQVD writer).
- Local glob only (no DuckDB file system: neither httpfs nor S3).

## Important limitations and behaviour notes

### Writing memory usage

`COPY ... TO ... (FORMAT qvd)` accumulates **all rows in memory** before writing
(`src/copy.rs`). This is a limitation of the underlying OpenQVD writer, not of
DuckDB. For small and medium files this is fine, but for very large outputs you
should expect memory consumption proportional to the total row count × column
count. If you hit memory limits, split the output into smaller chunks (for
example with `LIMIT`/`OFFSET` or by partition) or export to another format.

### Column names in `COPY ... TO`

The DuckDB C copy-function API does **not expose column names**, so by default
the QVD fields are named `field0`, `field1`, `field2`, … regardless of the source
column names. Use the `FIELD_NAMES` option to preserve meaningful names. Both
syntaxes are accepted:

```sql
COPY (SELECT id, amount, day FROM sales)
  TO 'export.qvd' (FORMAT qvd, FIELD_NAMES (id, amount, day));

COPY (SELECT id, amount, day FROM sales)
  TO 'export.qvd' (FORMAT qvd, FIELD_NAMES ['id', 'amount', 'day']);
```

A common pattern is to query the column names from the result metadata and build
the option dynamically. This works for a table, a CTE, or any query expression:

```sql
-- build the field-name array from the output schema of the query
SELECT list(column_name)
FROM (DESCRIBE SELECT * FROM sales);
-- → [id, amount, sale_date]

-- then use that list in the COPY statement
COPY (SELECT * FROM sales)
  TO 'sales.qvd' (FORMAT qvd, FIELD_NAMES (SELECT list(column_name)
                                           FROM (DESCRIBE SELECT * FROM sales)));
```

You can use the same pattern with arbitrary expressions (CTEs, joins, computed
columns, aliases). Just make sure the inner query inside `COPY` and the query
inside `DESCRIBE` produce the **same columns in the same order**:

```sql
WITH enriched AS (
    SELECT
        s.id,
        s.amount,
        c.category,
        s.sale_date,
        s.amount * 0.2 AS vat_amount
    FROM sales s
    JOIN categories c ON c.id = s.category_id
)
COPY (SELECT * FROM enriched)
  TO 'enriched_sales.qvd' (
    FORMAT qvd,
    FIELD_NAMES (SELECT list(column_name) FROM (DESCRIBE SELECT * FROM enriched))
  );
```

The number of names must exactly match the number of output columns, otherwise
the statement errors out.

### Precision of `DECIMAL` and `INTERVAL`

- `DECIMAL` is written as `DOUBLE` because the QVD format does not store a
  fixed-point decimal type. Values are converted with full `f64` precision, but
  the round-trip is `DECIMAL` → `DOUBLE`. If exact decimal precision is
  required, cast explicitly before writing or choose another format.

- `INTERVAL` with a **month** component is approximated as **30 days per month**
  when writing to QVD, because the Qlik serial format has no month notion. For
  example, `INTERVAL '1 month'` is stored as 30 days and reads back as such.
  Month-bearing intervals are therefore not perfectly round-trippable.

## Performance (reading)

Measured on `flights.QVD` (**439 MB, 10 million rows, 49 columns**),
DuckDB v1.5.4, **release builds**, warm page cache, wall time + peak RSS via
`/usr/bin/time -l` (macOS, Apple Silicon). The **projected** reader (current)
extracts only the projected fields' bits per record; the previous reader decoded
**all 49 columns'** indices for every row regardless of the projection:

| Query | all-columns index decode (before) | **projected (current)** | speedup |
|---|---|---|---|
| `SELECT sum(DepDelay)` (1 col) | 2.9 s · 31 MB | **0.18 s · 29 MB** | **16×** |
| `SELECT count(*)` | 3.0 s · 31 MB | **0.10 s · 29 MB** | **30×** |
| 3 text cols `count(DISTINCT …)` | 4.2 s · 39 MB | **1.6 s · 37 MB** | 2.7× |
| 49 columns (`max(COLUMNS(*))`) | 10.7 s | 10.2 s | ≈ same |

**16–30× faster on narrow projections** (the common analytical case), same
bounded memory (~29–37 MB), and no regression on full-table reads. The former
bottleneck was decoding all 49 columns' bit-fields per row (≈ 490 M extractions
for 10 M rows) even for a single-column query; the projected reader extracts only
what is asked for. Symbol-table decoding turned out to be negligible (~0.01 s).
These figures predate reading only the projected symbol tables (#6), which
further cuts I/O and memory on wide files.

## Structure

| File                           | Role                                                                                            |
| ------------------------------ | ----------------------------------------------------------------------------------------------- |
| `src/lib.rs`                   | C-API entrypoint + `read_qvd` VTab (DuckDB plumbing, streaming)                                 |
| `src/qvd.rs`                   | QVD format: header validation/typing, projected symbol + index decode, conversions + unit tests |
| `src/copy.rs`                  | `COPY ... TO (FORMAT qvd)`: copy function (raw FFI), OpenQVD writer                             |
| `src/copy_from.rs`             | `COPY ... FROM (FORMAT qvd)`: copy-from table function (raw FFI)                                |
| `src/wasm_lib.rs`              | Re-export for the Wasm target (staticlib)                                                       |
| `Cargo.toml`                   | Dependencies (`duckdb`, `qvd`/qvdrs, `openqvd`, `glob`)                                         |
| `Makefile`                     | C-API build (`make debug`/`release`) + fast cargo targets                                       |
| `test/sql/*.test`              | SQLLogicTest suites (`read_qvd`, `COPY TO/FROM`)                                                |
| `docs/stable-abi-migration.md` | Why the extension uses the stable C ABI                                                         |
| `scripts/build-extension.sh`   | Standalone build without the Makefile/venv                                                      |

## Build

### Fast iteration (native library)

```sh
make cargo-build        # or: cargo build
```

### Loadable extension (`.duckdb_extension`)

This project uses the official DuckDB **community build template** from
[`extension-ci-tools`](https://github.com/duckdb/extension-ci-tools), vendored as
a **git submodule pinned to the `v2.0-cyanoptera` branch** (matching the DuckDB version of the `duckdb` crate)
— the same flow the community-extensions infrastructure runs. Clone with
`git clone --recurse-submodules …`, or initialise the submodule after the fact:

```sh
make bootstrap          # git submodule update --init --recursive (once)
make configure          # creates a Python venv (duckdb + sqllogictest runner)
make debug              # → build/debug/qvd.duckdb_extension
# make release          # → build/release/qvd.duckdb_extension
```

`make debug` compiles the library and appends the metadata footer via
`append_extension_metadata.py`. The extension uses the **stable C ABI**
(`C_STRUCT`): the stamped `TARGET_DUCKDB_VERSION` is the minimum V1 C API
version, **v1.5.6**, so one binary loads on DuckDB 1.5.6 and 2.0.x (see
[`docs/stable-abi-migration.md`](docs/stable-abi-migration.md)).

Loading (unsigned extension):

```sh
duckdb -unsigned -c "LOAD 'build/debug/qvd.duckdb_extension'; \
  SELECT * FROM read_qvd('QVD-Examples/Ventes.qvd');"
```

> A standalone script `./scripts/build-extension.sh` also produces a
> `build/qvd.duckdb_extension` (debug, stable ABI, V1 C API v1.5.6) without the
> Makefile/venv, handy for quick local checks.

## Tests

```sh
make test                   # SQLLogicTest suite against the built extension
cargo +1.97.1 test --lib    # Rust unit test: generates a QVD then reads it back
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
- [x] Projection pushdown (only the projected columns' symbols and bits are read).
- [x] Glob `read_qvd('data/*.qvd')` (concatenated rows, resolution by name).
- [x] Writing `COPY ... TO ... (FORMAT qvd)` (FFI copy function; round-trip verified).
- [x] Preserve column names on write (`FIELD_NAMES` option).
- [x] Writing `DECIMAL`s (all widths i16/i32/i64/i128 → DOUBLE).
- [x] Writing `TIME`/`INTERVAL` (full temporal round-trip verified).
- [x] Import `COPY table FROM 'x.qvd'` (FFI table function; round-trip verified).

## License

Apache-2.0, like OpenQVD.
