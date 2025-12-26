# DuckDB Ion Extension

This repository implements a DuckDB extension for reading (and eventually writing) AWS Ion data. It is based on the DuckDB extension template (`docs/TEMPLATE_README.md`) and is intended for eventual distribution via community extensions.

## Status
- `read_ion(path)` reads newline-delimited Ion structs and maps fields to columns using schema inference.
- `read_ion(path, columns := {field: 'TYPE', ...})` uses an explicit schema and skips inference.
- `read_ion(path, records := 'false')` reads scalar values into a single `ion` column.
- `read_ion(path, format := 'array')` reads from a top-level Ion list (array) instead of top-level values.
- Nested Ion structs and lists are supported and map to DuckDB `STRUCT` and `LIST` types.
- Binary Ion is supported when input starts with the Ion version marker.
- Complex types (list/struct) and richer binary support are planned next.

## Parameters
- `columns`: struct of `name: 'SQLTYPE'` pairs; skips inference and uses that schema. Nested types are supported (e.g., `STRUCT(name VARCHAR)` or `INTEGER[]`).
- `format`: `'auto'` (default), `'newline_delimited'`, `'array'`, or `'unstructured'`.
- `records`: `'auto'` (default), `'true'`, `'false'`, or a BOOLEAN.
- You can combine `format` with `records` (e.g., `format := 'array', records := 'false'`). `columns` requires `records=true`.
- When input structs have different fields, the schema is the union of field names and missing fields are returned as NULL.
- Type conflicts are promoted across rows (e.g., INT + DOUBLE → DOUBLE, mixed types → VARCHAR, nested fields are merged).

## Building
### Dependencies
This extension uses vcpkg for dependencies (ion-c, OpenSSL). Ensure `VCPKG_ROOT` points to your vcpkg checkout and that you have built it (`./bootstrap-vcpkg.sh`).

```sh
CCACHE_DISABLE=1 VCPKG_TOOLCHAIN_PATH="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" \
VCPKG_TARGET_TRIPLET="$(uname -m)-osx" make
```

The repo uses a local vcpkg overlay in `vcpkg_ports/`.

### Build Outputs
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/ion/ion.duckdb_extension
```

## Running
```sh
./build/release/duckdb
```

Example:
```sql
SELECT bigint, varchar, bool FROM read_ion('test/ion/sample.ion');
SELECT bigint, varchar, bool
FROM read_ion('test/ion/sample.ion', columns := {bigint: 'BIGINT', varchar: 'VARCHAR', bool: 'BOOLEAN'});
SELECT ion FROM read_ion('test/ion/scalars.ion', records := false);
SELECT id,
       nested.name,
       list_extract(tags, 1)
FROM read_ion('test/ion/nested.ion',
              columns := {id: 'VARCHAR',
                          nested: 'STRUCT(name VARCHAR, score BIGINT)',
                          tags: 'VARCHAR[]'});
SELECT bigint, varchar, bool
FROM read_ion('test/ion/array.ion', format := 'array');
SELECT name FROM read_ion('test/ion/structs_binary.ion');
SELECT name FROM read_ion('test/ion/array_binary.ion', format := 'array');
```

Binary test fixtures can be regenerated with:
```sh
python3 scripts/generate_binary_ion_fixtures.py
```

## Performance Notes
Current parsing builds `Value` objects for each Ion value and casts per row. This is functional but not optimized.
If you want a quick baseline, use a large file and measure a full scan:
```sql
SELECT COUNT(*) FROM read_ion('path/to/large.ion');
```
Likely hotspots to optimize later: nested list/struct parsing, string conversions, and per‑row casting.

## Tests
SQLLogicTests live in `test/sql`:
```sh
make test
```

## Writing Ion
Use `COPY ... TO` with `FORMAT ION` to export newline‑delimited Ion structs:
```sql
COPY (SELECT 1 AS a, 'x' AS b) TO 'out.ion' (FORMAT ION);
```
To wrap output in a single Ion list:
```sql
COPY (SELECT 1 AS a UNION ALL SELECT 2 AS a) TO 'out.ion' (FORMAT ION, ARRAY TRUE);
```

### to_ion
Use `to_ion` to serialize a value (including structs/lists) into Ion text:
```sql
SELECT to_ion(struct_pack(a := 1, b := ['x', 'y']));
```
Notes:
- `BLOB` values are emitted as Ion blobs using base64 inside `{{...}}`.
- `DATE`/`TIMESTAMP`/`TIMESTAMPTZ` values are serialized using DuckDB’s default string representation (with a `T` separator).

## Installing (Unsigned)
If you are loading a local or custom build, you may need `allow_unsigned_extensions`:
```sql
INSTALL ion;
LOAD ion;
```

For custom repositories:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/ion/latest';
INSTALL ion;
LOAD ion;
```
