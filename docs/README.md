# DuckDB Ion Extension

[![Main Extension Distribution Pipeline](https://github.com/proddata/duckdb-ion/actions/workflows/MainDistributionPipeline.yml/badge.svg)](https://github.com/proddata/duckdb-ion/actions/workflows/MainDistributionPipeline.yml)
![GitHub License](https://img.shields.io/github/license/proddata/duckdb-ion)


This repository implements a DuckDB extension for reading and writing AWS Ion data. It is based on the DuckDB extension template (`docs/TEMPLATE_README.md`) and is intended for eventual distribution via community extensions.

## Status
- `read_ion(path)` reads newline-delimited Ion structs and maps fields to columns using schema inference.
- `read_ion(path, columns := {field: 'TYPE', ...})` uses an explicit schema and skips inference.
- `read_ion(path, records := false)` reads scalar values into a single `ion` column.
- `read_ion(path, format := 'array')` reads from a top-level Ion list (array) instead of top-level values.
- Nested Ion structs and lists are supported and map to DuckDB `STRUCT` and `LIST` types.
- Binary Ion is supported when input starts with the Ion version marker.

## Parameters
`read_ion` accepts a single file path, a glob (e.g., `test/ion/*.ion`), or a list of paths.
- `columns`: struct of `name: 'SQLTYPE'` pairs; skips inference and uses that schema. Nested types are supported (e.g., `STRUCT(name VARCHAR)` or `INTEGER[]`).
- `format`: `'auto'` (default), `'newline_delimited'`, `'array'`, or `'unstructured'`.
- `records`: `'auto'` (default), `'true'`, `'false'`, or a BOOLEAN.
- `maximum_depth`: maximum nested depth to infer; `-1` means unlimited.
- `field_appearance_threshold`: when inferring structs, if average field appearance falls below this threshold, infer `MAP` instead of `STRUCT`.
- `map_inference_threshold`: if a struct has at least this many fields and types are similar, infer `MAP`; `-1` disables map inference.
- `sample_size`: number of values to sample for schema inference; `-1` means sample all input.
- `maximum_sample_files`: cap on files sampled during schema inference; `-1` removes the cap.
- `union_by_name`: when reading multiple files, infer the schema from all files instead of the first file only.
- `use_extractor`: BOOLEAN (default `false`). Experimental projection path using ion-c extractors; currently unreliable for record structs and intended for debugging only.
- You can combine `format` with `records` (e.g., `format := 'array', records := 'false'`). `columns` requires `records=true`.
- When input structs have different fields, the schema is the union of field names and missing fields are returned as NULL.
- Type conflicts are promoted across rows (e.g., INT + DOUBLE → DOUBLE, mixed types → VARCHAR, nested fields are merged).
- Ion DECIMAL values are inferred as DOUBLE unless an explicit `columns` schema is provided.
- `use_extractor` note: ion-c extractors currently only fire callbacks at depth 0 in our repro; when `read_ion`
  steps into a struct (depth 1), callbacks do not fire even with `match_relative_paths=true`. Use only for
  experiments; see `docs/ION_JSON_COMPARISON.md` for details.

## Building
### Dependencies
This extension uses vcpkg for dependencies (ion-c) when available, and falls back to FetchContent if `IonC` is not found.
Ensure `VCPKG_ROOT` points to your vcpkg checkout and that you have built it (`./bootstrap-vcpkg.sh`).

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

### Updating Submodules
DuckDB extensions use two submodules that are included in your forked extension repo when you use the `--recurse-submodules` flag. These modules are:

| Name                  | Repository                                      | Description |
|-----------------------|-------------------------------------------------|-------------|
| duckdb                | https://github.com/duckdb/duckdb                | This repository contains core DuckDB code required for building extensions.            |
| extension-ci-tools    | https://github.com/duckdb/extension-ci-tools    | This repository contains reusable components for building, testing and deploying DuckDB extensions.            |


> [!IMPORTANT]  
> It is recommended that you update your submodules at least once every other major LTS release to avoid CI/CD pipeline build errors caused by remaining pinned to a stale commit of these submodules.

To update all submodules to the latest commit hash:
```bash
git submodule update --init --recursive
```

To update your submodules to a specific commit hash, for example to update duckdb to the hash `8e146474d7adb960c5a2941142fe4482cc7dfc08`:
```bash
cd duckdb 
git fetch --all
git checkout 8e146474d7adb960c5a2941142fe4482cc7dfc08   # or any tag/branch/commit hash
cd ..
git add duckdb
git commit -m "Pin DuckDB submodule to cc7dfc08"
git push HEAD:update-submodule-branch
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
- `DATE` values are serialized as `YYYY-MM-DD`.
- `TIMESTAMP`/`TIMESTAMPTZ` values are serialized with a `T` separator and a `Z` suffix when no timezone is present.
- Struct field names are written as Ion symbols (bare when valid identifiers, otherwise single‑quoted).

## Installing (Unsigned)
If you are loading a local or custom build, you may need `allow_unsigned_extensions`:
```sql
SET allow_unsigned_extensions=true;
INSTALL ion;
LOAD ion;
```

For custom repositories:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/ion/latest';
INSTALL ion;
LOAD ion;
```
