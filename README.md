# DuckDB Ion Extension

[![Main Extension Distribution Pipeline](https://github.com/proddata/duckdb-ion/actions/workflows/MainDistributionPipeline.yml/badge.svg)](https://github.com/proddata/duckdb-ion/actions/workflows/MainDistributionPipeline.yml)
![GitHub License](https://img.shields.io/github/license/proddata/duckdb-ion)


This repository implements a DuckDB extension for reading and writing AWS Ion data.


## Parameters
`read_ion` accepts a single file path, a glob (e.g., `test/ion/*.ion`), or a list of paths.
- `columns`: struct of `name: 'SQLTYPE'` pairs; skips inference and uses that schema. Nested types are supported (e.g., `STRUCT(name VARCHAR)` or `INTEGER[]`).
- `format`: `'auto'` (default), `'newline_delimited'`, or `'array'`.
- `records`: `'auto'` (default), `'true'`, `'false'`, or a BOOLEAN.
- `maximum_depth`: maximum nested depth to infer; `-1` means unlimited.
- `field_appearance_threshold`: when inferring structs, if average field appearance falls below this threshold, infer `MAP` instead of `STRUCT`.
- `map_inference_threshold`: if a struct has at least this many fields and types are similar, infer `MAP`; `-1` disables map inference.
- `sample_size`: number of values to sample for schema inference; `-1` means sample all input.
- `maximum_sample_files`: cap on files sampled during schema inference; `-1` removes the cap.
- `union_by_name`: when reading multiple files, infer the schema from all files instead of the first file only.
- `conflict_mode`: `'varchar'` (default) or `'json'` to map conflicting fields to JSON (auto-loads json when available).
- `use_extractor`: BOOLEAN (default false). Experimental and currently disabled due to incorrect results; do not enable.
- You can combine `format` with `records` (e.g., `format := 'array', records := 'false'`). `columns` requires `records=true`.
- When input structs have different fields, the schema is the union of field names and missing fields are returned as NULL.
- Type conflicts are promoted across rows (e.g., INT + DOUBLE → DOUBLE, mixed types → VARCHAR, nested fields are merged).

## Ion Type Mapping
For background on Ion types and encodings, see the AWS Ion documentation: https://amazon-ion.github.io/ion-docs/

| Ion type | DuckDB type | Notes |
| --- | --- | --- |
| null | NULL | Typed nulls follow the target column type when one is specified. |
| bool | BOOLEAN |  |
| int | BIGINT |  |
| float | DOUBLE |  |
| decimal | DECIMAL(18,3) | Uses DuckDB's default DECIMAL width/scale unless `columns` overrides it. |
| timestamp | TIMESTAMPTZ |  |
| string | VARCHAR |  |
| symbol | VARCHAR | Symbols are read as strings. |
| clob | VARCHAR |  |
| blob | BLOB |  |
| list | LIST | Element type inferred; mixed types promote to VARCHAR. |
| struct | STRUCT | Field union with NULLs for missing fields. |

Unsupported or not preserved yet:
- Ion annotations are not preserved.
- Ion symbol tables are not exposed.
- S-expressions (sexp) are not supported.

## Development
Build, test, and local run instructions live in `DEVELOPMENT.md`.

## Usage Examples
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
SELECT nested.score, list_extract(nums, 1)
FROM read_ion('test/ion/conflicts.ion', conflict_mode := 'json');
```

## Writing Ion
`COPY ... TO` with `FORMAT ION` writes Ion text (newline‑delimited structs). Add `BINARY TRUE` to write Ion binary.
```sql
COPY (SELECT 1 AS a, 'x' AS b) TO 'out.ion' (FORMAT ION);
```
Binary output:
```sql
COPY (SELECT 1 AS a, 'x' AS b) TO 'out.ion' (FORMAT ION, BINARY TRUE);
```
Notes:
- Binary output currently normalizes timestamps to UTC (`Z`) and ignores source timezone offsets.
  This is intended for interoperability but should be considered when round-tripping TIMESTAMPTZ values.
To wrap output in a single Ion list:
```sql
COPY (SELECT 1 AS a UNION ALL SELECT 2 AS a) TO 'out.ion' (FORMAT ION, ARRAY TRUE);
```

### DuckDB Type Mapping (Write)
`to_ion` and text output follow the mappings below; binary output writes the native Ion type when available.

| DuckDB type | Ion type (text) | Ion type (binary) | Notes |
| --- | --- | --- | --- |
| BOOLEAN | bool | bool |  |
| INT/UINT (all widths) | int | int |  |
| FLOAT/DOUBLE | float | float |  |
| DECIMAL | decimal | decimal |  |
| DATE | string | timestamp | Text uses `YYYY-MM-DD`; binary uses day precision. |
| TIMESTAMP/TIMESTAMPTZ | timestamp | timestamp | Text uses `T` separator and `Z` when no timezone is present. |
| VARCHAR/CHAR | string | string |  |
| BLOB | blob | blob | Text uses base64 inside `{{...}}`. |
| LIST | list | list |  |
| STRUCT | struct | struct | Field names are written as Ion symbols. |
| other | string | string | Falls back to `value.ToString()`. |

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
If you are loading a local or custom build, you may need to allow unsigned extensions:

```sh
% duckdb --unsigned
```

```sql
LOAD '/<path_to_extension>/ion.duckdb_extension';
```

## Third-party licenses

This project includes **Amazon Ion-C**, licensed under the Apache License 2.0.

Copyright © 2009–2016 Amazon.com, Inc. or its affiliates.

See:
- `LICENSES/Apache-2.0.txt`
- `LICENSES/ion-c-NOTICE.txt`
