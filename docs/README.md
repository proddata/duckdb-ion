# DuckDB Ion Extension

This repository implements a DuckDB extension for reading (and eventually writing) AWS Ion data. It is based on the DuckDB extension template (`docs/TEMPLATE_README.md`) and is intended for eventual distribution via community extensions.

## Status
- `read_ion(path)` is implemented using the Ion C library and currently supports scalar Ion values (null, bool, int, float, string/symbol/clob).
- Complex types (list/struct) and binary Ion support are planned next.

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
SELECT ion FROM read_ion('test/ion/sample.ion');
```

## Tests
SQLLogicTests live in `test/sql`:
```sh
make test
```

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
