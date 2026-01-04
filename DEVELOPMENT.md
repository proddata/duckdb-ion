# Development

## Dependencies
This extension uses vcpkg for dependencies (ion-c) when available, and falls back to FetchContent if `IonC` is not found.
Ensure `VCPKG_ROOT` points to your vcpkg checkout and that you have built it (`./bootstrap-vcpkg.sh`).

```sh
CCACHE_DISABLE=1 VCPKG_TOOLCHAIN_PATH="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" \
VCPKG_TARGET_TRIPLET="$(uname -m)-osx" make
```

The repo uses a local vcpkg overlay in `vcpkg_ports/`.

## Build
```sh
make
```

## Pre-commit Checklist
Run these before committing changes:
```sh
# Formatting (requires black/clang-format/cmake-format; see repo guidelines)
make format-check

# Lint (requires clang-tidy)
make tidy-check

# Tests
make test
```

## Project Notes
- This repo uses a local vcpkg overlay in `vcpkg_ports/`.
- `GEN=ninja make` is supported and recommended for faster rebuilds.
- Formatter tooling follows DuckDB conventions; use `make format` / `make format-check` when touching `src/` or `test/`.

### Build Outputs
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/ion/ion.duckdb_extension
```

## Updating Submodules
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

## Tests
SQLLogicTests live in `test/sql`:
```sh
make test
```

## Community Extension Notes
DuckDB community extensions are built and distributed from the Community Extension repository, not this repo.
Before submitting:
- Ensure CI is green (tests, perf checks, mem checks).
- Keep README up to date with current limitations (experimental flags, binary behavior).
- Verify the extension loads and installs via `INSTALL ion FROM community;` once published.
Community extension docs: https://community-extensions.duckdb.org

## Fixtures
Binary test fixtures can be regenerated with:
```sh
python3 scripts/generate_binary_ion_fixtures.py
```
Notes:
- CI does not run this script; generated `*_binary.ion` files should be committed.
- The script needs the ion-c text-to-binary tool; set `ION_TEXT_TO_BINARY` or install ion-c via vcpkg so it can build `build/ion_text_to_binary`.

## Performance Notes
Current parsing builds `Value` objects for each Ion value and casts per row. This is functional but not optimized.
If you want a quick baseline, use a large file and measure a full scan:
```sql
SELECT COUNT(*) FROM read_ion('path/to/large.ion');
```
Likely hotspots to optimize later: nested list/struct parsing, string conversions, and per-row casting.
