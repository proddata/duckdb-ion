# Repository Guidelines

## Project Goal & Scope
This repository is a DuckDB extension template fork. The original template README is preserved as `docs/TEMPLATE_README.md`. This repo now focuses on an Ion extension (no legacy quack example). The target is a community DuckDB extension that can:
- Read AWS Ion files in text and binary formats, including newline-delimited variants.
- Preserve and map Ion types where available (avoid lossy string-only ingestion).
- Provide performance comparable to DuckDB’s JSON ingestion where feasible.
- Write Ion files back out with correct typing.

The intent is to ship via the DuckDB community extensions repository once stable. The detailed POC plan lives in `docs/PLAN.md`.

## Project Structure & Module Organization
- `src/` holds extension implementation (C++), with headers in `src/include/`.
- `test/sql/` contains SQLLogicTests (e.g., `test/sql/ion_read.test`).
- `docs/` includes build and maintenance notes.
- `duckdb/` and `extension-ci-tools/` are submodules required for building/testing.
- `build/` is the default output directory for binaries and test runners.

## Build, Test, and Development Commands
```sh
make
```
Builds the extension and DuckDB binaries (release by default).

When using vcpkg dependencies (like ion-c), run:
```sh
CCACHE_DISABLE=1 VCPKG_TOOLCHAIN_PATH="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" \
VCPKG_TARGET_TRIPLET="$(uname -m)-osx" make
```
This repo uses a local vcpkg overlay under `vcpkg_ports/`.

```sh
GEN=ninja make
```
Builds using Ninja (recommended with ccache for faster rebuilds).

```sh
make test
make test_debug
```
Runs SQLLogicTests in `test/sql` against release or debug builds.

```sh
make format
make format-check
```
Applies or checks DuckDB’s formatter on `src/` and `test/`.
If formatter deps are missing, create a local venv and install them:
```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install "black>=24" clang_format==11.0.1 cmake-format
PATH="$(pwd)/.venv/bin:$PATH" make format-check
```

## Coding Style & Naming Conventions
- Follow existing DuckDB C++ style in `src/` (tabs for indentation, aligned braces).
- Run the formatter before submitting changes: `make format`.
- Keep extension symbols and filenames aligned with the extension name (`ion`).

## Testing Guidelines
- Prefer SQLLogicTests in `test/sql/` for new functionality.
- Name test files with the `.test` suffix and keep cases focused and minimal.
- Run at least `make test` for PRs that change behavior.

## Commit & Pull Request Guidelines
- Git history is minimal (single “Initial commit”); no strict convention yet.
- Use concise, imperative messages (e.g., “Add ion reader tests”).
- PRs should describe changes, note test commands run, and link relevant issues.
- If updating submodules, mention the pinned commit in the PR description.

## Dependencies & Submodules
- `duckdb/` provides core build/test infrastructure; keep it updated when needed.
- Optional dependency management uses VCPKG; see `docs/README.md` for setup.
