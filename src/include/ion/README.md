# `src/include/ion/`

This directory holds headers for the Ion extension that are shared across multiple translation units.

Implementation lives under `src/ion/` (bind/infer/scan/value/register plus copy/serialize helpers). The top-level
entrypoint `src/ion_extension.cpp` is intentionally thin and only wires registration into DuckDB.
