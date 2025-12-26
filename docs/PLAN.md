# Ion Extension POC Plan

## Change Log
- 2025-02-15: Added `read_ion` with schema inference, nested struct/list support, `format`/`records`/`columns` options, and binary fixtures/tests.

## Goal
Deliver a first proof-of-concept DuckDB extension that reads AWS Ion (text and binary), supports newline-delimited variants, preserves types where available, and provides a basic write path. Performance should be in the same ballpark as DuckDB’s JSON ingestion for comparable workloads.

## Non-Goals (Initial POC)
- Full Ion feature coverage (e.g., annotations or symbol tables beyond defaults).
- Maximum performance tuning; focus on correctness first.
- Cross-language client bindings.

## Milestone 1: Reader Spike (Ion Text)
1. Select an Ion parsing library (prefer vcpkg integration). (done: ion-c via vcpkg)
2. Build a minimal table function to read `.ion` files in text form. (done)
3. Map core Ion types to DuckDB types. (done; includes decimal, blob)
4. Add basic SQLLogicTests for text input. (done)

## Milestone 2: Schema Inference + ND-Ion
1. Implement schema inference with clear type promotion rules. (done; recursive for structs/lists)
2. Support newline-delimited Ion where each line is a value or struct. (done via `records` option)
3. Document supported input shapes (scalar vs struct). (done in README)
4. Add explicit schema support (`columns`) for nested types. (done)

## Milestone 3: Binary Ion Support
1. Extend the reader to binary Ion. (done; IVM auto-detection via ion-c)
2. Add fixtures and SQLLogicTests for both formats. (done; binary scalars/structs/array)
3. Validate type fidelity across text vs binary. (in progress)

## Milestone 4: Writer POC
1. Implement a minimal write/export path to Ion text. (done; COPY TO ION)
2. Add round-trip tests from DuckDB tables to Ion and back. (done; basic COPY TO + read_ion)

## Next Up
1. Validate binary vs text type fidelity with more nested fixtures.
2. Decide how to represent annotations and symbol tables (ignore vs column).
3. Design a minimal write path (newline-delimited structs first).
4. Add writer options (pretty printing, timestamp format) when needed.

## Open Questions
- Which Ion library provides stable C/C++ APIs and licensing suitable for community extensions?
- How should Ion annotations be represented (ignore, preserve as metadata, or map to columns)?
- What is the preferred mapping for Ion types with no direct DuckDB equivalent?

## Candidate Ion Libraries
- `ion-c` (Amazon Ion C, native C API; likely best fit for C++ extension interop).
- `ion-cpp` (C++ wrapper, depends on `ion-c`).
- `ion-java` (reference implementation, not ideal for this extension).

## Initial Type Mapping Rules (POC)
- `null` -> `NULL` (typed nulls map to target column type during inference).
- `bool` -> `BOOLEAN`.
- `int` -> `BIGINT` (promote to `HUGEINT` if out of range).
- `float`/`decimal` -> `DOUBLE` (or `DECIMAL` if scale/precision provided).
- `string`/`symbol` -> `VARCHAR`.
- `timestamp` -> `TIMESTAMP` (preserve timezone if available).
- `blob`/`clob` -> `BLOB` (or `VARCHAR` for clob if needed).
- `list` -> `LIST<T>` when homogeneous; otherwise `LIST<VARCHAR>` as fallback.
- `struct` -> `STRUCT` with unioned fields; missing fields become NULL.

## Deliverables Checklist
- Reader table function for text and binary Ion. (done)
- ND-Ion support + `format`/`records` options. (done)
- Basic schema inference and type mapping docs. (done)
- Nested struct/list support. (done)
- Minimal writer/export path. (todo)
- SQLLogicTests covering representative files. (done)
