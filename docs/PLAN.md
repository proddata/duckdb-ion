# Ion Extension POC Plan

## Goal
Deliver a first proof-of-concept DuckDB extension that reads AWS Ion (text and binary), supports newline-delimited variants, preserves types where available, and provides a basic write path. Performance should be in the same ballpark as DuckDB’s JSON ingestion for comparable workloads.

## Non-Goals (Initial POC)
- Full Ion feature coverage (e.g., annotations or symbol tables beyond defaults).
- Maximum performance tuning; focus on correctness first.
- Cross-language client bindings.

## Milestone 1: Reader Spike (Ion Text)
1. Select an Ion parsing library (prefer vcpkg integration).
2. Build a minimal table function to read `.ion` files in text form.
3. Map core Ion types to DuckDB types:
   - null, bool, int, float, string, timestamp.
4. Add basic SQLLogicTests for text input.

## Milestone 2: Schema Inference + ND-Ion
1. Implement schema inference with clear type promotion rules.
2. Support newline-delimited Ion where each line is a value or struct.
3. Document supported input shapes (scalar vs struct).

## Milestone 3: Binary Ion Support
1. Extend the reader to binary Ion.
2. Add fixtures and SQLLogicTests for both formats.
3. Validate type fidelity across text vs binary.

## Milestone 4: Writer POC
1. Implement a minimal write/export path to Ion text.
2. Add round-trip tests from DuckDB tables to Ion and back.

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
- `struct` -> `STRUCT` when keys are stable; otherwise a single `VARCHAR`/`JSON` column.

## Deliverables Checklist
- Reader table function for text and binary Ion.
- ND-Ion support.
- Basic schema inference and type mapping docs.
- Minimal writer/export path.
- SQLLogicTests covering representative files.
