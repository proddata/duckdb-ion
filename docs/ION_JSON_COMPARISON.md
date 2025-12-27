# Ion vs JSON: Implementation Comparison

This note summarizes how `read_ion` (Ion extension) differs from `read_json` (DuckDB JSON extension) and where
the biggest optimization opportunities likely sit. It focuses on the ingestion path used in the perf suite.

## Parsing & Data Flow
- `read_json` uses `yyjson` to parse JSONL buffers into a DOM and then extracts typed values in batches.
- `read_ion` uses `ion-c`’s streaming reader to walk Ion values/fields and materialize values row-by-row.
- JSON parsing benefits from tight loops over already-parsed DOM nodes; Ion parsing currently does more per-value
  reader calls and branching.

## Schema Inference
- JSON inference (`read_json`) builds a candidate structure tree and refines types across samples.
- Ion inference (`read_ion`) inspects up to a fixed number of rows (default 1000) and merges types with a
  promotion heuristic (e.g., DECIMAL vs DOUBLE vs VARCHAR).

## Projection & Field Lookup
- JSON has native projection pushdown in its reader path and can skip irrelevant fields while parsing.
- Ion uses a field-name lookup per struct field, with a SID cache to accelerate repeated field names.
- An experimental extractor path exists for very small projections, but it still routes through value parsing.

## Type Materialization
- JSON transforms `yyjson` numeric and string types directly into vectors without round-trips through string
  formatting, and applies casting rules inline.
- Ion currently converts DECIMAL via `ion_decimal_to_string` + `TryCastToDecimal`, which is correct but slower.
- Both paths write into DuckDB vectors, but Ion does more per-row `Value` construction and branching.

## Parallelism & I/O
- JSON reader uses a buffered scan with line/object parsing; the perf suite uses JSONL with good parallelism.
- Ion parallel scans are only enabled for newline-delimited formats and are not currently used in the baseline
  comparisons.
- Bytes read are similar for comparable inputs; gaps are dominated by parse and materialization costs.

## Behavioral Differences
- Ion supports typed values (decimals, timestamps, blobs, symbols) that do not have JSON equivalents.
- JSON allows unstructured ingestion into `JSON` type when inference conflicts; Ion currently promotes to
  `VARCHAR` as a last resort.

## Improvement Options
- **Reduce struct traversal cost:** minimize `step_in/step_out` and per-field overhead; treat this as the primary
  bottleneck based on profiling.
- **Batch key mapping:** emulate JSON's per-batch key map (name -> column index) to avoid repeated string lookups.
- **SID-driven projection:** eagerly map projected column names to SIDs (when available) and skip string compares.
- **Wider extractor usage:** expand ion-c extractor usage beyond <=3 columns, or mirror its logic for larger
  projections.
- **Skip logic:** ensure `SkipIonValue` is efficient so non-projected fields are cheap to ignore.
- **Vectorized materialization:** avoid per-row `Value` creation; write directly into vectors whenever possible.
- **Decimal/timestamp fast path:** keep binary-to-decimal conversions in-memory to avoid string casts.
- **Parallel scans:** tune newline-delimited parallelism and chunk sizes once parsing hot spots are reduced.

## Profiling Findings (ION_PROFILE)
- Type mix aligns with dataset: `int`, `string`, `decimal` counts match expected rows; value conversion cost is
  not dominating.
- `struct_ms` is consistently the largest bucket, far larger than `value_ms` in text Ion.
- Binary Ion dramatically reduces `struct_ms`, reinforcing that text traversal/name resolution is the hotspot.

## Ion Extractor Findings (2025-02)
- A standalone repro in `scripts/ion_extractor_repro.cpp` shows callbacks fire when the reader is **before first value**
  at depth 0 (ion-c expects `ion_extractor_match` to drive `ion_reader_next` internally).
- If the reader is already positioned on a value (`ion_reader_next` called) or stepped into a struct (depth 1),
  callbacks do **not** fire, even with `match_relative_paths = true`.
- A zero-length path at depth>0 does fire, but only as a **match-all**; field-level filtering must then happen in the
  callback, which removes most pruning benefit.
- `read_ion` steps into structs before reading fields, so extractor matching is unreliable beyond depth 0.
- Extractor usage is disabled by default; it can be forced for experiments via `use_extractor := true`.

## Next Implementation Sketch
- **Phase 1: Field lookup fast path**
  - Build a per-scan hash map from field name -> column index (similar to JSON key map) for projections.
  - Prefer SID lookups; cache name-to-col misses to avoid repeated string work.
- **Phase 2: Extractor expansion**
  - Consider enabling ion-c extractor for larger projected column sets only at depth 0 (before-first reader state).
  - Avoid relying on extractor for nested paths; use key map + SID lookup or a two-pass approach.
- **Phase 3: Batch transform**
  - Accumulate per-column arrays of Ion values per batch and materialize in vectorized loops.
  - Align conversion logic with JSON's `TransformObject` pattern for predictable costs.

## Recent Insights (Perf)
- Text Ion is still ~3–4x slower than JSON on CPU with much larger latency deltas; overhead is likely in per-row
  materialization and conversions rather than I/O.
- Binary Ion narrows the gap or beats JSON on CPU for several queries, but latency remains higher, suggesting
  remaining overhead outside the parser.
- Wide-schema wins for binary Ion indicate text parsing cost dominates in that scenario.
