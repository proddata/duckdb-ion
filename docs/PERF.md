# Performance Baseline (read_ion vs read_json)

## Goal
Establish a repeatable baseline comparing `read_ion` to `read_json` on equivalent data and queries, without changing implementation yet.

## Step 1: Prepare comparable datasets
- Use the same schema and record count for Ion (newline‑delimited structs) and JSONL.
- If needed, export from DuckDB once and convert:
  - `COPY (SELECT * FROM my_table) TO 'data.ion' (FORMAT ION);`
  - `COPY (SELECT * FROM my_table) TO 'data.jsonl' (FORMAT JSON);`
- Or use the helper script to generate both:
  - `ROWS=100000 OUT_DIR=perf/data ./scripts/perf_generate.sh`
  - Re-run this script whenever serialization rules change.

## Step 2: Run standardized queries
Pick at least three queries and run each on both formats:
1. Full scan count
   - `SELECT COUNT(*) FROM read_ion('data.ion');`
   - `SELECT COUNT(*) FROM read_json('data.jsonl');`
2. Projection
   - `SELECT col1, col2 FROM read_ion('data.ion');`
   - `SELECT col1, col2 FROM read_json('data.jsonl');`
3. Filter + aggregate
   - `SELECT col1, COUNT(*) FROM read_ion('data.ion') WHERE col2 > 10 GROUP BY col1;`
   - `SELECT col1, COUNT(*) FROM read_json('data.jsonl') WHERE col2 > 10 GROUP BY col1;`

## Step 3: Capture profiling output
Use DuckDB’s built‑in profiling:
```sql
PRAGMA enable_profiling='json';
PRAGMA profiling_output='ion_profile.json';
SELECT COUNT(*) FROM read_ion('data.ion');

PRAGMA profiling_output='json_profile.json';
SELECT COUNT(*) FROM read_json('data.jsonl');
```
Repeat for the other queries (use distinct profiling_output files).

## Step 4: Record baseline numbers
For each query, capture:
- wall time (from client or shell timing)
- rows/sec (if your client reports it)
- top operators from profiling output

## Step 5: Identify hotspots
Common hotspots to look for:
- per‑value conversions and casts
- recursive list/struct parsing
- string conversion for timestamps/decimals/blobs

## Step 6: Keep a log
Keep a simple table (spreadsheet or markdown) with:
- dataset size + row count
- query name
- read_ion time / read_json time
- notes on hotspots from profiling
You can run the standard set with:
- `DATA_DIR=perf/data OUT_DIR=perf/results ./scripts/perf_run.sh`

## Potential Improvements
- Enable projection pushdown to skip parsing unselected columns (mirrors `read_json` behavior).
- Batch-transform Ion values into vectors to reduce per-row `Value` allocations and casts.
- Cache field-to-column mappings beyond per-field SID lookup (e.g., stable field order heuristics).
- Consider `ION_READER_OPTIONS.skip_character_validation` for trusted data.
- Avoid string round-trips for decimals/timestamps by decoding Ion primitives directly.
- Parallelize newline-delimited scans to better utilize threads.

## Parallel Scan Notes
- Parallel scans currently only apply when `format='newline_delimited'` and the file is seekable.
- Use `PRAGMA threads=<n>;` to force multiple threads during benchmarks.
