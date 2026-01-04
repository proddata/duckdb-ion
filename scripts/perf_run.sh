#!/usr/bin/env bash
set -euo pipefail

DUCKDB_BIN="${DUCKDB_BIN:-./build/release/duckdb}"
DATA_DIR="${DATA_DIR:-perf/data}"
OUT_DIR="${OUT_DIR:-perf/results}"
ION_PROFILE="${ION_PROFILE:-0}"
ND_RUNS="${ND_RUNS:-3}"
RUN_TS="${RUN_TS:-$(date -u +"%Y-%m-%dT%H:%M:%SZ")}"
GIT_SHA="${GIT_SHA:-$(git rev-parse --short HEAD 2>/dev/null || echo unknown)}"
export RUN_TS GIT_SHA ND_RUNS

ION_FILE="$DATA_DIR/data.ion"
JSON_FILE="$DATA_DIR/data.jsonl"
ION_BINARY_FILE="$DATA_DIR/data_binary.ion"
ION_WIDE_FILE="$DATA_DIR/data_wide.ion"
JSON_WIDE_FILE="$DATA_DIR/data_wide.jsonl"
ION_WIDE_BINARY_FILE="$DATA_DIR/data_wide_binary.ion"

mkdir -p "$OUT_DIR"

HAS_ION_BINARY=1
if [[ ! -f "$ION_BINARY_FILE" ]]; then
  echo "perf_run: skipping binary read benchmarks (missing $ION_BINARY_FILE)" >&2
  HAS_ION_BINARY=0
fi

HAS_ION_WIDE_BINARY=1
if [[ ! -f "$ION_WIDE_BINARY_FILE" ]]; then
  echo "perf_run: skipping wide binary read benchmarks (missing $ION_WIDE_BINARY_FILE)" >&2
  HAS_ION_WIDE_BINARY=0
fi

ION_PROFILE_ARG=""
if [[ "$ION_PROFILE" == "1" ]]; then
  ION_PROFILE_ARG=", profile := true"
fi

ION_ND_PARALLEL_SQL=""
JSON_ND_PARALLEL_SQL=""
for i in $(seq 1 "$ND_RUNS"); do
  ION_ND_PARALLEL_SQL+="PRAGMA threads=4;
PRAGMA profiling_output='$OUT_DIR/ion_count_nd_parallel_${i}.json';
SELECT COUNT(*)
FROM read_ion('$ION_FILE', format := 'newline_delimited'${ION_PROFILE_ARG});

PRAGMA profiling_output='$OUT_DIR/ion_project_min_nd_parallel_${i}.json';
SELECT id
FROM read_ion('$ION_FILE', format := 'newline_delimited'${ION_PROFILE_ARG});

PRAGMA threads=1;

"
  JSON_ND_PARALLEL_SQL+="PRAGMA threads=4;
PRAGMA profiling_output='$OUT_DIR/json_count_nd_parallel_${i}.json';
SELECT COUNT(*)
FROM read_json('$JSON_FILE');

PRAGMA profiling_output='$OUT_DIR/json_project_min_nd_parallel_${i}.json';
SELECT id
FROM read_json('$JSON_FILE');

PRAGMA threads=1;

"
done

ION_COUNT_BINARY_SQL=""
ION_PROJECT_BINARY_SQL=""
ION_COUNT_WIDE_BINARY_SQL=""
ION_PROJECT_WIDE_BINARY_SQL=""
if [[ "$HAS_ION_BINARY" == "1" ]]; then
  ION_COUNT_BINARY_SQL="PRAGMA profiling_output='$OUT_DIR/ion_count_binary.json';
SELECT COUNT(*) FROM read_ion('$ION_BINARY_FILE'${ION_PROFILE_ARG});"
  ION_PROJECT_BINARY_SQL="PRAGMA profiling_output='$OUT_DIR/ion_project_binary.json';
SELECT id, category, amount::DOUBLE FROM read_ion('$ION_BINARY_FILE'${ION_PROFILE_ARG});"
fi
if [[ "$HAS_ION_WIDE_BINARY" == "1" ]]; then
  ION_COUNT_WIDE_BINARY_SQL="PRAGMA profiling_output='$OUT_DIR/ion_count_wide_binary.json';
SELECT COUNT(*) FROM read_ion('$ION_WIDE_BINARY_FILE'${ION_PROFILE_ARG});"
  ION_PROJECT_WIDE_BINARY_SQL="PRAGMA profiling_output='$OUT_DIR/ion_project_wide_binary.json';
SELECT id, w_int_00, w_str_00, w_dec_00
FROM read_ion('$ION_WIDE_BINARY_FILE'${ION_PROFILE_ARG});"
fi

SQL_FILE="$OUT_DIR/run_perf.sql"
cat > "$SQL_FILE" <<SQL
INSTALL json;
LOAD json;
LOAD ion;

PRAGMA enable_profiling='json';

-- Parallel newline-delimited sub-suite first to reduce noise from later wide scans + writes.
${ION_ND_PARALLEL_SQL}
${JSON_ND_PARALLEL_SQL}

CREATE OR REPLACE TEMP TABLE perf_source AS
SELECT * FROM read_ion('$ION_FILE');
CREATE OR REPLACE TEMP TABLE perf_wide AS
SELECT * FROM read_ion('$ION_WIDE_FILE');

PRAGMA profiling_output='$OUT_DIR/ion_count.json';
SELECT COUNT(*) FROM read_ion('$ION_FILE'${ION_PROFILE_ARG});

${ION_COUNT_BINARY_SQL}

PRAGMA profiling_output='$OUT_DIR/ion_count_explicit.json';
SELECT COUNT(*) FROM read_ion(
  '$ION_FILE',
  columns := {
    id: 'BIGINT',
    category: 'VARCHAR',
    amount: 'DOUBLE',
    flag: 'BOOLEAN',
    ts: 'TIMESTAMP',
    nested: 'STRUCT(sub_id BIGINT, sub_name VARCHAR)',
    tags: 'VARCHAR[]'
  }${ION_PROFILE_ARG}
);

PRAGMA profiling_output='$OUT_DIR/json_count.json';
SELECT COUNT(*) FROM read_json('$JSON_FILE');

PRAGMA profiling_output='$OUT_DIR/ion_project.json';
SELECT id, category, amount::DOUBLE FROM read_ion('$ION_FILE'${ION_PROFILE_ARG});

${ION_PROJECT_BINARY_SQL}

PRAGMA profiling_output='$OUT_DIR/ion_project_explicit.json';
SELECT id, category, amount::DOUBLE
FROM read_ion(
  '$ION_FILE',
  columns := {
    id: 'BIGINT',
    category: 'VARCHAR',
    amount: 'DOUBLE',
    flag: 'BOOLEAN',
    ts: 'TIMESTAMP',
    nested: 'STRUCT(sub_id BIGINT, sub_name VARCHAR)',
    tags: 'VARCHAR[]'
  }${ION_PROFILE_ARG}
);

PRAGMA profiling_output='$OUT_DIR/json_project.json';
SELECT id, category, amount::DOUBLE FROM read_json('$JSON_FILE');

PRAGMA profiling_output='$OUT_DIR/ion_project_min.json';
SELECT id FROM read_ion('$ION_FILE'${ION_PROFILE_ARG});

PRAGMA profiling_output='$OUT_DIR/ion_project_min_explicit.json';
SELECT id
FROM read_ion(
  '$ION_FILE',
  records := true,
  columns := {
    id: 'BIGINT',
    category: 'VARCHAR',
    amount: 'DOUBLE',
    flag: 'BOOLEAN',
    ts: 'TIMESTAMP',
    nested: 'STRUCT(sub_id BIGINT, sub_name VARCHAR)',
    tags: 'VARCHAR[]'
  }${ION_PROFILE_ARG}
);

PRAGMA profiling_output='$OUT_DIR/ion_project_min_extractor.json';
SELECT id
FROM read_ion(
  '$ION_FILE',
  records := true,
  columns := {
    id: 'BIGINT',
    category: 'VARCHAR',
    amount: 'DOUBLE',
    flag: 'BOOLEAN',
    ts: 'TIMESTAMP',
    nested: 'STRUCT(sub_id BIGINT, sub_name VARCHAR)',
    tags: 'VARCHAR[]'
  }${ION_PROFILE_ARG}
);

PRAGMA profiling_output='$OUT_DIR/json_project_min.json';
SELECT id FROM read_json('$JSON_FILE');

PRAGMA profiling_output='$OUT_DIR/ion_filter_agg.json';
SELECT category, COUNT(*)
FROM read_ion('$ION_FILE'${ION_PROFILE_ARG})
WHERE amount::DOUBLE > 5
GROUP BY category;

PRAGMA profiling_output='$OUT_DIR/ion_filter_agg_explicit.json';
SELECT category, COUNT(*)
FROM read_ion(
  '$ION_FILE',
  columns := {
    id: 'BIGINT',
    category: 'VARCHAR',
    amount: 'DOUBLE',
    flag: 'BOOLEAN',
    ts: 'TIMESTAMP',
    nested: 'STRUCT(sub_id BIGINT, sub_name VARCHAR)',
    tags: 'VARCHAR[]'
  }${ION_PROFILE_ARG}
)
WHERE amount::DOUBLE > 5
GROUP BY category;

PRAGMA profiling_output='$OUT_DIR/json_filter_agg.json';
SELECT category, COUNT(*)
FROM read_json('$JSON_FILE')
WHERE amount::DOUBLE > 5
GROUP BY category;

PRAGMA profiling_output='$OUT_DIR/ion_count_wide.json';
SELECT COUNT(*) FROM read_ion('$ION_WIDE_FILE'${ION_PROFILE_ARG});

${ION_COUNT_WIDE_BINARY_SQL}

PRAGMA profiling_output='$OUT_DIR/json_count_wide.json';
SELECT COUNT(*) FROM read_json('$JSON_WIDE_FILE');

PRAGMA profiling_output='$OUT_DIR/ion_project_wide.json';
SELECT id, w_int_00, w_str_00, w_dec_00
FROM read_ion('$ION_WIDE_FILE'${ION_PROFILE_ARG});

${ION_PROJECT_WIDE_BINARY_SQL}

PRAGMA profiling_output='$OUT_DIR/json_project_wide.json';
SELECT id, w_int_00, w_str_00, w_dec_00
FROM read_json('$JSON_WIDE_FILE');

PRAGMA profiling_output='$OUT_DIR/ion_write_text.json';
COPY perf_source TO '$OUT_DIR/write_text.ion' (FORMAT ION, OVERWRITE TRUE);

PRAGMA profiling_output='$OUT_DIR/ion_write_binary.json';
COPY perf_source TO '$OUT_DIR/write_binary.ion' (FORMAT ION, BINARY TRUE, OVERWRITE TRUE);

PRAGMA profiling_output='$OUT_DIR/json_write_text.json';
COPY perf_source TO '$OUT_DIR/write_text.jsonl' (FORMAT JSON, OVERWRITE TRUE);

PRAGMA profiling_output='$OUT_DIR/ion_write_text_wide.json';
COPY perf_wide TO '$OUT_DIR/write_text_wide.ion' (FORMAT ION, OVERWRITE TRUE);

PRAGMA profiling_output='$OUT_DIR/ion_write_binary_wide.json';
COPY perf_wide TO '$OUT_DIR/write_binary_wide.ion' (FORMAT ION, BINARY TRUE, OVERWRITE TRUE);

PRAGMA profiling_output='$OUT_DIR/json_write_text_wide.json';
COPY perf_wide TO '$OUT_DIR/write_text_wide.jsonl' (FORMAT JSON, OVERWRITE TRUE);
SQL

"$DUCKDB_BIN" < "$SQL_FILE"

python3 - <<'PY'
import json
import os

out_dir = os.environ.get("OUT_DIR", "perf/results")
summary_path = os.path.join(out_dir, "summary.md")
summary_jsonl = os.path.join(out_dir, "summary.jsonl")
run_ts = os.environ.get("RUN_TS", "unknown")
git_sha = os.environ.get("GIT_SHA", "unknown")
nd_runs = int(os.environ.get("ND_RUNS", "3"))
summary_lines = []
jsonl_rows = []

def load(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_median(paths, key):
    vals = []
    for p in paths:
        if not os.path.exists(p):
            continue
        j = load(p)
        if key in j and j[key] is not None:
            vals.append(j[key])
    if not vals:
        return None
    vals.sort()
    return vals[len(vals) // 2]

def fmt(val):
    return f"{val:.3f}s"

def ratio(a, b):
    if b == 0:
        return "n/a"
    return f"{a / b:.2f}x"

def summarize_pairs(title, pairs, replicate_pattern=False):
    print(title)
    print("Query | Ion CPU | JSON CPU | CPU Ratio | Ion Lat | JSON Lat | Lat Ratio")
    print("--- | --- | --- | --- | --- | --- | ---")
    summary_lines.append(f"## {title}")
    summary_lines.append("Query | Ion CPU | JSON CPU | CPU Ratio | Ion Lat | JSON Lat | Lat Ratio")
    summary_lines.append("--- | --- | --- | --- | --- | --- | ---")
    for label, ion_file, json_file in pairs:
        if replicate_pattern:
            ion_paths = [
                os.path.join(out_dir, ion_file.replace(".json", f"_{i}.json"))
                for i in range(1, nd_runs + 1)
            ]
            json_paths = [
                os.path.join(out_dir, json_file.replace(".json", f"_{i}.json"))
                for i in range(1, nd_runs + 1)
            ]
            ion_cpu = load_median(ion_paths, "cpu_time")
            json_cpu = load_median(json_paths, "cpu_time")
            ion_lat = load_median(ion_paths, "latency")
            json_lat = load_median(json_paths, "latency")
            if ion_cpu is None or json_cpu is None or ion_lat is None or json_lat is None:
                print(f"Skipping {label} (missing replicated {ion_file} or {json_file})")
                continue
            ion = {"cpu_time": ion_cpu, "latency": ion_lat}
            js = {"cpu_time": json_cpu, "latency": json_lat}
        else:
            ion_path = os.path.join(out_dir, ion_file)
            json_path = os.path.join(out_dir, json_file)
            if not os.path.exists(ion_path) or not os.path.exists(json_path):
                print(f"Skipping {label} (missing {ion_file} or {json_file})")
                continue
            ion = load(ion_path)
            js = load(json_path)
        jsonl_rows.append({
            "run_ts": run_ts,
            "git_sha": git_sha,
            "suite": title,
            "label": label,
            "ion_file": ion_file,
            "json_file": json_file,
            "ion_cpu": ion["cpu_time"],
            "json_cpu": js["cpu_time"],
            "cpu_ratio": ion["cpu_time"] / js["cpu_time"] if js["cpu_time"] else None,
            "ion_latency": ion["latency"],
            "json_latency": js["latency"],
            "latency_ratio": ion["latency"] / js["latency"] if js["latency"] else None,
        })
        line = (
            f"{label} | {fmt(ion['cpu_time'])} | {fmt(js['cpu_time'])} | {ratio(ion['cpu_time'], js['cpu_time'])} | "
            f"{fmt(ion['latency'])} | {fmt(js['latency'])} | {ratio(ion['latency'], js['latency'])}"
        )
        print(line)
        summary_lines.append(line)
    print()
    summary_lines.append("")

def summarize_binary(title, pairs):
    print(title)
    print("Query | Text CPU | Binary CPU | Speedup | Text Lat | Binary Lat | Speedup")
    print("--- | --- | --- | --- | --- | --- | ---")
    summary_lines.append(f"## {title}")
    summary_lines.append("Query | Text CPU | Binary CPU | Speedup | Text Lat | Binary Lat | Speedup")
    summary_lines.append("--- | --- | --- | --- | --- | --- | ---")
    for label, text_file, bin_file in pairs:
        text_path = os.path.join(out_dir, text_file)
        bin_path = os.path.join(out_dir, bin_file)
        if not os.path.exists(text_path) or not os.path.exists(bin_path):
            print(f"Skipping {label} (missing {text_file} or {bin_file})")
            continue
        text = load(text_path)
        binary = load(bin_path)
        jsonl_rows.append({
            "run_ts": run_ts,
            "git_sha": git_sha,
            "suite": title,
            "label": label,
            "text_file": text_file,
            "binary_file": bin_file,
            "text_cpu": text["cpu_time"],
            "binary_cpu": binary["cpu_time"],
            "cpu_ratio": text["cpu_time"] / binary["cpu_time"] if binary["cpu_time"] else None,
            "text_latency": text["latency"],
            "binary_latency": binary["latency"],
            "latency_ratio": text["latency"] / binary["latency"] if binary["latency"] else None,
        })
        line = (
            f"{label} | {fmt(text['cpu_time'])} | {fmt(binary['cpu_time'])} | {ratio(text['cpu_time'], binary['cpu_time'])} | "
            f"{fmt(text['latency'])} | {fmt(binary['latency'])} | {ratio(text['latency'], binary['latency'])}"
        )
        print(line)
        summary_lines.append(line)
    print()
    summary_lines.append("")

summarize_pairs(
    "Ion vs JSON (text)",
    [
        ("COUNT(*)", "ion_count.json", "json_count.json"),
        ("Project 3 cols", "ion_project.json", "json_project.json"),
        ("Filter + group", "ion_filter_agg.json", "json_filter_agg.json"),
        ("Wide project (4 cols)", "ion_project_wide.json", "json_project_wide.json"),
    ],
)

summarize_pairs(
    "Ion vs JSON (parallel newline-delimited)",
    [
        ("COUNT(*)", "ion_count_nd_parallel.json", "json_count_nd_parallel.json"),
        ("Project min", "ion_project_min_nd_parallel.json", "json_project_min_nd_parallel.json"),
    ],
    replicate_pattern=True,
)

summarize_pairs(
    "Ion binary vs JSON (text)",
    [
        ("COUNT(*)", "ion_count_binary.json", "json_count.json"),
        ("Project 3 cols", "ion_project_binary.json", "json_project.json"),
        ("Wide project (4 cols)", "ion_project_wide_binary.json", "json_project_wide.json"),
    ],
)

summarize_binary(
    "Ion text vs binary",
    [
        ("COUNT(*)", "ion_count.json", "ion_count_binary.json"),
        ("Project 3 cols", "ion_project.json", "ion_project_binary.json"),
        ("Wide COUNT(*)", "ion_count_wide.json", "ion_count_wide_binary.json"),
        ("Wide project (4 cols)", "ion_project_wide.json", "ion_project_wide_binary.json"),
    ],
)

summarize_pairs(
    "Ion vs JSON (write)",
    [
        ("Write", "ion_write_text.json", "json_write_text.json"),
        ("Write wide", "ion_write_text_wide.json", "json_write_text_wide.json"),
    ],
)

summarize_binary(
    "Ion write text vs binary",
    [
        ("Write", "ion_write_text.json", "ion_write_binary.json"),
        ("Write wide", "ion_write_text_wide.json", "ion_write_binary_wide.json"),
    ],
)

with open(summary_path, "w", encoding="utf-8") as f:
    f.write("# Performance Summary\n\n")
    f.write("\n".join(summary_lines).rstrip())
    f.write("\n")

if jsonl_rows:
    with open(summary_jsonl, "a", encoding="utf-8") as f:
        for row in jsonl_rows:
            f.write(json.dumps(row) + "\n")

print(f"Wrote profiling output to {out_dir}")
print(f"Wrote summary to {summary_path}")
print(f"Appended summary to {summary_jsonl}")
PY
