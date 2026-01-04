#!/usr/bin/env python3
import argparse
import json
from datetime import datetime


def load_rows(path):
    rows = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def pick_run(rows, run_ts):
    if run_ts:
        return [row for row in rows if row.get("run_ts") == run_ts]
    # Fall back to max run_ts (ISO) if present.
    by_ts = {}
    for row in rows:
        ts = row.get("run_ts")
        if ts:
            by_ts.setdefault(ts, []).append(row)
    if not by_ts:
        return rows
    latest = max(by_ts.keys(), key=lambda val: datetime.fromisoformat(val.replace("Z", "+00:00")))
    return by_ts[latest]


def build_index(rows):
    index = {}
    for row in rows:
        key = (row.get("suite"), row.get("label"))
        index[key] = row
    return index


def compare_metric(metric, baseline, current, threshold):
    base_val = baseline.get(metric)
    curr_val = current.get(metric)
    if base_val is None or curr_val is None:
        return None
    if base_val == 0:
        return None
    ratio = curr_val / base_val
    if ratio > 1.0 + threshold:
        return ratio
    return None


def main():
    parser = argparse.ArgumentParser(description="Compare perf runs against a baseline.")
    parser.add_argument("--baseline", required=True, help="Baseline summary.jsonl")
    parser.add_argument("--current", required=True, help="Current run summary.jsonl")
    parser.add_argument("--run-ts", default="", help="Filter current run to a specific run_ts")
    parser.add_argument("--cpu-threshold", type=float, default=0.2, help="Allowed CPU regression fraction")
    parser.add_argument("--lat-threshold", type=float, default=0.2, help="Allowed latency regression fraction")
    args = parser.parse_args()

    baseline_rows = load_rows(args.baseline)
    current_rows = pick_run(load_rows(args.current), args.run_ts)

    baseline_index = build_index(baseline_rows)
    current_index = build_index(current_rows)

    regressions = []
    missing = []
    for key, base in baseline_index.items():
        if "ion_cpu" not in base or "ion_latency" not in base:
            continue
        curr = current_index.get(key)
        if not curr:
            missing.append(key)
            continue
        suite, _ = key
        suite_lower = (suite or "").lower()
        cpu_reg = compare_metric("ion_cpu", base, curr, args.cpu_threshold)
        # Latency includes IO and tends to be noisy for write benchmarks; only gate on CPU there.
        if "write" in suite_lower:
            lat_reg = None
        else:
            lat_reg = compare_metric("ion_latency", base, curr, args.lat_threshold)
        if cpu_reg or lat_reg:
            regressions.append((key, cpu_reg, lat_reg, base, curr))

    if missing:
        print("Missing current perf rows for:")
        for suite, label in missing:
            print(f"- {suite} / {label}")
        print()

    if regressions:
        print("Perf regression detected:")
        for (suite, label), cpu_reg, lat_reg, base, curr in regressions:
            parts = [f"{suite} / {label}"]
            if cpu_reg:
                parts.append(f"cpu {curr['ion_cpu']:.3f}s vs {base['ion_cpu']:.3f}s ({cpu_reg:.2f}x)")
            if lat_reg:
                parts.append(f"lat {curr['ion_latency']:.3f}s vs {base['ion_latency']:.3f}s ({lat_reg:.2f}x)")
            print("- " + "; ".join(parts))
        raise SystemExit(1)

    print("Perf check passed.")


if __name__ == "__main__":
    main()
