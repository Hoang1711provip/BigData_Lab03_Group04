import csv
import json
import sys
from collections import Counter
from datetime import datetime
from decimal import Decimal
from pathlib import Path


ev = Path(sys.argv[1])


def read_jsonl(path):
    with path.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def incoming_is_better(s, t):
    st = timestamp(s["ingest_time_utc"])
    tt = timestamp(t["ingest_time_utc"])

    if st != tt:
        return st > tt

    return (
        s["source_object"], int(s["source_row"])
    ) < (
        t["source_object"], int(t["source_row"])
    )

def parse_bool(value):
    if isinstance(value, bool):
        return value

    text = str(value).strip().lower()
    if text == "true":
        return True
    if text == "false":
        return False

    raise ValueError(f"Invalid boolean: {value!r}")

def normalized(row):
    return {
        "record_id": row["record_id"],
        "sensor_id": row["sensor_id"],
        "site": row["site"],
        "event_time_utc": timestamp(row["event_time_utc"]),
        "ingest_time_utc": timestamp(row["ingest_time_utc"]),
        "temperature_c": Decimal(str(row["temperature_c"])),
        "is_late": parse_bool(row["is_late"]),
        "source_object": row["source_object"],
        "source_row": int(row["source_row"]),
        "source_sha256": row["source_sha256"],
    }


baseline_rows = read_jsonl(ev / "silver_baseline.jsonl")
after_rows = read_jsonl(ev / "silver_after_batch02.jsonl")
replay_rows = read_jsonl(ev / "silver_replay.jsonl")
history_rows = read_jsonl(ev / "silver_time_travel.jsonl")

before = {r["record_id"]: r for r in baseline_rows}
after = {r["record_id"]: r for r in after_rows}

assert len(before) == len(baseline_rows), "Duplicate baseline IDs"
assert len(after) == len(after_rows), "Duplicate final IDs"

with Path("data/changes/changes_batch02.csv").open(
    encoding="utf-8-sig", newline=""
) as f:
    incoming = list(csv.DictReader(f))

# Chọn winner độc lập bằng Python cho fixture nhỏ.
winners = {}
for row in incoming:
    key = row["record_id"]
    if key not in winners or incoming_is_better(row, winners[key]):
        winners[key] = row

counts = Counter()
trace = []
expected = dict(before)

for key, winner in sorted(winners.items()):
    old = before.get(key)

    if old is None:
        action, reason = "INSERT", "NEW_KEY"
        expected[key] = winner
    elif incoming_is_better(winner, old):
        action = "UPDATE"
        if timestamp(winner["ingest_time_utc"]) > timestamp(old["ingest_time_utc"]):
            reason = "NEWER_TIMESTAMP"
        elif winner["source_object"] < old["source_object"]:
            reason = "TIE_SOURCE_OBJECT"
        else:
            reason = "TIE_SOURCE_ROW"
        expected[key] = winner
    else:
        action = "KEEP"
        if timestamp(winner["ingest_time_utc"]) < timestamp(old["ingest_time_utc"]):
            reason = "STALE_TIMESTAMP"
        elif normalized(winner) == normalized(old):
            reason = "IDENTICAL"
        else:
            reason = "TIE_LOSER"

    counts[action] += 1
    counts[reason] += 1

    trace.append({
        "record_id": key,
        "action": action,
        "reason": reason,
        "before": old,
        "incoming_winner": winner,
        "after": after.get(key),
    })

expected_normalized = {
    k: normalized(v) for k, v in expected.items()
}
actual_normalized = {
    k: normalized(v) for k, v in after.items()
}

assert expected_normalized == actual_normalized, (
    "Final Silver differs from independently computed result"
)

def keyed(rows):
    result = {r["record_id"]: normalized(r) for r in rows}
    assert len(result) == len(rows), "Duplicate IDs in export"
    return result

assert keyed(after_rows) == keyed(replay_rows), "Replay differs"
assert keyed(baseline_rows) == keyed(history_rows), "History differs"

summary = {
    "verification_scope": "Saved exports and incoming transport; not live Delta reads",
    "incoming_rows": len(incoming),
    "incoming_winners": len(winners),
    "superseded_revisions": len(incoming) - len(winners),
    "baseline_rows": len(before),
    "final_rows": len(after),
    "decisions_for_incoming_keys": dict(counts),
    "baseline_keys_not_in_batch": len(set(before) - set(winners)),
    "full_silver_comparison_passed": True,
    "replay_export_comparison_passed": True,
    "historical_export_comparison_passed": True,
}

(ev / "task3_merge_summary.json").write_text(
    json.dumps(summary, indent=2, ensure_ascii=False),
    encoding="utf-8",
)
(ev / "task3_row_trace.json").write_text(
    json.dumps(trace, indent=2, ensure_ascii=False),
    encoding="utf-8",
)

# Chọn một ví dụ thực tế cho mỗi loại quyết định/lý do.
examples = {}
for row in trace:
    examples.setdefault(row["reason"], row)

(ev / "task3_trace_examples.json").write_text(
    json.dumps(list(examples.values()), indent=2, ensure_ascii=False),
    encoding="utf-8",
)

print(json.dumps(summary, indent=2, ensure_ascii=False))
print("\nPASS: independent Silver comparison, replay and historical exports")
print("Created: task3_merge_summary.json")
print("Created: task3_row_trace.json")
print("Created: task3_trace_examples.json")
