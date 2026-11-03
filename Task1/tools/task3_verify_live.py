import json
import os
import sys
from pathlib import Path

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from templates.student_impl import build_gold

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

ev = Path(sys.argv[1])
state = json.loads((ev / "state.json").read_text())

builder = (
    SparkSession.builder
    .appName("Task3-Live-Verification")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.ansi.enabled", "true")
    .config("spark.sql.shuffle.partitions", "2")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension",
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def read_version(path, version):
    return (
        spark.read.format("delta")
        .option("versionAsOf", int(version))
        .load(path)
    )


def assert_equal(left, right, label):
    assert set(left.columns) == set(right.columns), (
        f"{label}: different columns"
    )
    right = right.select(*left.columns)

    left_types = [(f.name, f.dataType) for f in left.schema.fields]
    right_types = [(f.name, f.dataType) for f in right.schema.fields]
    assert left_types == right_types, f"{label}: different types"

    assert left.exceptAll(right).limit(1).count() == 0, (
        f"{label}: rows only on left"
    )
    assert right.exceptAll(left).limit(1).count() == 0, (
        f"{label}: rows only on right"
    )


try:
    # Gold phải gắn với đúng snapshot Silver đã dùng để tạo nó.
    silver_version = state["gold_source_silver_version"]
    gold_version = state["gold_final_version"]

    assert silver_version == state["silver_final_version"], (
        "Gold/Silver binding mismatch"
    )

    silver = read_version(state["silver_path"], silver_version)
    stored_gold = read_version(state["gold_path"], gold_version)
    rebuilt_gold = build_gold(silver)

    assert_equal(rebuilt_gold, stored_gold, "Gold recomputation")
    print("PASS: stored Gold equals recomputation from pinned Silver")

    # Đối chiếu bản Silver ngay sau update và sau replay.
    replay = read_version(
        state["silver_path"], state["silver_replay_version"]
    )
    assert_equal(silver, replay, "Silver replay")
    print("PASS: live Silver replay equivalence")

    # Trong runner, bronze_final_version được ghi trước receipt replay.
    bronze_before_version = int(state["bronze_final_version"])
    bronze_latest_version = int(
        DeltaTable.forPath(spark, state["bronze_path"])
        .history(1).select("version").first()[0]
    )

    bronze_before = read_version(
        state["bronze_path"], bronze_before_version
    )
    bronze_latest = read_version(
        state["bronze_path"], bronze_latest_version
    )

    # So sánh toàn bộ hàng, gồm receipt metadata.
    assert_equal(bronze_before, bronze_latest, "Bronze replay")
    print("PASS: full Bronze content unchanged since pre-replay snapshot")

    receipt_keys = ["source_object", "source_row", "source_sha256"]
    duplicate_receipts = (
        bronze_latest.groupBy(*receipt_keys)
        .count()
        .filter("count > 1")
        .count()
    )
    assert duplicate_receipts == 0, "Duplicate Bronze receipts"
    print("PASS: no duplicate physical receipt keys")

    report = {
        "scope": "Read-only verification of actual Delta snapshots",
        "silver_path": state["silver_path"],
        "silver_version": silver_version,
        "silver_replay_version": state["silver_replay_version"],
        "gold_path": state["gold_path"],
        "gold_version": gold_version,
        "gold_groups": stored_gold.count(),
        "gold_recomputation_passed": True,
        "silver_live_replay_passed": True,
        "bronze_path": state["bronze_path"],
        "bronze_before_replay_version": bronze_before_version,
        "bronze_latest_version_at_verification": bronze_latest_version,
        "bronze_rows": bronze_latest.count(),
        "bronze_full_content_equal": True,
        "duplicate_receipt_keys": duplicate_receipts,
        "note": (
            "Bronze comparison uses the latest version at verification time. "
            "Run immediately after incremental, before any subsequent batches. "
            "Gold is recomputed using the current build_gold implementation."
        ),
    }

    output = ev / "task3_live_verification.json"
    output.write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(json.dumps(report, indent=2, ensure_ascii=False))
    print("ALL LIVE CHECKS PASSED")

finally:
    spark.stop()
