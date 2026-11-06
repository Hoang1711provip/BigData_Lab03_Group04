"""Complete these FIVE assessed functions. Do not edit the reference data.
Use the function signatures exactly: src/run_lab.py imports this module.
All functions operate on Spark DataFrames except validate_release_binding.
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

def build_gold(silver_df):
    gold_df = (
        silver_df
        .groupBy("site", "event_date")
        .agg(
            F.count("*")
             .cast("BIGINT")
             .alias("n_observations"),

            F.sum("temperature_c")
             .cast("DECIMAL(18,2)")
             .alias("sum_temperature_c"),

            (
                F.sum("temperature_c") / F.count("*")
            ).cast("DECIMAL(18,6)")
             .alias("mean_temperature_c"),

            F.min("temperature_c")
             .cast("DECIMAL(18,2)")
             .alias("min_temperature_c"),

            F.max("temperature_c")
             .cast("DECIMAL(18,2)")
             .alias("max_temperature_c"),

            F.sum(
                F.when(F.col("is_late") == True, 1).otherwise(0)
            ).cast("BIGINT")
             .alias("n_late"),

            F.sum(
                F.when(F.col("temperature_c") > 25, 1).otherwise(0)
            ).cast("BIGINT")
             .alias("n_above_25"),

            F.countDistinct("sensor_id")
             .cast("BIGINT")
             .alias("n_sensors")
        )
    )

    gold_df = gold_df.withColumn(
        "late_rate",
        (
            F.col("n_late") / F.col("n_observations")
        ).cast("DECIMAL(18,6)")
    )

    return gold_df.select(
        "site",
        "event_date",
        "n_observations",
        "sum_temperature_c",
        "mean_temperature_c",
        "min_temperature_c",
        "max_temperature_c",
        "n_late",
        "late_rate",
        "n_above_25",
        "n_sensors"
    )



def choose_revisions(changes_df):
    window_spec = (
        Window
        .partitionBy("record_id")
        .orderBy(
            F.col("ingest_time_utc").desc(),
            F.col("source_object").asc(),
            F.col("source_row").asc()
        )
    )

    winners_df = (
        changes_df
        .withColumn("_revision_rank", F.row_number().over(window_spec))
        .filter(F.col("_revision_rank") == 1)
        .drop("_revision_rank")
    )

    return winners_df

def merge_silver(spark, silver_path, winners_df):
    source = winners_df.withColumn(
        "event_date",
        F.to_date("event_time_utc"),
    )

    update_condition = """
        s.ingest_time_utc > t.ingest_time_utc
        OR (
            s.ingest_time_utc = t.ingest_time_utc
            AND s.source_object < t.source_object
        )
        OR (
            s.ingest_time_utc = t.ingest_time_utc
            AND s.source_object = t.source_object
            AND s.source_row < t.source_row
        )
    """

    (
        DeltaTable.forPath(spark, silver_path).alias("t")
        .merge(source.alias("s"), "t.record_id = s.record_id")
        .whenMatchedUpdateAll(condition=update_condition)
        .whenNotMatchedInsertAll()
        .execute()
    )


def benchmark_query(df):
    """Benchmark query for both data layouts."""

    result = (
        df
        .filter(
            F.col("event_date") == F.lit("2026-02-06").cast("date")
        )
        .groupBy("site")
        .agg(
            F.count("*")
             .cast("BIGINT")
             .alias("n_observations"),

            F.sum("temperature_c")
             .cast("DECIMAL(18,2)")
             .alias("sum_temperature_c"),

            F.sum(
                F.when(F.col("is_late") == True, 1).otherwise(0)
            )
             .cast("BIGINT")
             .alias("n_late")
        )
        .select(
            "site",
            "n_observations",
            "sum_temperature_c",
            "n_late"
        )
    )

    return result

def validate_release_binding(draft, expected):
    reasons = []

    if not isinstance(draft, dict):
        draft = {}

    if not isinstance(expected, dict):
        expected = {}

    MISSING = object()

    def get_value(data, key):
        return data.get(key, MISSING)

    def is_nonempty(value):
        return isinstance(value, str) and bool(value.strip())

    # Check metadata
    author = get_value(draft, "author")
    reviewer = get_value(draft, "reviewer")
    steward = get_value(draft, "steward")

    if not is_nonempty(author) or not is_nonempty(steward):
        reasons.append("METADATA")

    if (
        not is_nonempty(reviewer)
        or not is_nonempty(author)
        or reviewer == author
    ):
        reasons.append("REVIEW")

    # Check handoff binding
    key = "handoff_logical_sha256"
    if (
        get_value(draft, key) is MISSING
        or get_value(expected, key) is MISSING
        or get_value(draft, key) != get_value(expected, key)
    ):
        reasons.append("HANDOFF_BINDING")

    # Check code binding
    key = "code_sha256"
    if (
        get_value(draft, key) is MISSING
        or get_value(expected, key) is MISSING
        or get_value(draft, key) != get_value(expected, key)
    ):
        reasons.append("CODE_BINDING")

    # Check version and path binding
    version_keys = [
        "silver_path",
        "silver_version",
        "gold_path",
        "gold_version",
        "gold_source_silver_version"
    ]

    version_mismatch = any(
        get_value(draft, key) is MISSING
        or get_value(expected, key) is MISSING
        or get_value(draft, key) != get_value(expected, key)
        for key in version_keys
    )

    silver_version = get_value(draft, "silver_version")
    gold_source_version = get_value(
        draft, "gold_source_silver_version"
    )

    if (
        silver_version is MISSING
        or gold_source_version is MISSING
        or gold_source_version != silver_version
    ):
        version_mismatch = True

    if version_mismatch:
        reasons.append("VERSION_BINDING")

    return (len(reasons) == 0, reasons)
