"""Complete these FIVE assessed functions. Do not edit the reference data.
Use the function signatures exactly: src/run_lab.py imports this module.
All functions operate on Spark DataFrames except validate_release_binding.
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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
    """TODO 3 (T3). Mutate an existing Delta table; return None.
    Match by record_id. Update only if the incoming row precedes the stored
    row in the SAME ordering as TODO 2; insert missing business keys.
    Equal provenance and ingest timestamp is a no-op. Stale data must not
    replace a newer value. The runner validates the WHOLE batch first.
    """
    raise NotImplementedError('TODO 3: merge_silver')


def benchmark_query(df):
    """TODO 4 (T4). Both layouts must evaluate exactly this query:
    filter event_date = DATE '2026-02-06'; group by site; return
    site, n_observations, sum_temperature_c (decimal(18,2)), n_late.
    Do not use replica_id in the GROUP BY. Do not call collect() here.
    The supplied runner materializes this bounded aggregate for timing.
    """
    raise NotImplementedError('TODO 4: benchmark_query')


def validate_release_binding(draft, expected):
    """TODO 5 (T5). Return (passed: bool, reasons: list[str]).
    Require nonempty author, reviewer, steward and reviewer != author.
    Bind handoff_logical_sha256, code_sha256, silver_path, silver_version,
    gold_path, gold_version and gold_source_silver_version to expected.
    Also require draft.gold_source_silver_version == draft.silver_version.
    Rejection reasons: REVIEW, METADATA, VERSION_BINDING, CODE_BINDING,
    or HANDOFF_BINDING. Missing keys must reject, never crash/pass.
    This is a course release check, not a digital signature or IAM control.
    """
    raise NotImplementedError('TODO 5: validate_release_binding')
