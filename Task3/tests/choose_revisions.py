from pyspark.sql import SparkSession
from templates.student_impl import choose_revisions

spark = (
    SparkSession.builder
    .appName("TestChooseRevisions")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

changes_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/changes/changes_batch02.csv")
)

winners_df = choose_revisions(changes_df)

total_rows = changes_df.count()
winner_rows = winners_df.count()
distinct_ids = changes_df.select("record_id").distinct().count()

print("Input rows:", total_rows)
print("Winner rows:", winner_rows)
print("Distinct record IDs:", distinct_ids)

assert winner_rows == distinct_ids

assert (
    winners_df
    .groupBy("record_id")
    .count()
    .filter("count > 1")
    .count() == 0
)

print("TODO 2 TEST: PASS")

spark.stop()
