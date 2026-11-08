
from datetime import date
from decimal import Decimal

from pyspark.sql import SparkSession, types as T

from templates.student_impl import build_gold

spark = (
    SparkSession.builder
    .appName("Test-Task2-Gold")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

schema = T.StructType([
    T.StructField("site", T.StringType(), False),
    T.StructField("event_date", T.DateType(), False),
    T.StructField("sensor_id", T.StringType(), False),
    T.StructField("temperature_c", T.DecimalType(8, 2), False),
    T.StructField("is_late", T.BooleanType(), False),
])

data = [
    ("A", date(2026, 2, 6), "S01", Decimal("20.00"), True),
    ("A", date(2026, 2, 6), "S02", Decimal("30.00"), False),
    ("A", date(2026, 2, 6), "S01", Decimal("26.00"), True),
    ("B", date(2026, 2, 6), "S03", Decimal("25.00"), False),
]

silver_df = spark.createDataFrame(data, schema)

gold_df = build_gold(silver_df)

gold_df.orderBy("site").show(truncate=False)
gold_df.printSchema()

assert gold_df.count() == 2

a = gold_df.filter("site = 'A'").first()

assert a["n_observations"] == 3
assert a["sum_temperature_c"] == Decimal("76.00")
assert a["mean_temperature_c"] == Decimal("25.333333")
assert a["n_late"] == 2
assert a["late_rate"] == Decimal("0.666667")
assert a["n_above_25"] == 2
assert a["n_sensors"] == 2

b = gold_df.filter("site = 'B'").first()

assert b["n_observations"] == 1
assert b["n_above_25"] == 0  # 25.00 không lớn hơn 25
assert b["n_sensors"] == 1

print("TASK 2 TEST: PASS")

spark.stop()
