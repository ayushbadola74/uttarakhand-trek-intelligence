from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# ---------------------------------------------------------
# 1. Start Spark
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("UttarakhandTrek-BronzeToSilver")
    .master("local[*]")
    .config(
        "spark.hadoop.fs.file.impl",
        "org.apache.hadoop.fs.RawLocalFileSystem"
    )
    .getOrCreate()
)

print("=" * 70)
print("PYSPARK - BRONZE TO SILVER")
print("=" * 70)


# ---------------------------------------------------------
# 2. Paths
# ---------------------------------------------------------

INPUT_PATH = "pipeline/data/treks.csv"
OUTPUT_PATH = "pipeline/data/processed/treks_silver"

# ---------------------------------------------------------
# 3. Read Bronze / Raw data
# ---------------------------------------------------------

print("\nReading raw trek data...")

df = spark.read.csv(
    INPUT_PATH,
    header=True,
    inferSchema=True
)

print(f"Total records: {df.count()}")


# ---------------------------------------------------------
# 4. Remove completely empty columns
# ---------------------------------------------------------

print("\nRemoving empty columns...")

# Your CSV contains empty columns at the end.
# Spark represents them as _c16 and _c17.

empty_columns = [
    column_name
    for column_name in df.columns
    if column_name.startswith("_c")
]

if empty_columns:
    print(f"Removing columns: {empty_columns}")
    df = df.drop(*empty_columns)


# ---------------------------------------------------------
# 5. Remove duplicate treks
# ---------------------------------------------------------

print("\nRemoving duplicate records...")

df = df.dropDuplicates(["trek_id"])


# ---------------------------------------------------------
# 6. Clean numeric columns
# ---------------------------------------------------------

print("\nCleaning numeric columns...")

numeric_columns = [
    "start_lat",
    "start_lon",
    "start_elevation",
    "end_lat",
    "end_lon",
    "end_elevation",
    "distance_km",
    "duration_days"
]

for column_name in numeric_columns:

    if column_name in df.columns:

        # try_cast converts invalid values such as
        # 'needs_verification' into NULL instead of crashing.
        df = df.withColumn(
            column_name,
            expr(f"try_cast(`{column_name}` as double)")
        )


# ---------------------------------------------------------
# 7. Coordinate validation
# ---------------------------------------------------------

print("\nChecking coordinates...")

df = df.filter(
    col("start_lat").isNotNull()
    & col("start_lon").isNotNull()
)


# ---------------------------------------------------------
# 8. Show Silver data
# ---------------------------------------------------------

print("\nSilver dataset preview:")

df.select(
    "trek_id",
    "trek_name",
    "start_lat",
    "start_lon",
    "distance_km",
    "difficulty"
).show(
    10,
    truncate=False
)


# ---------------------------------------------------------
# 9. Count Silver records
# ---------------------------------------------------------

silver_count = df.count()

print(f"\nSilver records: {silver_count}")


# ---------------------------------------------------------
# 10. Save Silver as Parquet
# ---------------------------------------------------------

print("\nSaving Silver dataset...")

df.write.mode("overwrite").parquet(
    OUTPUT_PATH
)

print(f"Silver data saved to: {OUTPUT_PATH}")


# ---------------------------------------------------------
# 11. Stop Spark
# ---------------------------------------------------------

spark.stop()

print("\n" + "=" * 70)
print("BRONZE TO SILVER COMPLETED ✅")
print("=" * 70)