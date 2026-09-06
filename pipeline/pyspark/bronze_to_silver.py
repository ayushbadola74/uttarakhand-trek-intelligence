from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    expr,
    regexp_extract,
    trim,
    when
)

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

print(f"Total records loaded: {df.count()}")

# ---------------------------------------------------------
# 4. Remove completely empty columns
# ---------------------------------------------------------

print("\nRemoving empty columns...")

empty_columns = [
    column_name
    for column_name in df.columns
    if column_name.startswith("_c")
]

if empty_columns:
    print(f"Removing columns: {empty_columns}")
    df = df.drop(*empty_columns)
else:
    print("No empty columns found.")

# ---------------------------------------------------------
# 5. Remove duplicate treks
# ---------------------------------------------------------

print("\nRemoving duplicate records...")

before_duplicates = df.count()

df = df.dropDuplicates(["trek_id"])

after_duplicates = df.count()

print(f"Records before duplicate removal: {before_duplicates}")
print(f"Records after duplicate removal:  {after_duplicates}")
print(f"Duplicates removed:               {before_duplicates - after_duplicates}")

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
    "distance_km"
]

for column_name in numeric_columns:
    if column_name in df.columns:
        df = df.withColumn(
            column_name,
            expr(f"try_cast(`{column_name}` as double)")
        )

# ---------------------------------------------------------
# 7. Parse duration range
# ---------------------------------------------------------

print("\nParsing duration values...")

duration_text = trim(col("duration_days").cast("string"))

df = df.withColumn(
    "duration_min_raw",
    regexp_extract(
        duration_text,
        r"^(\d+(?:\.\d+)?)",
        1
    )
)

df = df.withColumn(
    "duration_max_raw",
    regexp_extract(
        duration_text,
        r"(?:[–-]|to)\s*(\d+(?:\.\d+)?)$",
        1
    )
)

df = df.withColumn(
    "duration_min_days",
    expr("try_cast(duration_min_raw AS DOUBLE)")
)

df = df.withColumn(
    "duration_max_days",
    when(
        col("duration_max_raw") == "",
        col("duration_min_days")
    ).otherwise(
        expr("try_cast(duration_max_raw AS DOUBLE)")
    )
)

df = df.withColumn(
    "duration_avg_days",
    (
        col("duration_min_days")
        + col("duration_max_days")
    ) / 2
)

# Remove temporary parsing columns

df = df.drop(
    "duration_min_raw",
    "duration_max_raw"
)

# ---------------------------------------------------------
# 8. Coordinate validation
# ---------------------------------------------------------

print("\n" + "=" * 70)
print("COORDINATE VALIDATION")
print("=" * 70)

location_columns = [
    "start_lat",
    "start_lon",
    "end_lat",
    "end_lon"
]

print("\nRequired location fields:")
for column_name in location_columns:
    print(f"  - {column_name}")

# Keep only records where ALL four coordinates exist

valid_location_condition = (
    col("start_lat").isNotNull()
    & col("start_lon").isNotNull()
    & col("end_lat").isNotNull()
    & col("end_lon").isNotNull()
)

valid_treks = df.filter(valid_location_condition)
skipped_treks = df.filter(~valid_location_condition)

valid_count = valid_treks.count()
skipped_count = skipped_treks.count()

print(f"\nTotal records:   {df.count()}")
print(f"Valid treks:     {valid_count} ✅")
print(f"Skipped treks:   {skipped_count} ⏸️")

# ---------------------------------------------------------
# 9. Show skipped treks
# ---------------------------------------------------------

if skipped_count > 0:

    print("\n" + "=" * 70)
    print("TREKS SKIPPED DUE TO INCOMPLETE LOCATION DATA")
    print("=" * 70)

    skipped_treks.select(
        "trek_id",
        "trek_name",
        "start_lat",
        "start_lon",
        "end_lat",
        "end_lon"
    ).show(
        20,
        truncate=False
    )

# ---------------------------------------------------------
# 10. Silver dataset
# ---------------------------------------------------------

df = valid_treks

print("\n" + "=" * 70)
print("SILVER DATASET PREVIEW")
print("=" * 70)

df.select(
    "trek_id",
    "trek_name",
    "start_lat",
    "start_lon",
    "end_lat",
    "end_lon",
    "distance_km",
    "duration_days",
    "duration_min_days",
    "duration_max_days",
    "duration_avg_days",
    "difficulty"
).show(
    10,
    truncate=False
)

# ---------------------------------------------------------
# 11. Count Silver records
# ---------------------------------------------------------

silver_count = df.count()

print(f"\nSilver records: {silver_count}")

# ---------------------------------------------------------
# 12. Save Silver as Parquet
# ---------------------------------------------------------

print("\nSaving Silver dataset...")

df.write.mode("overwrite").parquet(
    OUTPUT_PATH
)

print(f"Silver data saved to: {OUTPUT_PATH}")

# ---------------------------------------------------------
# 13. Final validation
# ---------------------------------------------------------

print("\n" + "=" * 70)
print("FINAL SILVER VALIDATION")
print("=" * 70)

print(f"Input records:        {after_duplicates}")
print(f"Valid records:        {valid_count}")
print(f"Skipped records:      {skipped_count}")
print(f"Silver records:       {silver_count}")

if silver_count == 88:
    print("\n✅ EXPECTED RESULT: 88 valid treks")
else:
    print(
        f"\n⚠️ Expected 88 treks, but Silver contains {silver_count}."
    )

# ---------------------------------------------------------
# 14. Stop Spark
# ---------------------------------------------------------

spark.stop()

print("\n" + "=" * 70)
print("BRONZE TO SILVER COMPLETED ✅")
print("=" * 70)