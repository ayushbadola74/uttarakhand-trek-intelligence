from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    regexp_extract,
    regexp_replace,
    trim,
    lower,
    expr
)

# ---------------------------------------------------------
# 1. START SPARK
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("UttarakhandTrek-SilverToGold")
    .master("local[*]")
    .config(
        "spark.hadoop.fs.file.impl",
        "org.apache.hadoop.fs.RawLocalFileSystem"
    )
    .getOrCreate()
)

print("=" * 70)
print("PYSPARK - SILVER TO GOLD")
print("=" * 70)

# ---------------------------------------------------------
# 2. PATHS
# ---------------------------------------------------------

INPUT_PATH = "pipeline/data/processed/treks_silver"
GOLD_PATH = "pipeline/data/processed/treks_gold"
SUMMARY_PATH = "pipeline/data/processed/trek_summary"

# ---------------------------------------------------------
# 3. READ SILVER DATA
# ---------------------------------------------------------

print("\nReading Silver dataset...")

df = spark.read.parquet(INPUT_PATH)

print(f"Silver records: {df.count()}")

print("\nSilver dataset preview:")

df.show(10, truncate=False)

# ---------------------------------------------------------
# 4. ELEVATION GAIN
# ---------------------------------------------------------

print("\n" + "=" * 70)
print("1. CALCULATING ELEVATION GAIN")
print("=" * 70)

df = df.withColumn(
    "elevation_gain_m",
    col("end_elevation") - col("start_elevation")
)

# ---------------------------------------------------------
# 5. DISTANCE CATEGORY
# ---------------------------------------------------------

print("\n" + "=" * 70)
print("2. CREATING DISTANCE CATEGORY")
print("=" * 70)

df = df.withColumn(
    "distance_category",
    when(col("distance_km").isNull(), "Unknown")
    .when(col("distance_km") < 10, "Short")
    .when(col("distance_km") < 20, "Medium")
    .otherwise("Long")
)

# ---------------------------------------------------------
# 6. DURATION CATEGORY
# ---------------------------------------------------------

print("\n" + "=" * 70)
print("3. CREATING DURATION CATEGORY")
print("=" * 70)

# Use maximum duration to classify the trek
# This works with ranges such as 5–6, 6–7, etc.

df = df.withColumn(
    "duration_category",
    when(col("duration_max_days").isNull(), "Unknown")
    .when(col("duration_max_days") <= 2, "Short")
    .when(col("duration_max_days") <= 5, "Medium")
    .otherwise("Long")
)

# ---------------------------------------------------------
# 7. STANDARDIZE DIFFICULTY
# ---------------------------------------------------------

print("\n" + "=" * 70)
print("4. STANDARDIZING DIFFICULTY")
print("=" * 70)

df = df.withColumn(
    "difficulty",
    trim(col("difficulty"))
)

difficulty_clean = lower(trim(col("difficulty")))

df = df.withColumn(
    "difficulty",
    when(
        difficulty_clean == "easy-moderate",
        "Easy-Moderate"
    )
    .when(
        difficulty_clean == "moderate-difficult",
        "Moderate-Difficult"
    )
    .when(
        difficulty_clean == "very difficult",
        "Very Difficult"
    )
    .when(
        difficulty_clean == "extreme",
        "Extreme"
    )
    .when(
        difficulty_clean == "difficult",
        "Difficult"
    )
    .when(
        difficulty_clean == "moderate",
        "Moderate"
    )
    .when(
        difficulty_clean == "easy",
        "Easy"
    )
    .when(
        difficulty_clean == "hard",
        "Hard"
    )
    .otherwise(trim(col("difficulty")))
)

# ---------------------------------------------------------
# 8. CLEAN COST
# ---------------------------------------------------------

print("\n" + "=" * 70)
print("5. CLEANING COST")
print("=" * 70)

df = df.withColumn(
    "cost_clean",
    regexp_replace(
        col("approx_cost").cast("string"),
        r"[₹,\s]",
        ""
    )
)

# ---------------------------------------------------------
# 9. EXTRACT MINIMUM COST
# ---------------------------------------------------------

print("\nExtracting minimum cost...")

df = df.withColumn(
    "cost_min_raw",
    regexp_extract(
        col("cost_clean"),
        r"(\d+(?:\.\d+)?)([kK]?)",
        1
    )
)

df = df.withColumn(
    "cost_min_unit",
    regexp_extract(
        col("cost_clean"),
        r"(\d+(?:\.\d+)?)([kK]?)",
        2
    )
)

df = df.withColumn(
    "cost_min",
    expr("""
        CASE
            WHEN cost_min_raw = '' THEN NULL
            WHEN lower(cost_min_unit) = 'k'
                THEN try_cast(cost_min_raw AS DOUBLE) * 1000
            ELSE try_cast(cost_min_raw AS DOUBLE)
        END
    """)
)

# ---------------------------------------------------------
# 10. EXTRACT MAXIMUM COST
# ---------------------------------------------------------

print("\nExtracting maximum cost...")

df = df.withColumn(
    "cost_max_raw",
    regexp_extract(
        col("cost_clean"),
        r"(?:[–-]|to)(\d+(?:\.\d+)?)([kK]?)",
        1
    )
)

df = df.withColumn(
    "cost_max_unit",
    regexp_extract(
        col("cost_clean"),
        r"(?:[–-]|to)(\d+(?:\.\d+)?)([kK]?)",
        2
    )
)

df = df.withColumn(
    "cost_max",
    expr("""
        CASE
            WHEN cost_max_raw = '' THEN NULL
            WHEN lower(cost_max_unit) = 'k'
                THEN try_cast(cost_max_raw AS DOUBLE) * 1000
            ELSE try_cast(cost_max_raw AS DOUBLE)
        END
    """)
)

# ---------------------------------------------------------
# 11. SELECT FINAL GOLD COLUMNS
# ---------------------------------------------------------

print("\n" + "=" * 70)
print("6. CREATING GOLD DATASET")
print("=" * 70)

gold_df = df.select(
    "trek_id",
    "trek_name",
    "category",
    "starting_point",
    "start_lat",
    "start_lon",
    "start_elevation",
    "ending_point",
    "end_lat",
    "end_lon",
    "end_elevation",
    "elevation_gain_m",
    "distance_km",
    "distance_category",
    "duration_days",
    "duration_min_days",
    "duration_max_days",
    "duration_avg_days",
    "duration_category",
    "difficulty",
    "best_season",
    "approx_cost",
    "cost_min",
    "cost_max"
)

# ---------------------------------------------------------
# 12. GOLD DATA PREVIEW
# ---------------------------------------------------------

print("\nGold dataset preview:")

gold_df.select(
    "trek_id",
    "trek_name",
    "elevation_gain_m",
    "distance_km",
    "distance_category",
    "duration_days",
    "duration_avg_days",
    "duration_category",
    "difficulty",
    "cost_min",
    "cost_max"
).show(10, truncate=False)

gold_count = gold_df.count()

print(f"\nGold records: {gold_count}")

# ---------------------------------------------------------
# 13. SUMMARY ANALYTICS
# ---------------------------------------------------------

print("\n" + "=" * 70)
print("7. CREATING SUMMARY ANALYTICS")
print("=" * 70)

summary_df = (
    gold_df
    .groupBy("difficulty")
    .count()
    .withColumnRenamed("count", "trek_count")
)

print("\nTreks by difficulty:")

summary_df.show(truncate=False)

# ---------------------------------------------------------
# 14. SAVE GOLD DATASET
# ---------------------------------------------------------

print("\n" + "=" * 70)
print("8. SAVING GOLD DATASET")
print("=" * 70)

gold_df.write.mode("overwrite").parquet(GOLD_PATH)

print(f"Gold dataset saved to: {GOLD_PATH}")

# ---------------------------------------------------------
# 15. SAVE SUMMARY
# ---------------------------------------------------------

summary_df.write.mode("overwrite").parquet(SUMMARY_PATH)

print(f"Summary analytics saved to: {SUMMARY_PATH}")

# ---------------------------------------------------------
# 16. FINISH
# ---------------------------------------------------------

spark.stop()

print("\n" + "=" * 70)
print("SILVER TO GOLD COMPLETED SUCCESSFULLY ✅")
print("=" * 70)