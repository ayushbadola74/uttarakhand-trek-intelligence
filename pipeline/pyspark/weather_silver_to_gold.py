from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    max,
    min,
    sum,
    when,
    round
)

# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("UttarakhandTrek-WeatherSilverToGold")
    .master("local[*]")
    .getOrCreate()
)

print("=" * 70)
print("PYSPARK - WEATHER SILVER TO GOLD")
print("=" * 70)


# ============================================================
# PATHS
# ============================================================

INPUT_PATH = "pipeline/data/processed/weather_silver"
OUTPUT_PATH = "pipeline/data/processed/weather_gold"
SUMMARY_PATH = "pipeline/data/processed/weather_summary"


# ============================================================
# READ WEATHER SILVER
# ============================================================

print("\nReading Weather Silver data...")

df = spark.read.parquet(INPUT_PATH)

silver_count = df.count()

print(f"Weather Silver records: {silver_count}")


# ============================================================
# WEATHER GOLD - HOURLY DATA
# ============================================================

print("\nCreating Weather Gold hourly dataset...")

gold_df = df.withColumn(
    "temperature_category",
    when(col("temperature_c") < 5, "Very Cold")
    .when(col("temperature_c") < 15, "Cold")
    .when(col("temperature_c") < 25, "Moderate")
    .when(col("temperature_c") < 35, "Warm")
    .otherwise("Very Hot")
)

gold_df = gold_df.withColumn(
    "precipitation_category",
    when(col("precipitation_mm") == 0, "No Rain")
    .when(col("precipitation_mm") <= 2.5, "Light Rain")
    .when(col("precipitation_mm") <= 7.5, "Moderate Rain")
    .otherwise("Heavy Rain")
)

gold_df = gold_df.withColumn(
    "wind_category",
    when(col("wind_speed_kmh") < 20, "Low")
    .when(col("wind_speed_kmh") < 40, "Moderate")
    .when(col("wind_speed_kmh") < 60, "Strong")
    .otherwise("Very Strong")
)

gold_df = gold_df.withColumn(
    "visibility_category",
    when(col("visibility_m") < 1000, "Very Low")
    .when(col("visibility_m") < 5000, "Low")
    .when(col("visibility_m") < 10000, "Moderate")
    .otherwise("Good")
)


# ============================================================
# CONDITION SCORE
# ============================================================

print("\nCalculating project-defined weather condition score...")

gold_df = gold_df.withColumn(
    "condition_score",
    (
        when(col("precipitation_mm") > 7.5, 30)
        .when(col("precipitation_mm") > 2.5, 15)
        .otherwise(0)
        +
        when(col("wind_speed_kmh") >= 60, 30)
        .when(col("wind_speed_kmh") >= 40, 15)
        .otherwise(0)
        +
        when(col("visibility_m") < 1000, 25)
        .when(col("visibility_m") < 5000, 10)
        .otherwise(0)
        +
        when(col("temperature_c") < 0, 15)
        .when(col("temperature_c") > 40, 15)
        .otherwise(0)
    )
)

gold_df = gold_df.withColumn(
    "condition_status",
    when(col("condition_score") >= 50, "Red - High Risk")
    .when(col("condition_score") >= 25, "Yellow - Caution")
    .otherwise("Green - Normal")
)


# ============================================================
# SELECT GOLD COLUMNS
# ============================================================

gold_df = gold_df.select(
    "trek_id",
    "trek_name",
    "latitude",
    "longitude",
    "elevation",
    "weather_date",
    "weather_time",
    "temperature_c",
    "humidity_pct",
    "precipitation_mm",
    "visibility_m",
    "wind_speed_kmh",
    "wind_gust_kmh",
    "temperature_category",
    "precipitation_category",
    "wind_category",
    "visibility_category",
    "condition_score",
    "condition_status",
    "source_file"
)


# ============================================================
# WEATHER SUMMARY
# ============================================================

print("\nCreating trek-level weather summary...")

weather_summary = gold_df.groupBy(
    "trek_id",
    "trek_name"
).agg(
    count("*").alias("weather_records"),

    round(avg("temperature_c"), 2)
        .alias("avg_temperature_c"),

    round(min("temperature_c"), 2)
        .alias("min_temperature_c"),

    round(max("temperature_c"), 2)
        .alias("max_temperature_c"),

    round(avg("humidity_pct"), 2)
        .alias("avg_humidity_pct"),

    round(sum("precipitation_mm"), 2)
        .alias("total_precipitation_mm"),

    round(avg("visibility_m"), 2)
        .alias("avg_visibility_m"),

    round(min("visibility_m"), 2)
        .alias("min_visibility_m"),

    round(max("wind_speed_kmh"), 2)
        .alias("max_wind_speed_kmh"),

    round(max("wind_gust_kmh"), 2)
        .alias("max_wind_gust_kmh"),

    round(avg("condition_score"), 2)
        .alias("avg_condition_score"),

    max(
        when(
            col("condition_status") == "Red - High Risk",
            1
        ).otherwise(0)
    ).alias("has_red_condition"),

    max(
        when(
            col("condition_status") == "Yellow - Caution",
            1
        ).otherwise(0)
    ).alias("has_yellow_condition")
)


# ============================================================
# SUMMARY STATUS
# ============================================================

weather_summary = weather_summary.withColumn(
    "overall_condition",
    when(
        col("has_red_condition") == 1,
        "Red - High Risk"
    )
    .when(
        col("has_yellow_condition") == 1,
        "Yellow - Caution"
    )
    .otherwise(
        "Green - Normal"
    )
)


# ============================================================
# SAVE WEATHER GOLD
# ============================================================

print("\nSaving Weather Gold dataset...")

gold_df.write \
    .mode("overwrite") \
    .parquet(OUTPUT_PATH)

print(f"Weather Gold saved to: {OUTPUT_PATH}")


# ============================================================
# SAVE WEATHER SUMMARY
# ============================================================

print("\nSaving Weather Summary...")

weather_summary.write \
    .mode("overwrite") \
    .parquet(SUMMARY_PATH)

print(f"Weather Summary saved to: {SUMMARY_PATH}")


# ============================================================
# PREVIEW GOLD
# ============================================================

print("\n" + "=" * 70)
print("WEATHER GOLD PREVIEW")
print("=" * 70)

gold_df.select(
    "trek_id",
    "trek_name",
    "weather_time",
    "temperature_c",
    "precipitation_mm",
    "wind_speed_kmh",
    "visibility_m",
    "condition_score",
    "condition_status"
).show(20, truncate=False)


# ============================================================
# PREVIEW SUMMARY
# ============================================================

print("\n" + "=" * 70)
print("WEATHER SUMMARY PREVIEW")
print("=" * 70)

weather_summary.orderBy("trek_id").show(
    20,
    truncate=False
)


# ============================================================
# FINAL VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("FINAL WEATHER GOLD VALIDATION")
print("=" * 70)

gold_count = gold_df.count()
summary_count = weather_summary.count()

print(f"\nWeather Silver records:  {silver_count}")
print(f"Weather Gold records:    {gold_count}")
print(f"Weather Summary records: {summary_count}")

if gold_count == silver_count:
    print("\n✅ GOLD RECORD COUNT MATCHES SILVER")
else:
    print("\n⚠️ GOLD RECORD COUNT DOES NOT MATCH SILVER")

if summary_count > 0:
    print("✅ WEATHER SUMMARY CREATED")
else:
    print("❌ WEATHER SUMMARY IS EMPTY")


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()

print("\n" + "=" * 70)
print("WEATHER SILVER TO GOLD COMPLETED ✅")
print("=" * 70)