from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    arrays_zip,
    col,
    explode,
    input_file_name,
    to_date,
    to_timestamp
)

# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("UttarakhandTrek-WeatherBronzeToSilver")
    .master("local[*]")
    .getOrCreate()
)

print("=" * 70)
print("PYSPARK - WEATHER BRONZE TO SILVER")
print("=" * 70)


# ============================================================
# PATHS
# ============================================================

INPUT_PATH = "pipeline/data/raw/weather/*.json"
OUTPUT_PATH = "pipeline/data/processed/weather_silver"


# ============================================================
# READ RAW WEATHER JSON
# ============================================================

print("\nReading raw weather JSON files...")

df = (
    spark.read
    .option("multiLine", "true")
    .json(INPUT_PATH)
)

print(f"Weather JSON files loaded.")
print(f"Number of trek records: {df.count()}")


# ============================================================
# SELECT REQUIRED FIELDS
# ============================================================

print("\nSelecting required weather fields...")

weather_df = df.select(
    col("trek_id"),
    col("trek_name"),
    col("latitude"),
    col("longitude"),
    col("elevation"),

    col("hourly.time").alias("times"),
    col("hourly.temperature_2m").alias("temperatures"),
    col("hourly.relative_humidity_2m").alias("humidities"),
    col("hourly.precipitation").alias("precipitations"),
    col("hourly.visibility").alias("visibilities"),
    col("hourly.wind_speed_10m").alias("wind_speeds"),
    col("hourly.wind_gusts_10m").alias("wind_gusts"),

    input_file_name().alias("source_file")
)


# ============================================================
# FLATTEN HOURLY ARRAYS
# ============================================================

print("\nFlattening hourly weather data...")

weather_df = weather_df.withColumn(
    "weather",
    explode(
        arrays_zip(
            "times",
            "temperatures",
            "humidities",
            "precipitations",
            "visibilities",
            "wind_speeds",
            "wind_gusts"
        )
    )
)


# ============================================================
# CREATE ONE ROW PER TREK + HOUR
# ============================================================

silver_df = weather_df.select(
    col("trek_id"),
    col("trek_name"),
    col("latitude"),
    col("longitude"),
    col("elevation"),

    to_timestamp(
        col("weather.times"),
        "yyyy-MM-dd'T'HH:mm"
    ).alias("weather_time"),

    col("weather.temperatures")
        .cast("double")
        .alias("temperature_c"),

    col("weather.humidities")
        .cast("double")
        .alias("humidity_pct"),

    col("weather.precipitations")
        .cast("double")
        .alias("precipitation_mm"),

    col("weather.visibilities")
        .cast("double")
        .alias("visibility_m"),

    col("weather.wind_speeds")
        .cast("double")
        .alias("wind_speed_kmh"),

    col("weather.wind_gusts")
        .cast("double")
        .alias("wind_gust_kmh"),

    col("source_file")
)


# ============================================================
# ADD WEATHER DATE
# ============================================================

silver_df = silver_df.withColumn(
    "weather_date",
    to_date(col("weather_time"))
)


# ============================================================
# REMOVE INVALID RECORDS
# ============================================================

print("\nRemoving records with invalid timestamps...")

before_filter = silver_df.count()

silver_df = silver_df.filter(
    col("weather_time").isNotNull()
)

after_filter = silver_df.count()

print(f"Records before filtering: {before_filter}")
print(f"Records after filtering:  {after_filter}")
print(f"Invalid records removed:  {before_filter - after_filter}")


# ============================================================
# REMOVE DUPLICATE WEATHER RECORDS
# ============================================================

print("\nRemoving duplicate weather records...")

before_duplicates = silver_df.count()

silver_df = silver_df.dropDuplicates(
    ["trek_id", "weather_time"]
)

after_duplicates = silver_df.count()

print(f"Records before duplicate removal: {before_duplicates}")
print(f"Records after duplicate removal:  {after_duplicates}")
print(f"Duplicates removed:               {before_duplicates - after_duplicates}")


# ============================================================
# DATA QUALITY CHECKS
# ============================================================

print("\n" + "=" * 70)
print("WEATHER DATA QUALITY CHECKS")
print("=" * 70)

total_records = silver_df.count()
distinct_treks = silver_df.select("trek_id").distinct().count()

print(f"\nTotal weather records: {total_records}")
print(f"Distinct trek IDs:     {distinct_treks}")

print("\nNull value counts:")

null_columns = [
    "trek_id",
    "trek_name",
    "latitude",
    "longitude",
    "weather_time",
    "temperature_c",
    "humidity_pct",
    "precipitation_mm",
    "visibility_m",
    "wind_speed_kmh",
    "wind_gust_kmh"
]

for column_name in null_columns:
    null_count = silver_df.filter(
        col(column_name).isNull()
    ).count()

    print(f"  {column_name:25} : {null_count}")


# ============================================================
# WEATHER DATA PREVIEW
# ============================================================

print("\n" + "=" * 70)
print("WEATHER SILVER DATA PREVIEW")
print("=" * 70)

silver_df.select(
    "trek_id",
    "trek_name",
    "weather_time",
    "temperature_c",
    "humidity_pct",
    "precipitation_mm",
    "visibility_m",
    "wind_speed_kmh",
    "wind_gust_kmh"
).show(20, truncate=False)


# ============================================================
# SAVE SILVER DATA
# ============================================================

print("\n" + "=" * 70)
print("SAVING WEATHER SILVER DATA")
print("=" * 70)

silver_df.write \
    .mode("overwrite") \
    .parquet(OUTPUT_PATH)

print(f"\nWeather Silver data saved to:")
print(OUTPUT_PATH)


# ============================================================
# FINAL VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("FINAL WEATHER SILVER VALIDATION")
print("=" * 70)

final_count = silver_df.count()

print(f"\nFinal weather Silver records: {final_count}")
print(f"Distinct trek IDs:            {distinct_treks}")

if final_count > 0:
    print("\n✅ WEATHER SILVER DATA CREATED SUCCESSFULLY")
else:
    print("\n❌ WEATHER SILVER DATA IS EMPTY")


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()

print("\n" + "=" * 70)
print("WEATHER BRONZE TO SILVER COMPLETED ✅")
print("=" * 70)