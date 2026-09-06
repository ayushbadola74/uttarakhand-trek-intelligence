import json
import time
import requests
import pandas as pd
from pathlib import Path


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = (
    PROJECT_ROOT
    / "pipeline"
    / "data"
    / "processed"
    / "valid_treks.csv"
)

RAW_WEATHER_DIR = (
    PROJECT_ROOT
    / "pipeline"
    / "data"
    / "raw"
    / "weather"
)

PROCESSED_DIR = (
    PROJECT_ROOT
    / "pipeline"
    / "data"
    / "processed"
)

RAW_WEATHER_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# OPEN-METEO CONFIGURATION
# ============================================================

API_URL = "https://api.open-meteo.com/v1/forecast"

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "precipitation_probability",
    "visibility",
    "wind_speed_10m",
    "wind_gusts_10m",
    "weather_code"
]


# ============================================================
# START
# ============================================================

print("=" * 70)
print("UTTARAKHAND TREK WEATHER INGESTION")
print("=" * 70)


# ============================================================
# LOAD VALID TREKS
# ============================================================

print("\nReading validated trek data:")
print(INPUT_FILE)

if not INPUT_FILE.exists():
    raise FileNotFoundError(
        f"Valid trek file not found: {INPUT_FILE}"
    )

df = pd.read_csv(INPUT_FILE)

print(f"\nValid treks available for weather processing: {len(df)}")


# ============================================================
# VALIDATE REQUIRED COLUMNS
# ============================================================

required_columns = [
    "trek_id",
    "trek_name",
    "start_lat",
    "start_lon"
]

missing_columns = [
    column
    for column in required_columns
    if column not in df.columns
]

if missing_columns:
    raise ValueError(
        f"Missing required columns: {missing_columns}"
    )

print("\nRequired weather columns found. ✅")


# ============================================================
# WEATHER INGESTION
# ============================================================

print("\n" + "=" * 70)
print("FETCHING WEATHER DATA")
print("=" * 70)

successful = 0
failed = 0

weather_summary = []

for index, row in df.iterrows():

    trek_id = int(row["trek_id"])
    trek_name = row["trek_name"]

    latitude = float(row["start_lat"])
    longitude = float(row["start_lon"])

    print(
        f"\n[{index + 1}/{len(df)}] "
        f"{trek_id} - {trek_name}"
    )

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ",".join(HOURLY_VARIABLES),
        "forecast_days": 7,
        "timezone": "auto"
    }

    output_file = RAW_WEATHER_DIR / f"trek_{trek_id}_weather.json"

    try:

        response = requests.get(
            API_URL,
            params=params,
            timeout=30
        )

        response.raise_for_status()

        weather_data = response.json()

        # Add trek metadata so the raw record
        # remains linked to the source trek.

        weather_data["trek_id"] = trek_id
        weather_data["trek_name"] = trek_name
        weather_data["trek_latitude"] = latitude
        weather_data["trek_longitude"] = longitude

        with open(
            output_file,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                weather_data,
                file,
                indent=2
            )

        successful += 1

        weather_summary.append({
            "trek_id": trek_id,
            "trek_name": trek_name,
            "latitude": latitude,
            "longitude": longitude,
            "status": "SUCCESS",
            "weather_file": str(output_file)
        })

        print("  Weather ingestion: SUCCESS ✅")

    except Exception as error:

        failed += 1

        weather_summary.append({
            "trek_id": trek_id,
            "trek_name": trek_name,
            "latitude": latitude,
            "longitude": longitude,
            "status": "FAILED",
            "weather_file": "",
            "error": str(error)
        })

        print(f"  Weather ingestion: FAILED ❌")
        print(f"  Error: {error}")

    # Small delay between API requests.

    time.sleep(0.2)


# ============================================================
# WEATHER INGESTION SUMMARY
# ============================================================

summary_df = pd.DataFrame(weather_summary)

summary_file = (
    PROCESSED_DIR
    / "weather_ingestion_summary.csv"
)

summary_df.to_csv(
    summary_file,
    index=False
)


# ============================================================
# FINAL RESULTS
# ============================================================

print("\n" + "=" * 70)
print("WEATHER INGESTION SUMMARY")
print("=" * 70)

print(f"\nValid treks processed: {len(df)}")
print(f"Successful requests:   {successful}")
print(f"Failed requests:       {failed}")

print("\nWeather summary saved to:")
print(summary_file)

print("\nRaw weather files saved to:")
print(RAW_WEATHER_DIR)


# ============================================================
# FINAL VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("FINAL WEATHER VALIDATION")
print("=" * 70)

if successful == len(df) and failed == 0:

    print("\n✅ WEATHER INGESTION COMPLETED SUCCESSFULLY")

elif successful > 0:

    print(
        "\n⚠️ WEATHER INGESTION COMPLETED "
        "WITH SOME FAILURES"
    )

else:

    print("\n❌ WEATHER INGESTION FAILED")


print("\n" + "=" * 70)
print("WEATHER INGESTION FINISHED")
print("=" * 70)