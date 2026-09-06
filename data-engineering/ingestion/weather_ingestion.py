import os
import re
import time
import requests
import pandas as pd


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../..")
)

INPUT_FILE = os.path.join(
    PROJECT_ROOT,
    "data",
    "processed",
    "valid_treks.csv"
)

OUTPUT_DIR = os.path.join(
    PROJECT_ROOT,
    "data",
    "raw",
    "weather"
)

API_URL = "https://api.open-meteo.com/v1/forecast"


# ============================================================
# CREATE OUTPUT DIRECTORY
# ============================================================

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================
# HELPER FUNCTION
# ============================================================

def create_filename(trek_name):
    """
    Convert trek name into a safe filename.
    Example:
    Nag Tibba → nag_tibba.json
    Landour–Lal Tibba → landour_lal_tibba.json
    """

    filename = trek_name.lower()

    # Replace non-alphanumeric characters with underscore
    filename = re.sub(r"[^a-z0-9]+", "_", filename)

    # Remove extra underscores
    filename = filename.strip("_")

    return filename + ".json"


# ============================================================
# MAIN WEATHER INGESTION
# ============================================================

def main():

    print("=" * 70)
    print("WEATHER DATA INGESTION - ALL VALID TREKS")
    print("=" * 70)

    # --------------------------------------------------------
    # Load trek dataset
    # --------------------------------------------------------

    print("\nLoading valid trek dataset...")

    df = pd.read_csv(INPUT_FILE)

    print(f"Total valid treks: {len(df)}")

    # --------------------------------------------------------
    # Process each trek
    # --------------------------------------------------------

    successful = 0
    failed = 0

    for index, trek in df.iterrows():

        trek_id = trek["trek_id"]
        trek_name = trek["trek_name"]

        latitude = trek["start_lat"]
        longitude = trek["start_lon"]

        print("\n" + "-" * 70)
        print(f"Trek {trek_id}: {trek_name}")
        print(f"Latitude: {latitude}")
        print(f"Longitude: {longitude}")

        # ----------------------------------------------------
        # Check coordinates
        # ----------------------------------------------------

        if pd.isna(latitude) or pd.isna(longitude):

            print("Skipping: Missing coordinates ❌")
            failed += 1
            continue

        # ----------------------------------------------------
        # Open-Meteo parameters
        # ----------------------------------------------------

        params = {
            "latitude": latitude,
            "longitude": longitude,

            "current": (
                "temperature_2m,"
                "relative_humidity_2m,"
                "precipitation,"
                "weather_code,"
                "wind_speed_10m"
            ),

            "hourly": (
                "temperature_2m,"
                "relative_humidity_2m,"
                "precipitation,"
                "visibility,"
                "wind_speed_10m,"
                "wind_gusts_10m"
            ),

            "forecast_days": 1,

            "timezone": "Asia/Kolkata"
        }

        # ----------------------------------------------------
        # API request
        # ----------------------------------------------------

        try:

            print("Calling Open-Meteo API...")

            response = requests.get(
                API_URL,
                params=params,
                timeout=30
            )

            print(f"HTTP Status Code: {response.status_code}")

            response.raise_for_status()

            weather_data = response.json()

            # ------------------------------------------------
            # Save raw JSON
            # ------------------------------------------------

            filename = create_filename(trek_name)

            output_file = os.path.join(
                OUTPUT_DIR,
                filename
            )

            # Add trek information to raw response
            weather_data["trek_id"] = int(trek_id)
            weather_data["trek_name"] = trek_name

            with open(
                output_file,
                "w",
                encoding="utf-8"
            ) as file:

                import json

                json.dump(
                    weather_data,
                    file,
                    indent=4,
                    ensure_ascii=False
                )

            print("API request successful! ✅")
            print(f"Weather JSON saved: {filename}")

            successful += 1

        except requests.exceptions.RequestException as e:

            print(f"API request failed ❌")
            print(f"Error: {e}")

            failed += 1

        except Exception as e:

            print(f"Unexpected error ❌")
            print(f"Error: {e}")

            failed += 1

        # ----------------------------------------------------
        # Small delay between API requests
        # ----------------------------------------------------

        time.sleep(1)


    # ========================================================
    # FINAL SUMMARY
    # ========================================================

    print("\n")
    print("=" * 70)
    print("WEATHER INGESTION COMPLETED")
    print("=" * 70)

    print(f"\nTotal treks:      {len(df)}")
    print(f"Successful:       {successful}")
    print(f"Failed/Skipped:   {failed}")

    print(f"\nWeather files saved in:")
    print(OUTPUT_DIR)

    print("=" * 70)


# ============================================================
# RUN PROGRAM
# ============================================================

if __name__ == "__main__":
    main()