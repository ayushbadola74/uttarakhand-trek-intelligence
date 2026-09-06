import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_FILE = PROJECT_ROOT / "pipeline" / "data" / "treks.csv"
PROCESSED_DIR = PROJECT_ROOT / "pipeline" / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

REQUIRED_COLUMNS = [
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
    "distance_km",
    "duration_days",
    "difficulty",
    "best_season",
    "approx_cost"
]



print("=" * 70)
print("UTTARAKHAND TREK DATA INGESTION")
print("=" * 70)

print("\nReading file:")
print(INPUT_FILE)


# ============================================================
# LOAD DATA
# ============================================================

df = pd.read_csv(INPUT_FILE)

print(f"\nTotal records loaded: {len(df)}")


# ============================================================
# DATA CLEANING
# ============================================================

print("\n" + "=" * 70)
print("DATA CLEANING")
print("=" * 70)

# Remove completely empty columns
empty_columns = df.columns[df.isna().all()].tolist()

if empty_columns:
    print("\nRemoving completely empty columns:")

    for column in empty_columns:
        print(f"  - {column}")

    df = df.drop(columns=empty_columns)

else:
    print("\nNo completely empty columns found.")


# Remove completely empty rows
before_rows = len(df)

df = df.dropna(how="all")

removed_rows = before_rows - len(df)

if removed_rows > 0:
    print(f"\nRemoved completely empty rows: {removed_rows}")
else:
    print("\nNo completely empty rows found.")


# ============================================================
# COLUMN VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("COLUMN VALIDATION")
print("=" * 70)

missing_columns = [
    column for column in REQUIRED_COLUMNS
    if column not in df.columns
]

if missing_columns:

    print("\nMissing required columns:")

    for column in missing_columns:
        print(f"  - {column}")

    raise ValueError("Required columns are missing.")

else:
    print("\nAll required columns are present. ✅")


# ============================================================
# DATA TYPE CLEANING
# ============================================================

numeric_columns = [
    "start_lat",
    "start_lon",
    "start_elevation",
    "end_lat",
    "end_lon",
    "end_elevation"
]

for column in numeric_columns:
    df[column] = pd.to_numeric(df[column], errors="coerce")


# ============================================================
# DATA QUALITY REPORT
# ============================================================

print("\n" + "=" * 70)
print("DATA QUALITY REPORT")
print("=" * 70)

quality_results = []

for column in numeric_columns:

    missing_count = df[column].isna().sum()

    status = "PASS" if missing_count == 0 else "FAIL"

    quality_results.append({
        "check": "Missing values",
        "column": column,
        "total_records": len(df),
        "missing_records": missing_count,
        "status": status
    })


quality_report = pd.DataFrame(quality_results)

print("\n")
print(quality_report.to_string(index=False))


# ============================================================
# SAVE QUALITY REPORT
# ============================================================

quality_report_file = PROCESSED_DIR / "trek_quality_report.csv"

quality_report.to_csv(
    quality_report_file,
    index=False
)

print("\nQuality report saved to:")
print(quality_report_file)


# ============================================================
# COORDINATE VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("COORDINATE VALIDATION")
print("=" * 70)


invalid_latitude = (
    ((df["start_lat"].notna()) & ~df["start_lat"].between(-90, 90)) |
    ((df["end_lat"].notna()) & ~df["end_lat"].between(-90, 90))
)

invalid_longitude = (
    ((df["start_lon"].notna()) & ~df["start_lon"].between(-180, 180)) |
    ((df["end_lon"].notna()) & ~df["end_lon"].between(-180, 180))
)


print(f"\nInvalid latitude records: {invalid_latitude.sum()}")
print(f"Invalid longitude records: {invalid_longitude.sum()}")


if invalid_latitude.sum() == 0:
    print("\nLatitude validation: PASS ✅")
else:
    print("\nLatitude validation: FAIL ❌")


if invalid_longitude.sum() == 0:
    print("Longitude validation: PASS ✅")
else:
    print("Longitude validation: FAIL ❌")


# ============================================================
# IDENTIFY VALID AND SKIPPED TREKS
# ============================================================

print("\n" + "=" * 70)
print("TREK DATASET SPLIT")
print("=" * 70)


# Required location information
location_columns = [
    "start_lat",
    "start_lon",
    "end_lat",
    "end_lon"
]

# A trek is considered valid if all required location
# coordinates are available.
valid_mask = df[location_columns].notna().all(axis=1)

valid_treks = df[valid_mask].copy()
skipped_treks = df[~valid_mask].copy()


# ============================================================
# SAVE VALID TREKS
# ============================================================

valid_file = PROCESSED_DIR / "valid_treks.csv"

valid_treks.to_csv(
    valid_file,
    index=False
)


# ============================================================
# SAVE SKIPPED TREKS
# ============================================================

skipped_file = PROCESSED_DIR / "skipped_treks.csv"

skipped_treks.to_csv(
    skipped_file,
    index=False
)


# ============================================================
# DISPLAY RESULTS
# ============================================================

print(f"\nTotal treks: {len(df)}")

print(f"Valid treks: {len(valid_treks)} ✅")
print(f"Skipped treks: {len(skipped_treks)} ⏸️")


print("\nValid trek file saved to:")
print(valid_file)

print("\nSkipped trek file saved to:")
print(skipped_file)


# ============================================================
# SHOW SKIPPED TREKS
# ============================================================

if len(skipped_treks) > 0:

    print("\n" + "=" * 70)
    print("TREKS SKIPPED DUE TO MISSING LOCATION DATA")
    print("=" * 70)

    print(
        skipped_treks[
            [
                "trek_id",
                "trek_name",
                "starting_point",
                "ending_point",
                "start_lat",
                "start_lon",
                "end_lat",
                "end_lon"
            ]
        ].to_string(index=False)
    )


# ============================================================
# FIRST 5 VALID TREKS
# ============================================================

print("\n" + "=" * 70)
print("FIRST 5 VALID TREKS")
print("=" * 70)

print(
    valid_treks.head(5).to_string(index=False)
)


# ============================================================
# FINAL MESSAGE
# ============================================================

print("\n" + "=" * 70)
print("INGESTION COMPLETED SUCCESSFULLY ✅")
print("=" * 70)