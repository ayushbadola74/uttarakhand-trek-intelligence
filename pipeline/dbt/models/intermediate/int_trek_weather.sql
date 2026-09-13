{{ config(materialized='view') }}

SELECT
    t.*,
    w."weather_records",
    w."avg_temperature_c",
    w."min_temperature_c",
    w."max_temperature_c",
    w."avg_humidity_pct",
    w."weather_date",
    w."weather_time",
    w."temperature_c",
    w."total_precipitation_mm",
    w."humidity_pct",
    w."avg_visibility_m",
    w."min_visibility_m",
    w."precipitation_mm",
    w."visibility_m",
    w."max_wind_speed_kmh",
    w."max_wind_gust_kmh",
    w."wind_speed_kmh",
    w."avg_condition_score",
    w."wind_gust_kmh",
    w."has_red_condition",
    w."temperature_category",
    w."precipitation_category",
    w."has_yellow_condition",
    w."overall_condition"
FROM {{ ref('stg_treks') }} t
LEFT JOIN {{ ref('stg_weather') }} w
    ON t."trek_id" = w."trek_id"