{{ config(materialized='view') }}

SELECT *
FROM {{ source('trek_analytics', 'WEATHER_GOLD') }}