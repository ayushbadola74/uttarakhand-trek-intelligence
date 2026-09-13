{{ config(materialized='view') }}

SELECT *
FROM {{ source('trek_analytics', 'TREKS_GOLD') }}