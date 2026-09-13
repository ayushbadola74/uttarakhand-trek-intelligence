{{ config(materialized='table') }}

SELECT *
FROM {{ ref('int_trek_weather') }}