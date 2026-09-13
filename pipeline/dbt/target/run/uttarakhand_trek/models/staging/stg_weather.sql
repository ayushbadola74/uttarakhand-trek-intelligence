
  create or replace   view UTTARAKHAND_TREK_DB.TREK_ANALYTICS.stg_weather
  
  
  
  
  as (
    

SELECT *
FROM UTTARAKHAND_TREK_DB.TREK_ANALYTICS.WEATHER_GOLD
  );

