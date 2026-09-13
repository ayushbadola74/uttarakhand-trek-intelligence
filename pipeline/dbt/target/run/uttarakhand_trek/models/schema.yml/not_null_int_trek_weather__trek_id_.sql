
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select "trek_id"
from UTTARAKHAND_TREK_DB.TREK_ANALYTICS.int_trek_weather
where "trek_id" is null



  
  
      
    ) dbt_internal_test