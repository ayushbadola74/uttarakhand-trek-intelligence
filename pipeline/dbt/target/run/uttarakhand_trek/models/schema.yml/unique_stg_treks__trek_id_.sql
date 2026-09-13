
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    "trek_id" as unique_field,
    count(*) as n_records

from UTTARAKHAND_TREK_DB.TREK_ANALYTICS.stg_treks
where "trek_id" is not null
group by "trek_id"
having count(*) > 1



  
  
      
    ) dbt_internal_test