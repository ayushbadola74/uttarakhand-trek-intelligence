
    
    

select
    trek_id as unique_field,
    count(*) as n_records

from UTTARAKHAND_TREK_DB.TREK_ANALYTICS.stg_treks
where trek_id is not null
group by trek_id
having count(*) > 1


