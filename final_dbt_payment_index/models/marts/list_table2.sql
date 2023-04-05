
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (

  select index, isFraud,type, amount from {{ref('dataset_index')}} 



)

select *
from source_data order by index asc

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
