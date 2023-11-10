{{
  config(
    materialized='table',
    database = 'dm-solutions',
    schema = 'dbt_staging',
    pre_hook=[" select current_date();
                --TRUNCATE TABLE {{ this }}; "]
)
}}

select 
    id,
    year,
    industry_code_ANZSIC,
    industry_name_ANZSIC,
    rme_size_grp,
    variable,
    value,
    unit, 
    source_file, 
    current_timestamp() as created_ts, 
    'dbt' as created_by, 
    current_timestamp() as last_modified_ts, 
    'dbt' as last_modified_by 
from {{ ref('lndg_enterprise_survey') }}