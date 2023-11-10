{{
  config(
    materialized='incremental',
    unique_key='id',
    database = 'dm-solutions',
    schema = 'dbt_consumption',
    partition_by={
      "field": "created_date",
      "data_type": "timestamp",
      "granularity": "day",
      "time_ingestion_partitioning": true
    }
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
    'dbt' as last_modified_by,
    timestamp_trunc(current_timestamp(), day) as created_date
from {{ ref('stg_enterprise_survey') }}