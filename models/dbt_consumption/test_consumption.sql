{{
  config(
    materialized='incremental',
    database = 'dm-solutions',
    schema = 'dbt_consumption',
    pre_hook=["TRUNCATE TABLE {{ this }}"]
)
}}



select id, first_name, last_name, current_timestamp() as created_ts from {{ ref('test_staging') }}