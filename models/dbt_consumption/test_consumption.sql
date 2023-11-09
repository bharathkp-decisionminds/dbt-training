{{
  config(
    materialized='table',
    database = 'dm-solutions',
    schema = 'dbt_consumption'
)
}}



select id, first_name, last_name, current_timestamp() as created_ts from {{ ref('test_staging') }}