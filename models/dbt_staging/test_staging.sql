{{
  config(
    materialized='table',
    database = 'dm-solutions',
    schema = 'dbt_staging'
)
}}



select id, first_name, last_name, current_timestamp() as created_ts from {{ ref('test_landing') }}