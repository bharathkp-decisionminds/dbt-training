{{
  config(
    materialized='table'
)
}}



select id, first_name, last_name, current_timestamp() as created_ts from {{ ref('test_landing') }}