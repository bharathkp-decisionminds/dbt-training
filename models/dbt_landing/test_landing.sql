{{
  config(
    materialized='incremental',
  
  pre_hook=["CREATE OR REPLACE EXTERNAL TABLE {{ source('dbt_landing', 'test_csv_external') }}(
                id INT64,
                first_name STRING,
                last_name STRING
                ) OPTIONS (
                    format = 'CSV',
                    uris = ['gs://dbt-training-landing/test/test_csv.csv'],
                    skip_leading_rows = 1);
                    
            TRUNCATE TABLE {{ this }};
            "]
)
}}



select *, current_timestamp() as created_ts from {{ source('dbt_landing', 'test_csv_external') }}