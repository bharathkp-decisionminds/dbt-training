{{
  config(
    materialized='table',
  
  pre_hook=["CREATE OR REPLACE EXTERNAL TABLE {{ env_var('DBT_GCP_PROJECT_ID') }}.{{ env_var('DBT_BQ_LANDING_DATASET') }}.test_csv (
                id INT64,
                first_name STRING,
                last_name STRING
                ) OPTIONS (
                    format = 'CSV',
                    uris = ['gs://dbt-training-landing/test/test_csv.csv'],
                    skip_leading_rows = 1);"]
)
}}



select * from {{ env_var('DBT_GCP_PROJECT_ID') }}.{{ env_var('DBT_BQ_LANDING_DATASET') }}.test_csv