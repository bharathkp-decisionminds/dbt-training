{{
  config(
    materialized='incremental',
  
  pre_hook=["
             CREATE OR REPLACE EXTERNAL TABLE {{ source('dbt_landing', 'ext_enterprise_survey') }}
                (
                id STRING,
                year INT64,
                industry_code_ANZSIC STRING,
                industry_name_ANZSIC STRING,
                rme_size_grp STRING,
                variable STRING,
                value STRING,
                unit STRING,
                ) OPTIONS (
                    format = 'CSV',
                    uris = ['gs://dbt-training-landing/enterprise/survey/enterprise-survey-*.csv'],
                    skip_leading_rows = 1);
                    
            TRUNCATE TABLE {{ this }};
            "]
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
    lower(_FILE_NAME) as source_file, 
    current_timestamp() as created_ts, 
    'dbt' as created_by, 
    current_timestamp() as last_modified_ts, 
    'dbt' as last_modified_by 
from {{ source('dbt_landing', 'ext_enterprise_survey') }}
where cast(concat(split(split(_FILE_NAME, ".")[0], "-")[4],"-",
split(split(_FILE_NAME, ".")[0], "-")[5],"-",
split(split(_FILE_NAME, ".")[0], "-")[6]) as date) = current_date()