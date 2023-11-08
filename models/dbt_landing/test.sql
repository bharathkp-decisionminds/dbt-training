{{
  config(
    materialized='table'
  )
  pre_hook=["CREATE OR REPLACE EXTERNAL TABLE dbt_landing.test_csv (
                id INT64,
                first_name STRING,
                last_name STRING
                ) OPTIONS (
                    format = 'CSV',
                    uris = ['gs://dbt-training-landing/test/test_csv.csv'],
                    skip_leading_rows = 1);"]
}}

with customers as (

    select
        id as customer_id,
        first_name,
        last_name

    from   {{ env_var("DBT_GCP_PROJECT_ID").env_var("DBT_BQ_LANDING_DATASET").env_var("DBT_GCP_PROJECT_ID") }}
    `dbt-tutorial`.jaffle_shop.customers

),

orders as (

    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status

    from `dbt-tutorial`.jaffle_shop.orders

),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1

),

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders

    from customers

    left join customer_orders using (customer_id)

)

select * from final