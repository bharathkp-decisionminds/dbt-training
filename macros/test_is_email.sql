{% macro test_is_email(model, column_name) %}

with validation as (

    select
        {{column_name}} as email_field

    from {{model}}

),

validation_errors as (

    select
        email_field
    from validation
    where (email_field not like '%@%') and (email_field not like '%.com')

)

select *
from validation_errors

{% endmacro %}