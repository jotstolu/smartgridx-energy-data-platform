{{ config(materialized='table') }}

with date_spine as (

    select explode(
        sequence(
            to_date('2026-01-01'),
            to_date('2026-12-31'),
            interval 1 day
        )
    ) as date_day

)

select
    cast(date_format(date_day, 'yyyyMMdd') as int) as date_sk,
    date_day,
    year(date_day) as year_number,
    quarter(date_day) as quarter_number,
    month(date_day) as month_number,
    date_format(date_day, 'MMMM') as month_name,
    dayofmonth(date_day) as day_of_month,
    dayofweek(date_day) as day_of_week,
    date_format(date_day, 'E') as day_name,
    weekofyear(date_day) as week_of_year,
    case
        when dayofweek(date_day) in (1, 7) then true
        else false
    end as is_weekend,
    cast(date_trunc('month', date_day) as date) as month_start_date,
    last_day(date_day) as month_end_date,
    {{ add_model_audit_columns() }}
from date_spine