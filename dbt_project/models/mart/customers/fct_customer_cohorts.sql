-- models/mart/customers/fct_customer_cohorts.sql
-- 
-- Monthly cohort retention analysis.
-- For each cohort (month of first purchase), tracks how many customers
-- purchased again in subsequent months (M+1, M+2, ...).
-- Classic retention table used in e-commerce analytics.

{{
    config(
        materialized='table',
        sort=['cohort_month', 'months_since_first_order'],
        dist='cohort_month',
        tags=['mart', 'customers', 'cohort']
    )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
    where order_date >= '{{ var("start_date") }}'
      and order_status not in ('canceled', 'unavailable')
),

-- Map each order to the unique customer (not per-order customer_id)
customer_orders as (
    select
        c.customer_unique_id,
        o.order_date,
        date_trunc('month', o.order_date)::date     as order_month,
        o.order_id
    from orders o
    join customers c on o.customer_id = c.customer_id
),

-- First order date per unique customer
first_orders as (
    select
        customer_unique_id,
        min(order_month)    as cohort_month,
        min(order_date)     as first_order_date
    from customer_orders
    group by 1
),

-- Join back to get months since first order for each subsequent purchase
customer_activity as (
    select
        co.customer_unique_id,
        fo.cohort_month,
        co.order_month,
        datediff(
            'month',
            fo.cohort_month,
            co.order_month
        )                   as months_since_first_order
    from customer_orders co
    join first_orders fo on co.customer_unique_id = fo.customer_unique_id
),

-- Cohort size (how many unique customers acquired per month)
cohort_sizes as (
    select
        cohort_month,
        count(distinct customer_unique_id)  as cohort_size
    from first_orders
    group by 1
),

-- Retained customers per cohort-month combination
retention_counts as (
    select
        cohort_month,
        months_since_first_order,
        count(distinct customer_unique_id)  as retained_customers
    from customer_activity
    group by 1, 2
),

final as (
    select
        rc.cohort_month,
        cs.cohort_size,
        rc.months_since_first_order,
        rc.retained_customers,
        round(
            100.0 * rc.retained_customers / cs.cohort_size,
            2
        )                                   as retention_rate_pct,
        current_timestamp                   as dbt_updated_at
    from retention_counts rc
    join cohort_sizes cs on rc.cohort_month = cs.cohort_month
    where rc.months_since_first_order <= 12   -- track up to 12 months
)

select * from final
order by cohort_month, months_since_first_order
