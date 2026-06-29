-- models/mart/finance/category_revenue_monthly.sql
-- Monthly revenue breakdown by product category (English name)

{{
    config(
        materialized='table',
        sort=['order_month', 'total_gmv'],
        tags=['mart', 'finance']
    )
}}

with items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
    where order_date >= '{{ var("start_date") }}'
      and order_status not in ('canceled', 'unavailable')
),

products as (
    select * from {{ ref('stg_products') }}
),

joined as (
    select
        date_trunc('month', o.order_date)::date     as order_month,
        p.category_en,
        count(distinct o.order_id)                  as order_count,
        count(i.order_item_id)                      as items_sold,
        round(sum(i.item_price), 2)                 as total_gmv,
        round(avg(i.item_price), 2)                 as avg_item_price,
        round(sum(i.freight_value), 2)              as total_freight
    from items i
    join orders o   on i.order_id  = o.order_id
    join products p on i.product_id = p.product_id
    group by 1, 2
),

ranked as (
    select
        *,
        rank() over (
            partition by order_month
            order by total_gmv desc
        )                       as revenue_rank_in_month,
        current_timestamp       as dbt_updated_at
    from joined
)

select * from ranked
