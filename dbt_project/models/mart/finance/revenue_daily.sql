-- models/mart/finance/revenue_daily.sql
-- 
-- Daily GMV (Gross Merchandise Value) report.
-- Joins orders + order_items + payments to get true revenue per day.
-- Materialized as TABLE on Redshift with sort/dist keys for fast BI queries.
--
-- Key metrics:
--   gmv         — sum of item prices (pre-freight)
--   total_revenue — gmv + freight
--   avg_order_value — revenue / distinct orders
--   cancellation_rate — % orders canceled that day

{{
    config(
        materialized='table',
        sort='order_date',
        dist='order_date',
        tags=['mart', 'finance', 'daily']
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
    where order_date >= '{{ var("start_date") }}'
),

items as (
    select * from {{ ref('stg_order_items') }}
),

payments as (
    select
        order_id,
        sum(payment_value)    as total_payment_value,
        max(payment_type)     as primary_payment_type   -- simplification for 1-row-per-order
    from {{ ref('stg_order_payments') }}
    group by 1
),

order_revenue as (
    select
        o.order_id,
        o.order_date,
        o.order_status,
        o.is_delivered,
        o.is_late_delivery,
        sum(i.item_price)           as gmv,
        sum(i.freight_value)        as freight_revenue,
        sum(i.total_item_revenue)   as gross_revenue,
        count(distinct i.product_id) as distinct_products,
        count(i.order_item_id)      as item_count,
        p.total_payment_value,
        p.primary_payment_type
    from orders o
    left join items i    on o.order_id = i.order_id
    left join payments p on o.order_id = p.order_id
    group by 1, 2, 3, 4, 5, p.total_payment_value, p.primary_payment_type
),

daily_aggregated as (
    select
        order_date,

        -- Volume
        count(distinct order_id)                                as total_orders,
        count(distinct case when is_delivered then order_id end) as delivered_orders,
        count(distinct case when order_status = 'canceled'
                           then order_id end)                   as canceled_orders,

        -- Revenue
        round(sum(gmv), 2)                                      as gmv,
        round(sum(freight_revenue), 2)                          as freight_revenue,
        round(sum(gross_revenue), 2)                            as total_revenue,
        round(avg(gross_revenue), 2)                            as avg_order_value,

        -- Delivery quality
        round(
            100.0 * count(distinct case when is_late_delivery then order_id end)
            / nullif(count(distinct case when is_delivered then order_id end), 0),
            2
        )                                                       as late_delivery_pct,

        -- Cancellation rate
        round(
            100.0 * count(distinct case when order_status = 'canceled'
                                        then order_id end)
            / nullif(count(distinct order_id), 0),
            2
        )                                                       as cancellation_rate_pct,

        -- Items
        sum(item_count)                                         as total_items_sold,
        round(avg(item_count), 2)                               as avg_items_per_order,

        -- Payment mix
        sum(case when primary_payment_type = 'credit_card'
                 then 1 else 0 end)                             as orders_credit_card,
        sum(case when primary_payment_type = 'boleto'
                 then 1 else 0 end)                             as orders_boleto,

        current_timestamp                                       as dbt_updated_at

    from order_revenue
    group by 1
)

select * from daily_aggregated
order by order_date
