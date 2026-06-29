-- models/mart/operations/seller_performance.sql
-- 
-- Seller-level performance scorecard.
-- Aggregates revenue, order count, avg review score, late delivery rate.

{{
    config(
        materialized='table',
        dist='seller_id',
        tags=['mart', 'operations']
    )
}}

with sellers as (
    select * from {{ ref('stg_sellers') }}
),

items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
    where order_date >= '{{ var("start_date") }}'
),

reviews as (
    select * from {{ ref('stg_order_reviews') }}
),

-- revenue per seller
seller_revenue as (
    select
        i.seller_id,
        count(distinct i.order_id)          as total_orders,
        count(i.order_item_id)              as total_items_sold,
        round(sum(i.item_price), 2)         as total_gmv,
        round(avg(i.item_price), 2)         as avg_item_price,
        round(sum(i.freight_value), 2)      as total_freight,
        min(o.order_date)                   as first_order_date,
        max(o.order_date)                   as last_order_date
    from items i
    join orders o on i.order_id = o.order_id
    group by 1
),

-- review scores per seller
seller_reviews as (
    select
        i.seller_id,
        round(avg(r.review_score), 2)       as avg_review_score,
        count(r.review_id)                  as total_reviews,
        sum(case when r.sentiment = 'negative' then 1 else 0 end) as negative_reviews
    from items i
    join reviews r on i.order_id = r.order_id
    group by 1
),

-- delivery performance per seller
seller_delivery as (
    select
        i.seller_id,
        count(distinct o.order_id)           as delivered_orders,
        sum(case when o.is_late_delivery then 1 else 0 end) as late_deliveries
    from items i
    join orders o on i.order_id = o.order_id
    where o.is_delivered = true
    group by 1
),

final as (
    select
        s.seller_id,
        s.city                                              as seller_city,
        s.state_code                                        as seller_state,

        -- Revenue
        coalesce(r.total_orders, 0)                         as total_orders,
        coalesce(r.total_items_sold, 0)                     as total_items_sold,
        coalesce(r.total_gmv, 0)                            as total_gmv,
        coalesce(r.avg_item_price, 0)                       as avg_item_price,
        r.first_order_date,
        r.last_order_date,

        -- Reviews
        coalesce(rv.avg_review_score, 0)                    as avg_review_score,
        coalesce(rv.total_reviews, 0)                       as total_reviews,
        round(
            100.0 * coalesce(rv.negative_reviews, 0)
            / nullif(coalesce(rv.total_reviews, 0), 0),
            2
        )                                                   as negative_review_pct,

        -- Delivery
        round(
            100.0 * coalesce(d.late_deliveries, 0)
            / nullif(coalesce(d.delivered_orders, 0), 0),
            2
        )                                                   as late_delivery_pct,

        -- Composite score: simple weighted rank signal (0–100)
        round(
            (coalesce(rv.avg_review_score, 3) / 5.0 * 50)   -- 50% weight on reviews
            + (1 - coalesce(d.late_deliveries, 0)
                / nullif(coalesce(d.delivered_orders, 1), 0)) * 50,  -- 50% on on-time
            1
        )                                                   as seller_score,

        current_timestamp                                   as dbt_updated_at

    from sellers s
    left join seller_revenue  r  on s.seller_id = r.seller_id
    left join seller_reviews  rv on s.seller_id = rv.seller_id
    left join seller_delivery d  on s.seller_id = d.seller_id
)

select * from final
