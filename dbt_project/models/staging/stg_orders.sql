-- models/staging/stg_orders.sql
-- 
-- Cleans and standardises the raw_orders table.
-- Applies type casting, null handling, and adds derived columns.
-- Materialized as view (cheap, always fresh).

with source as (
    select * from {{ source('olist_raw', 'raw_orders') }}
),

cleaned as (
    select
        -- Keys
        order_id,
        customer_id,

        -- Enums
        lower(trim(order_status))                         as order_status,

        -- Timestamps — cast and normalise
        order_purchase_timestamp::timestamp               as ordered_at,
        order_approved_at::timestamp                      as approved_at,
        order_delivered_carrier_date::timestamp           as shipped_at,
        order_delivered_customer_date::timestamp          as delivered_at,
        order_estimated_delivery_date::timestamp          as estimated_delivery_at,

        -- Derived
        date_trunc('day', order_purchase_timestamp)::date as order_date,
        date_trunc('week', order_purchase_timestamp)::date as order_week,
        date_trunc('month', order_purchase_timestamp)::date as order_month,

        -- Is the order actually delivered?
        case
            when lower(order_status) = 'delivered'
             and order_delivered_customer_date is not null
            then true
            else false
        end                                               as is_delivered,

        -- Was delivery late?
        case
            when order_delivered_customer_date is not null
             and order_estimated_delivery_date is not null
             and order_delivered_customer_date > order_estimated_delivery_date
            then true
            else false
        end                                               as is_late_delivery,

        -- Days late (negative = early)
        datediff(
            'day',
            order_estimated_delivery_date,
            order_delivered_customer_date
        )                                                 as days_late,

        _loaded_at

    from source
    where order_id is not null
)

select * from cleaned
