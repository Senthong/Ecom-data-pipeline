-- models/staging/stg_order_items.sql
with source as (
    select * from {{ source('olist_raw', 'raw_order_items') }}
),
cleaned as (
    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date::timestamp              as shipping_limit_at,
        price::decimal(10,2)                        as item_price,
        freight_value::decimal(10,2)                as freight_value,
        (price + freight_value)::decimal(10,2)      as total_item_revenue
    from source
    where order_id is not null
      and price >= 0
      and freight_value >= 0
)
select * from cleaned
