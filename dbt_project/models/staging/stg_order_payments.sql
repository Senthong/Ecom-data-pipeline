-- models/staging/stg_order_payments.sql
with source as (
    select * from {{ source('olist_raw', 'raw_order_payments') }}
),
cleaned as (
    select
        order_id,
        payment_sequential,
        lower(payment_type)             as payment_type,
        payment_installments,
        payment_value::decimal(10,2)    as payment_value
    from source
    where order_id is not null
      and payment_value >= 0
)
select * from cleaned
