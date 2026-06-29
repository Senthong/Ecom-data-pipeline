-- models/staging/stg_customers.sql
with source as (
    select * from {{ source('olist_raw', 'raw_customers') }}
),
cleaned as (
    select
        customer_id,
        customer_unique_id,
        lpad(trim(customer_zip_code), 8, '0')   as zip_code,
        initcap(lower(trim(customer_city)))      as city,
        upper(trim(customer_state))              as state_code
    from source
    where customer_id is not null
)
select * from cleaned
