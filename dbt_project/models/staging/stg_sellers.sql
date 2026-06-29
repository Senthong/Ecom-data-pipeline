-- models/staging/stg_sellers.sql
with source as (
    select * from {{ source('olist_raw', 'raw_sellers') }}
),
cleaned as (
    select
        seller_id,
        lpad(trim(seller_zip_code), 8, '0')  as zip_code,
        initcap(lower(trim(seller_city)))    as city,
        upper(trim(seller_state))            as state_code
    from source
    where seller_id is not null
)
select * from cleaned
