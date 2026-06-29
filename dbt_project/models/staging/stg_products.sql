-- models/staging/stg_products.sql
with products as (
    select * from {{ source('olist_raw', 'raw_products') }}
),
translations as (
    select * from {{ source('olist_raw', 'raw_category_translation') }}
),
joined as (
    select
        p.product_id,
        coalesce(t.product_category_name_english, 'uncategorized')  as category_en,
        p.product_category_name                                      as category_pt,
        p.product_name_length,
        p.product_description_length,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm,
        -- volumetric weight in grams (L*H*W / 5000 * 1000)
        round(
            (p.product_length_cm * p.product_height_cm * p.product_width_cm) / 5.0,
            2
        )                                                            as volumetric_weight_g
    from products p
    left join translations t
        on p.product_category_name = t.product_category_name
    where p.product_id is not null
)
select * from joined
