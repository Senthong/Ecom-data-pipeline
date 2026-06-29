-- models/staging/stg_order_reviews.sql
with source as (
    select * from {{ source('olist_raw', 'raw_order_reviews') }}
),
cleaned as (
    select
        review_id,
        order_id,
        review_score::smallint                          as review_score,
        -- Sentiment bucket
        case
            when review_score >= 4 then 'positive'
            when review_score = 3  then 'neutral'
            else 'negative'
        end                                             as sentiment,
        nullif(trim(review_comment_title), '')          as review_title,
        nullif(trim(review_comment_message), '')        as review_body,
        review_creation_date::timestamp                 as review_created_at,
        review_answer_timestamp::timestamp              as review_answered_at,
        datediff(
            'day',
            review_creation_date,
            review_answer_timestamp
        )                                               as days_to_answer
    from source
    where order_id is not null
      and review_score between 1 and 5
)
select * from cleaned
