with n3 as (

    select * from {{ ref('base_syndicate_share_ozl_n3') }}
),

n4 as (

    select * from {{ ref('base_syndicate_share_ozl_n4') }}
),

syndicate_share_unioned as (

    select * from n3
    union all
    select * from n4
),

syndicate_share_status as (

    select * from {{ ref('stg_syndicate_share_status') }}
),

draw as (

    select * from {{ ref('stg_draw') }}
),

lottery as (

    select * from {{ ref('stg_lottery') }}
),

final as (

    select 
        syndicate_share_unioned.syndicate_share_id,
        syndicate_share_unioned.syndicate_share_timestamp,
        syndicate_share_unioned.syndicate_session_id,
        syndicate_share_unioned.customer_id,
        syndicate_share_unioned.syndicate_share_count,
        syndicate_share_unioned.account_transaction_id,
        syndicate_share_unioned.checkout_id,
        syndicate_share_status.syndicate_share_status_name,
        syndicate_share_unioned.syndicate_id,
        lottery.lottery_name,
        draw.draw_no,
        draw.draw_date,
        draw.draw_prize_pool
    from syndicate_share_unioned
    left join syndicate_share_status on syndicate_share_unioned.syndicate_share_status_id = syndicate_share_status.syndicate_share_status_id
    left join draw on syndicate_share_unioned.draw_id = draw.draw_id
    left join lottery on draw.lottery_id = lottery.lottery_id
)

select * from final