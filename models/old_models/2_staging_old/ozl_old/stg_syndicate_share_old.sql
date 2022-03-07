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

recurring_syndicate_share as (

    select * from {{ ref('stg_recurring_syndicate_share') }}
),

checkout as (

    select * from {{ ref('stg_checkout') }}
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
        syndicate_share_unioned.draw_id,
        recurring_syndicate_share.recurring_purchase_id,
        checkout.channel,
        checkout.checkout_sale_data,
        syndicate_share_unioned.source
    from syndicate_share_unioned
    left join syndicate_share_status on syndicate_share_unioned.syndicate_share_status_id = syndicate_share_status.syndicate_share_status_id
    left join recurring_syndicate_share on syndicate_share_unioned.syndicate_share_id = recurring_syndicate_share.syndicate_share_id
    left join checkout on syndicate_share_unioned.checkout_id = checkout.checkout_id
)

select * from final