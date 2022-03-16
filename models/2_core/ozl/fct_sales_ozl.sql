with ticket as (

    select
        source,
        ticket_timestamp as sale_timestamp,
        customer_id,
        checkout_id,
        ticket_id,
        null as syndicate_share_id,
        account_transaction_id,
        draw_id,
        game_offer_id,
        ticket_status_name as status,
        recurring_purchase_id,
        channel,
        checkout_sale_data,
        null as syndicate_session_id,
        null as syndicate_id,
        null as syndicate_share_count
    from {{ ref('stg_ticket_ozl') }}
),

syndicate_share as (

    select
        source,
        syndicate_share_timestamp as sale_timestamp,
        customer_id,
        checkout_id,
        null as ticket_id,
        syndicate_share_id,
        account_transaction_id,
        draw_id,
        null as game_offer_id,
        syndicate_share_status_name as status,
        recurring_purchase_id,
        channel,
        checkout_sale_data,
        syndicate_session_id,
        syndicate_id,
        syndicate_share_count
    from {{ ref('stg_syndicate_share_ozl') }}
),

sales_unioned as (

    select * from ticket
    union all
    select * from syndicate_share
),

account_transaction as (

    select * from {{ ref('stg_account_transaction_ozl') }}
),

final as (

    select 
        md5(coalesce(s.ticket_id, s.syndicate_share_id)) as sale_id,
        s.source,
        s.sale_timestamp,
        s.customer_id,
        s.checkout_id,
        s.ticket_id,
        s.syndicate_share_id,
        s.account_transaction_id,
        at.transaction_amount as TTV,
        s.draw_id,
        s.game_offer_id,
        s.status,
        s.recurring_purchase_id,
        s.channel,
        s.checkout_sale_data,
        s.syndicate_session_id,
        s.syndicate_id,
        s.syndicate_share_count
    from sales_unioned s
    left join account_transaction at on at.account_transaction_id = s.account_transaction_id
)

select * 
from final