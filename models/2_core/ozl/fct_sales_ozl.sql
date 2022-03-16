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

final as (

    select 
        md5(coalesce(ticket_id, syndicate_share_id)) as sale_id,
        source,
        sale_timestamp,
        customer_id,
        checkout_id,
        ticket_id,
        syndicate_share_id,
        account_transaction_id,
        draw_id,
        game_offer_id,
        status,
        recurring_purchase_id,
        channel,
        checkout_sale_data,
        syndicate_session_id,
        syndicate_id,
        syndicate_share_count
    from sales_unioned
)

select * 
from final