with ticket as (

    select
        concat(ticket_id, '_', database) ticket_id,
        convert_timezone('UTC', 'Australia/Brisbane', ticket_timestamp) as ticket_timestamp,
        game_offer_id,
        draw_id,
        concat(account_transaction_id, '_', database) as account_transaction_id,
        ticket_status_id,
        concat(checkout_id, '_', database) as checkout_id,
        database as source
    from {{ source('ozl_node_4', 'ticket') }}
)

select *
from ticket