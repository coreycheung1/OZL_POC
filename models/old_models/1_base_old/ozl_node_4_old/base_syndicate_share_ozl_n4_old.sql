with syndicate_share as (

    select
        concat(syndicate_share_id, '_', database) syndicate_share_id,
        convert_timezone('UTC', 'Australia/Brisbane', syndicate_share_timestamp) as syndicate_share_timestamp,
        concat(syndicate_session_id, '_', database) syndicate_session_id,
        customer_id,
        syndicate_share_count,
        concat(account_transaction_id, '_', database) as account_transaction_id,
        concat(checkout_id, '_', database) as checkout_id,
        syndicate_share_status_id,
        concat(syndicate_id, '_', database) as syndicate_id,
        draw_id,
        database as source
    from {{ source('ozl_node_4', 'syndicate_share') }}
)

select *
from syndicate_share