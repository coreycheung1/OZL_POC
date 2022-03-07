with recurring_syndicate_share as (

    select
        concat(syndicate_share_id, '_', database) as syndicate_share_id,
        concat(recurring_purchase_id, '_', database) as recurring_purchase_id
    from {{ source('ozl_node_3', 'recurring_syndicate_share') }}
)

select *
from recurring_syndicate_share
