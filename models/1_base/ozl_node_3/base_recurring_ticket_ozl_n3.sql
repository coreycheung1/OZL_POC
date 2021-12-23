with recurring_ticket as (

    select
        concat(ticket_id, '_', database) as ticket_id,
        concat(recurring_purchase_id, '_', database) as recurring_purchase_id
    from {{ source('ozl_node_3', 'recurring_ticket') }}
)

select * 
from recurring_ticket
