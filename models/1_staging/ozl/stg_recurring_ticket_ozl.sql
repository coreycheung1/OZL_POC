with unioned_recurring_ticket as (

    {{ union_node_sources('ozl', 4, 'recurring_ticket') }}
),

final as (

    select
        source,
        concat(ticket_id, '_', database) as ticket_id,
        concat(recurring_purchase_id, '_', database) as recurring_purchase_id
    from unioned_recurring_ticket 
)

select *
from final