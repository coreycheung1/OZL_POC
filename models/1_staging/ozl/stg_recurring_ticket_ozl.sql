with recurring_ticket_unioned as (

    {{ union_node_sources('ozl', 4, 'recurring_ticket') }}
),

recurring_ticket_cleaned as (

    select
        source,
        concat(ticket_id, '_', source) as ticket_id,
        concat(recurring_purchase_id, '_', source) as recurring_purchase_id
    from recurring_ticket_unioned 
),

final as (

    select
        source,
        ticket_id,
        recurring_purchase_id
    from recurring_ticket_cleaned 
)

select *
from final