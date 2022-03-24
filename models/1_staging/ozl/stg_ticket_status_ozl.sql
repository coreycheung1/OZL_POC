with ticket_status as (

    select
        ticket_status_id,
        ticket_status_name as ticket_status
    from {{ source('ozl_node_3', 'ticket_status') }}
)

select *
from ticket_status