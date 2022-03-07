with channel as (
    
    select 
        channel_id,
        channel_name
    from {{ source('ozl_node_3', 'channel') }}
)

select *
from channel