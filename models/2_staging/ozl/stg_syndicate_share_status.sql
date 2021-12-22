with syndicate_share_status as (

    select 
        syndicate_share_status_id,
        syndicate_share_status_name
    from {{ source('ozl_node_3', 'syndicate_share_status') }}
)

select *
from syndicate_share_status