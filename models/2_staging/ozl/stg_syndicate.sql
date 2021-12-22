with syndicate as (

    select 
        syndicate_id,
        convert_timezone('UTC', 'Australia/Brisbane', syndicate_timestamp) as syndicate_timestamp,
        syndicate_name,
        syndicate_public,
        syndicate_desc,
        syndicate_size,
        syndicate_type_id,
        syndicate_type_name,
        syndicate_cost_per_share
    from {{ source('ozl_node_3', 'syndicate') }}
)

select * 
from syndicate
