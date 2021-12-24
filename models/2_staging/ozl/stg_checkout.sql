with checkout_ozl_n3 as (
    
    select *
    from {{ source('ozl_node_3', 'checkout') }}
),

with checkout_ozl_n4 as (
    
    select *
    from {{ source('ozl_node_4', 'checkout') }}
)

select *
from channel
