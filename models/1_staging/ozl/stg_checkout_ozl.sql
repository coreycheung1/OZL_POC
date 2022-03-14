with unioned_checkout as (

    {{ union_node_sources('ozl', 4, 'checkout')}}
),

channel as (

    select * from {{ ref('stg_channel_ozl') }}
),

final as (

    select
        c.source,
        c.checkout_id,
        c.checkout_timestamp,
        c.customer_id,
        ch.channel_name as channel,
        c.checkout_sale_data       
    from unioned_checkout c 
    join channel ch on c.channel_id = ch.channel_id
)

select * 
from final