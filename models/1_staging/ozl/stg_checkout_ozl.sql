with checkout_unioned as (

    {{ union_node_sources('ozl', 4, 'checkout')}}
),

checkout_cleaned as (
    select
        source,
        concat(checkout_id, '_', source) as checkout_id,
        convert_timezone('UTC', 'Australia/Brisbane', checkout_timestamp) as checkout_timestamp,
        customer_id,
        channel_id,
        checkout_sale_data
    from checkout_unioned
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
    from checkout_cleaned c 
    join channel ch on c.channel_id = ch.channel_id
)

select * 
from final