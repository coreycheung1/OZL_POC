with checkout as (

    select
        concat(checkout_id, '_', database) as checkout_id,
        convert_timezone('UTC', 'Australia/Brisbane', checkout_timestamp) as checkout_timestamp,
        customer_id,
        channel_id,
        checkout_sale_data,
        database as source
    from {{ source('ozl_node_3', 'checkout') }}
)

select *
from checkout
