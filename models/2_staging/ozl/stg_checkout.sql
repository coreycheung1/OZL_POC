with n3 as (
    
    select *
    from {{ ref('base_checkout_ozl_n3') }}
),

n4 as (
    
    select *
    from {{ ref('base_checkout_ozl_n4') }}
),

checkout_unioned as (

    select * from n3
    union all
    select * from n4
),

channel as (

    select * from {{ ref('stg_channel') }}
),

final as (

    select 
        checkout_unioned.checkout_id,
        checkout_unioned.checkout_timestamp,
        checkout_unioned.customer_id,
        channel.channel_name as channel,
        checkout_unioned.checkout_sale_data,
        checkout_unioned.source
    from checkout_unioned
    join channel on checkout_unioned.channel_id = channel.channel_id
)

select *
from final
