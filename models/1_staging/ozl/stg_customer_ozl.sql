with unioned_customer as (

    {{ union_node_sources('ozl', 4, 'customer')}}
),

final as (

    select
        source,
        customer_id,
        concat(account_id, '_', source) as account_id,
        firstname,
        lastname,
        dob,
        parse_json(address) as address, -- cast to json
        suburb,
        postcode,
        state,
        country,
        phone,
        email,
        convert_timezone('UTC', 'Australia/Brisbane', customer_timestamp) as signup_timestamp
    from unioned_customer
)

select * 
from final