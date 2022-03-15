with customer_unioned as (

    {{ union_node_sources('ozl', 4, 'customer')}}
),

customer_cleaned as (

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
    from customer_unioned
),

final as (

    select
        source,
        customer_id,
        account_id,
        firstname,
        lastname,
        dob,
        address,
        suburb,
        postcode,
        state,
        country,
        phone,
        email,
        signup_timestamp
    from customer_cleaned
)

select * 
from final