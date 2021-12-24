with customer as (

    select
        customer_id,
        concat(account_id, '_', database) as account_id,
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
        convert_timezone('UTC', 'Australia/Brisbane', customer_timestamp) as signup_timestamp,
        database as source
    from {{ source('ozl_node_4', 'customer') }}       
)

select * 
from customer