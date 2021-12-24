with n3 as (

    select * from {{ ref('base_customer_ozl_n3') }}
),

n4 as (

    select * from {{ ref('base_customer_ozl_n4') }}
),

customer_unioned as (

    select * from n3
    union all
    select * from n4
),

final as (

    select
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
        signup_timestamp,
        source
    from customer_unioned
)

select *
from final

