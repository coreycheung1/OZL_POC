with final as (

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
    from {{ ref('stg_customer') }}
)

select * from final
