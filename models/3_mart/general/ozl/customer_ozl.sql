with customer as (

    select * from {{ ref('dim_customer_ozl') }}
)

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
    signup_timestamp,
    first_sale_id,
    first_sale_timestamp,
    first_checkout_id,
    first_draw_id,
    first_game_offer_id,
    first_channel,
    first_checkout_sale_data

from customer