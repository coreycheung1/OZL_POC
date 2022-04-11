{%- macro dim_customer(pbj) -%}
    
    with customer as (

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
    from {{ ref('stg_customer_' ~ pbj) }}
),

first_sale as (

    select
        customer_id,
        first_sale_id,
        first_sale_timestamp,
        first_checkout_id,
        first_draw_id,
        first_game_offer_id,
        first_channel,
        first_checkout_sale_data
    from {{ ref('fct_first_sale_' ~ pbj) }}
),

final as (

    select 
        c.source,
        c.customer_id,
        c.account_id,
        c.firstname,
        c.lastname,
        c.dob,
        c.address,
        c.suburb,
        c.postcode,
        c.state,
        c.country,
        c.phone,
        c.email,
        c.signup_timestamp,
        fs.first_sale_id,
        fs.first_sale_timestamp,
        fs.first_checkout_id,
        fs.first_draw_id,
        fs.first_game_offer_id,
        fs.first_channel,
        fs.first_checkout_sale_data
    from customer c
    left join first_sale fs on fs.customer_id = c.customer_id
)

select *
from final

{%- endmacro -%}

