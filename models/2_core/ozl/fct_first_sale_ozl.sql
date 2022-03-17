with first_sale as (

    select 
        customer_id,
        sale_id as first_sale_id,
        sale_timestamp as first_sale_timestamp,
        checkout_id as first_checkout_id,
        draw_id as first_draw_id,
        game_offer_id as first_game_offer_id,
        channel as first_channel,
        checkout_sale_data as first_checkout_sale_data,
        row_number() over (partition by customer_id order by sale_timestamp asc, TTV desc) ords 
    from {{ ref('fct_sale_ozl') }}
    where status in ('SYNDICATE_SHARE_PURCHASED', 'SYNDICATE_SHARE_PAID', 'TICKET_PURCHASED', 'TICKET_PAID')
    qualify ords = 1
)

select * 
from first_sale