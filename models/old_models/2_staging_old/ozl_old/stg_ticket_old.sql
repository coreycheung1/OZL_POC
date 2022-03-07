with n3 as (

    select * from {{ ref('base_ticket_ozl_n3') }}
),

n4 as (

    select * from {{ ref('base_ticket_ozl_n4') }}
),

ticket_unioned as (

    select * from n3
    union all
    select * from n4
),

ticket_status as (

  select * from {{ ref('stg_ticket_status') }}
),

recurring_ticket as (

    select * from {{ ref('stg_recurring_ticket') }}
),

checkout as (

    select * from {{ ref('stg_checkout') }}
),

final as (

    select 
        ticket_unioned.ticket_id,
        ticket_unioned.ticket_timestamp,
        ticket_unioned.game_offer_id,
        ticket_unioned.draw_id,
        ticket_unioned.account_transaction_id,
        ticket_status.ticket_status_name,
        ticket_unioned.checkout_id,
        recurring_ticket.recurring_purchase_id,
        checkout.customer_id,
        checkout.channel,
        checkout.checkout_sale_data,
        ticket_unioned.source
    from ticket_unioned
    left join ticket_status on ticket_unioned.ticket_status_id = ticket_status.ticket_status_id
    left join recurring_ticket on ticket_unioned.ticket_id = recurring_ticket.ticket_id
    left join checkout on ticket_unioned.checkout_id = checkout.checkout_id
)

select * from final