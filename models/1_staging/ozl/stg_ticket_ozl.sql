with ticket_unioned as (

    {{ union_node_sources('ozl', 4, 'ticket') }}
),

ticket_cleaned as (

    select
        source,
        ticket_id,
        convert_timezone('UTC', 'Australia/Brisbane', ticket_timestamp) as ticket_timestamp,
        game_offer_id,
        draw_id,
        concat(account_transaction_id, '_', source) as account_transaction_id,
        ticket_status_id,
        concat(checkout_id, '_', source) as checkout_id
    from ticket_unioned
),

ticket_status as (

  select * from {{ ref('stg_ticket_status_ozl') }}
),

recurring_ticket as (

    select * from {{ ref('stg_recurring_ticket_ozl') }}
),

checkout as (

    select * from {{ ref('stg_checkout_ozl') }}
),

final as (

    select
        t.source,
        t.ticket_id,
        t.ticket_timestamp,
        t.game_offer_id,
        t.draw_id,
        t.account_transaction_id,
        ts.ticket_status_name,
        t.checkout_id,
        rt.recurring_purchase_id,
        c.customer_id,
        c.channel,
        c.checkout_sale_data
    from ticket_cleaned t
    left join ticket_status ts on t.ticket_status_id = ts.ticket_status_id
    left join recurring_ticket rt on t.ticket_id = rt.ticket_id
    left join checkout c on t.checkout_id = c.checkout_id
)

select *
from final