with fct_sale as (

    select * from {{ ref('fct_sale_ozl') }}
),

dim_draw as (

    select * from {{ ref('dim_draw_ozl') }}
),

dim_lottery as (

    select * from {{ ref('dim_lottery_ozl') }}
),

dim_game_offer as (

    select * from {{ ref('dim_game_offer_ozl') }}
),

final as (

    select
        s.source,
        s.sale_id,
        s.sale_timestamp,
        s.customer_id,
        s.checkout_id,
        s.ticket_id,
        s.syndicate_share_id,
        s.account_transaction_id,
        s.TTV,
        d.draw_no,
        d.draw_prize_pool,
        d.draw_stop,
        l.lottery_name,
        l.lottery_type_name,
        go.game_offer_name,
        go.equivalent_standard_games_count,
        s.status,
        s.recurring_purchase_id,
        s.channel,
        s.checkout_sale_data,
        s.syndicate_session_id,
        s.syndicate_id,
        s.syndicate_share_count
    from fct_sale s
    left join dim_draw d on s.draw_id = d.draw_id
    left join dim_lottery l on d.lottery_id = l.lottery_id
    left join dim_game_offer go on s.game_offer_id = go.game_offer_id
)

select * 
from final

