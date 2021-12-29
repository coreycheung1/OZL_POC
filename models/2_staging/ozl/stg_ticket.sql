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

game_offer as (

    select * from {{ ref('stg_game_offer') }}
),

draw as (

    select * from {{ ref('stg_draw') }}
),

lottery as (

    select * from {{ ref('stg_lottery') }}
),

final as (

    select 
      ticket_unioned.ticket_id,
      ticket_unioned.ticket_timestamp,
      lottery.lottery_name,
      game_offer.game_offer_name,
      game_offer.equivalent_standard_games_count,
      draw.draw_no,
      draw.draw_date,
      draw.draw_prize_pool,
      ticket_unioned.account_transaction_id,
      ticket_status.ticket_status_name,
      ticket_unioned.checkout_id,
      source
    from ticket_unioned
    left join ticket_status on ticket_unioned.ticket_status_id = ticket_status.ticket_status_id
    left join game_offer on ticket_unioned.game_offer_id = game_offer.game_offer_id
    left join draw on ticket_unioned.draw_id = draw.draw_id
    left join lottery on draw.lottery_id = lottery.lottery_id
)

select * from final