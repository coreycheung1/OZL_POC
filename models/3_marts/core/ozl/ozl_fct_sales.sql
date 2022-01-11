with ticket as (

    select
        ticket_timestamp as sale_timestamp,
        customer_id,
        checkout_id,
        ticket_id,
        null as syndicate_share_id,
        account_transaction_id,
        draw_id,
        game_offer_id,
        ticket_status_name as status,
        recurring_purchase_id,
        channel,
        checkout_sale_data,
        null as syndicate_session_id,
        null as syndicate_id,
        null as syndicate_share_count,
        source
    from {{ ref('stg_ticket') }}
),

syndicate_share as (

    select
        syndicate_share_timestamp as sale_timestamp,
        customer_id,
        checkout_id,
        null as ticket_id,
        syndicate_share_id,
        account_transaction_id,
        draw_id,
        null as game_offer_id,
        syndicate_share_status_name as status,
        recurring_purchase_id,
        channel,
        checkout_sale_data,
        syndicate_session_id,
        syndicate_id,
        syndicate_share_count,
        source
    from {{ ref('stg_syndicate_share') }}
),

sales_unioned as (

    select * from ticket
    union all
    select * from syndicate_share
),

account_transaction as (

    select * from {{ ref('stg_account_transaction') }}
),

draw as (

    select * from {{ ref('stg_draw') }}
),

lottery as (

    select * from {{ ref('stg_lottery') }}
),

game_offer as (

    select * from {{ ref('stg_game_offer') }}
),

final as (

    select 
      md5(coalesce(ticket_id, syndicate_share_id)) as sale_id,
      sales_unioned.sale_timestamp,
      sales_unioned.customer_id,
      sales_unioned.checkout_id,
      sales_unioned.ticket_id,
      sales_unioned.syndicate_share_id,
      sales_unioned.account_transaction_id,
      account_transaction.transaction_amount as TTV,
      account_transaction.account_transaction_type,
      draw.draw_no,
      draw.draw_prize_pool,
      draw.draw_stop,
      lottery.lottery_name,
      lottery.lottery_type_name,
      game_offer.game_offer_name,
      game_offer.equivalent_standard_games_count,
      sales_unioned.status,
      sales_unioned.recurring_purchase_id,
      sales_unioned.channel,
      sales_unioned.checkout_sale_data,
      sales_unioned.syndicate_session_id,
      sales_unioned.syndicate_id,
      sales_unioned.syndicate_share_count,
      sales_unioned.source
  from sales_unioned
  join account_transaction on sales_unioned.account_transaction_id = account_transaction.account_transaction_id
  join draw on sales_unioned.draw_id = draw.draw_id
  join lottery on draw.lottery_id = lottery.lottery_id
  left join game_offer on sales_unioned.game_offer_id = game_offer.game_offer_id
)

select *
from final