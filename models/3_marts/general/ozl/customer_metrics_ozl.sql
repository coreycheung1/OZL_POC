with agg_sale as (

    select * from {{ ref('fct_agg_sale_ozl') }}
),

first_sale as (

    select * from {{ ref('fct_first_sale_ozl') }}
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
        fs.source,
        fs.customer_id,
        ag.total_ttv,
        ag._180_day_ttv,
        fs.first_sale_id,
        fs.first_sale_timestamp,
        fs.first_checkout_id,
        first_l.lottery_name first_lottery,
        first_l.lottery_type_name first_lottery_type,
        first_d.draw_no first_draw_no,
        first_d.draw_prize_pool first_prize_pool,
        first_go.game_offer_name as first_game_offer,
        first_channel,
        first_checkout_sale_data
    from first_sale fs
    left join agg_sale ag on fs.customer_id = ag.customer_id
    left join dim_draw first_d on fs.first_draw_id = first_d.draw_id
    left join dim_lottery first_l on first_d.lottery_id = first_l.lottery_id
    left join dim_game_offer first_go on fs.first_game_offer_id = first_go.game_offer_id
)

select *
from final