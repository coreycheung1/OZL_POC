with game_offer as (

    select 
        game_offer_id,
        lottery_id,
        game_offer_name as game_offer,
        equivalent_standard_games_count
    from {{ source('ozl_node_3', 'game_offer') }}
)

select *
from game_offer
