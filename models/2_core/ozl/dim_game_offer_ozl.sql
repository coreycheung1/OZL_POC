with game_offer as (

    select 
        game_offer_id,
        lottery_id,
        game_offer_name,
        equivalent_standard_games_count
    from {{ ref('stg_game_offer_ozl') }}
)

select * 
from game_offer