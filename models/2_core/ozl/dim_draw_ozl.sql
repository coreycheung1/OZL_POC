with draw as (

    select 
        draw_id,
        draw_timestamp,
        lottery_id,
        draw_no,
        draw_date,
        draw_prize_pool,
        draw_start,
        draw_stop,
        draw_status_id
    from {{ ref('stg_draw_ozl') }}
)

select * 
from draw