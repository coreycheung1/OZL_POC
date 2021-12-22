with draw as (
    
    select 
        draw_id,
        convert_timezone('UTC', 'Australia/Brisbane', draw_timestamp) as draw_timestamp,
        lottery_id,
        draw_date,
        draw_prize_pool,
        convert_timezone('UTC', 'Australia/Brisbane', draw_start) as draw_start,
        convert_timezone('UTC', 'Australia/Brisbane', draw_stop) as draw_stop,
        draw_status_id
    from {{ source('ozl_node_3', 'draw') }}
)

select *
from draw