with first_checkout as (
    select 
        customer_id,
        checkout_id as first_checkout_id,
        sale_timestamp as first_sale_timestamp,
        lottery_name as first_lottery,
        lottery_type_name as first_lottery_type,
        channel as first_channel,
        draw_prize_pool as first_prize_pool,
        row_number() over (partition by customer_id order by sale_timestamp asc, ttv desc) ords -- orders sales
    from {{ ref('ozl_fct_sales') }}
    qualify ords = 1 -- first sale
),

final as (
    select 
        customer_id,
        first_checkout_id,
        first_sale_timestamp,
        first_lottery,
        first_lottery_type,
        first_channel,
        first_prize_pool
    from first_checkout
)

select *
from final
