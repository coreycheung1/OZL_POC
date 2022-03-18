with sales as (

    select *
    from {{ ref('fct_sale_ozl') }}
    where status in ('SYNDICATE_SHARE_PURCHASED', 'SYNDICATE_SHARE_PAID', 'TICKET_PURCHASED', 'TICKET_PAID')
),

first_sale as (

    select *
    from {{ ref('fct_first_sale_ozl') }}
),

total_TTV as (

    select
        source,
        customer_id,
        sum(TTV) as total_TTV
    from sales
    group by source, customer_id
),

_180_day_TTV as (

    select
        s.source,
        s.customer_id,
        sum(TTV) as _180_day_TTV
    from sales s
    join first_sale fs on s.customer_id = fs.customer_id
    where datediff('day', fs.first_sale_timestamp, s.sale_timestamp) <= 180
    and first_sale_timestamp <= current_date - interval '180days'
    group by s.source, s.customer_id
),

final as (

    select 
        t.source,
        t.customer_id,
        total_TTV,
        _180_day_TTV
    from total_TTV as t
    join _180_day_TTV as _180 on t.customer_id = _180.customer_id

)

select *
from final