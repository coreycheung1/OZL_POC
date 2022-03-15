with recurring_syndicate_share_unioned as (

    {{ union_node_sources('ozl', 4, 'recurring_syndicate_share') }}
),

recurring_syndicate_share_cleaned as (

     select
        source,
        syndicate_share_id,
        concat(recurring_purchase_id, '_', source) as recurring_purchase_id
    from recurring_syndicate_share_unioned
),

final as (

    select
        source,
        syndicate_share_id,
        recurring_purchase_id
    from recurring_syndicate_share_cleaned
)

select * 
from final