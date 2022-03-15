with unioned_recurring_syndicate_share as (

    {{ union_node_sources('ozl', 4, 'recurring_syndicate_share') }}
),

final as (

    select
        source,
        concat(syndicate_share_id, '_', database) as syndicate_share_id,
        concat(recurring_purchase_id, '_', database) as recurring_purchase_id
    from unioned_recurring_syndicate_share
)

select * 
from final