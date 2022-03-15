with syndicate_share_unioned as (

    {{ union_node_sources('ozl', 4, 'syndicate_share') }}
),

syndicate_share_cleaned as (

    select
        source,
        syndicate_share_id,
        convert_timezone('UTC', 'Australia/Brisbane', syndicate_share_timestamp) as syndicate_share_timestamp,
        syndicate_session_id,
        customer_id,
        syndicate_share_count,
        concat(account_transaction_id, '_', source) as account_transaction_id,
        concat(checkout_id, '_', source) as checkout_id,
        syndicate_share_status_id,
        syndicate_id,
        draw_id
    from syndicate_share_unioned
),

syndicate_share_status as (

    select * from {{ ref('stg_syndicate_share_status_ozl') }}
),

recurring_syndicate_share as (

    select * from {{ ref('stg_recurring_syndicate_share_ozl') }}
),

checkout as (

    select * from {{ ref('stg_checkout_ozl') }}
),

final as (

    select
        ss.source,
        ss.syndicate_share_id,
        ss.syndicate_share_timestamp,
        ss.syndicate_session_id,
        ss.customer_id,
        ss.syndicate_share_count,
        ss.account_transaction_id,
        ss.checkout_id,
        status.syndicate_share_status_name,
        ss.syndicate_id,
        ss.draw_id,
        rss.recurring_purchase_id,
        c.channel,
        c.checkout_sale_data
    from syndicate_share_cleaned ss
    join checkout c on ss.checkout_id = c.checkout_id
    join recurring_syndicate_share rss on ss.syndicate_share_id = rss.syndicate_share_id
    join syndicate_share_status status on ss.syndicate_share_status_id = status.syndicate_share_status_id 
)

select *
from final
