with n3 as (

    select * from {{ ref('base_recurring_syndicate_share_ozl_n3') }}
),

n4 as (

    select * from {{ ref('base_recurring_syndicate_share_ozl_n4') }}
),

recurring_syndcate_share_unioned as (

    select * from n3
    union all
    select * from n4
),

final as (

    select 
        syndicate_share_id,
        recurring_purchase_id
    from recurring_syndcate_share_unioned
)

select * from final