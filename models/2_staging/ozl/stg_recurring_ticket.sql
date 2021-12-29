with n3 as (

    select * from {{ ref('base_recurring_ticket_ozl_n3') }}
),

n4 as (

    select * from {{ ref('base_recurring_ticket_ozl_n4') }}
),

recurring_ticket_unioned as (

    select * from n3
    union all
    select * from n4
),

final as (

    select 
        ticket_id,
        recurring_purchase_id
    from recurring_ticket_unioned
)

select * from final