with customer_ozl_n4 as (

    select *
    from {{ ref('base_customer_ozl_n4') }} 
),

customer_ozl_n3 as (

    select *
    from {{ ref('base_customer_ozl_n3') }}
),

unioned as (

    select *
    from customer_ozl_n4
    union all
    select *
    from customer_ozl_n3
)

select *
from unioned

