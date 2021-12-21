with customer_ozl_n4 as (

    select *
    from {{ ref('stg_customer_ozl_n4') }} 
),
    customer_ozl_n3 as (

        select *
        from {{ ref('stg_customer_ozl_n3') }}
    )

select *
from n4
union all
select *
from n3
