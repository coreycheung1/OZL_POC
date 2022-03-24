with lottery as (

    select 
        lottery_id,
        lottery,
        lottery_type_id,
        lottery_type
    from {{ ref('stg_lottery_ozl') }}
)

select * 
from lottery