with lottery as (

    select 
        lottery_id,
        lottery_name,
        lottery_type_id,
        lottery_type_name
    from {{ ref('stg_lottery_ozl') }}
)

select * 
from lottery