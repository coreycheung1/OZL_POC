with lottery as (

    select 
        lottery_id,
        case
            when lottery_name like 'Set For Life%' then 'Set For Life'
            when lottery_name like 'Powerball%' then 'Powerball'
            else lottery_name 
        end as lottery_name,
        lottery_type_id,
        lottery_type_name
    from {{ source('ozl_node_3', 'lottery') }}
)

select * from lottery