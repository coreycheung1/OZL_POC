with account_transaction_ozl_n3 as (

    select *
    from {{ ref('base_account_transaction_ozl_n3') }} 
),

account_transaction_ozl_n4 as (

    select *
    from {{ ref('base_account_transaction_ozl_n4') }}
),

unioned_account_transaction as (

    select *
    from account_transaction_ozl_n3
    union all
    select *
    from account_transaction_ozl_n4
),

account_transaction_type as (

    select *
    from {{ ref('stg_account_transaction_type') }}
),

final as (

    select 
        account_transaction_id,
        account_transaction_timestamp,
        account_id,
        transaction_reason,
        transaction_amount,
        account_transaction_type_name as account_transaction_type,
        source
    from unioned_account_transaction
    join account_transaction_type using(account_transaction_type_id)
)

select *
from final

