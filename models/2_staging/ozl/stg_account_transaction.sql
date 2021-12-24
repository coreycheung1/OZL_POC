with n3 as (

    select * from {{ ref('base_account_transaction_ozl_n3') }} 
),

n4 as (

    select * from {{ ref('base_account_transaction_ozl_n4') }}
),

account_transaction_unioned as (

    select * from n3
    union all
    select * from n4
),

account_transaction_type as (

    select * from {{ ref('stg_account_transaction_type') }}
),

final as (

    select 
        account_transaction_unioned.account_transaction_id,
        account_transaction_unioned.account_transaction_timestamp,
        account_transaction_unioned.account_id,
        account_transaction_unioned.transaction_reason,
        account_transaction_unioned.transaction_amount,
        account_transaction_type.account_transaction_type_name as account_transaction_type,
        account_transaction_unioned.source
    from account_transaction_unioned
    join account_transaction_type on account_transaction_unioned.account_transaction_type_id = account_transaction_type.account_transaction_type_id
)

select *
from final

