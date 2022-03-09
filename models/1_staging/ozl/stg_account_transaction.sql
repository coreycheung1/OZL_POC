with account_transaction_unioned as (

    {{ union_node_sources('ozl', 4, 'account_transaction') }}
),

account_transaction_type as (

    select * from {{ ref('stg_account_transaction_type') }}
),

final as (

    select
        atu.source,
        atu.account_transaction_id,
        atu.account_transaction_timestamp,
        atu.account_id,
        atu.transaction_reason,
        atu.transaction_amount,
        att.account_transaction_type_name as account_transaction_type
    from account_transaction_unioned atu
    join account_transaction_type att on atu.account_transaction_type_id = att.account_transaction_type_id
)

select *
from final