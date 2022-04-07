with account_transaction_unioned as (

    {{ union_node_sources('ozl', 4, 'account_transaction') }}
),

account_transaction_cleaned as (

    select
        source,
        concat(account_transaction_id, '_', source) as account_transaction_id,
        convert_timezone('UTC', 'Australia/Brisbane', account_transaction_timestamp) as account_transaction_timestamp,
        concat(account_id, '_', source) as account_id,
        try_parse_json(transaction_reason) as transaction_reason,
        transaction_amount,
        account_transaction_type_id
    from account_transaction_unioned        
),

account_transaction_type as (

    select * from {{ ref('stg_account_transaction_type_ozl') }}
),

final as (

    select
        at.source,
        at.account_transaction_id,
        at.account_transaction_timestamp,
        at.account_id,
        at.transaction_reason,
        at.transaction_amount,
        at.account_transaction_type_id,
        att.account_transaction_type
    from account_transaction_cleaned at
    join account_transaction_type att on at.account_transaction_type_id = att.account_transaction_type_id
)

select *
from final