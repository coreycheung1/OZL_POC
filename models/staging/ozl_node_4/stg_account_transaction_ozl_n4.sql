with account_transaction as (

    select
        account_transaction_id,
        account_transaction_timestamp,
        account_id,
        parse_json(transaction_reason) as transaction_reason,
        transaction_amount,
        account_transaction_type_id,
        database as source
    from {{ source('ozl_node_4', 'account_transaction') }}
),
    account_transaction_type as (

        select 
            account_transaction_type_id,
            account_transaction_type_name
        from {{ source('ozl_node_4', 'account_transaction_type') }}
    )

select
    a.account_transaction_id,
    a.account_transaction_timestamp,
    a.account_id,
    a.transaction_reason,
    a.transaction_amount,
    a.account_transaction_type_id,
    at.account_transaction_type_name,
    a.source
from account_transaction a
left join account_transaction_type at using(account_transaction_type_id)