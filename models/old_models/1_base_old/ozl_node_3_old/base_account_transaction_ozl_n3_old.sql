with account_transaction as (

    select
        concat(account_transaction_id, '_', database) as account_transaction_id,
        convert_timezone('UTC', 'Australia/Brisbane', account_transaction_timestamp) as account_transaction_timestamp,
        concat(account_id, '_', database) as account_id,
        try_parse_json(transaction_reason) as transaction_reason,
        transaction_amount,
        account_transaction_type_id,
        database as source
    from {{ source('ozl_node_3', 'account_transaction') }}
)

select *
from account_transaction
