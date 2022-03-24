with account_transaction_type as (
    
    select 
        account_transaction_type_id,
        account_transaction_type_name as account_transaction_type,
        account_transaction_type_description
    from {{ source('ozl_node_3', 'account_transaction_type') }}
)

select *
from account_transaction_type