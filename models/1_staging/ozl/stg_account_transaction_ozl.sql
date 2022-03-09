with account_transaction_unioned as (

    {{ union_node_sources('ozl', 4, 'account_transaction') }}
),

account_transaction_type as (

    select * from {{ ref('stg_account_transaction_type_ozl') }}
),

final as (

    select
        atu.source,
        concat(atu.account_transaction_id, '_', source) as account_transaction_id,
        convert_timezone('UTC', 'Australia/Brisbane', atu.account_transaction_timestamp) as account_transaction_timestamp,
        concat(atu.account_id, '_', database) as account_id,
        try_parse_json(atu.transaction_reason) as transaction_reason,
        atu.transaction_amount,
        atu.account_transaction_type_id,
        att.account_transaction_type_name as account_transaction_type
    from account_transaction_unioned atu
    join account_transaction_type att on atu.account_transaction_type_id = att.account_transaction_type_id
)

select *
from final