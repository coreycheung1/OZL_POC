select 
    distinct customer_id
from {{ ref('ozl_fct_sales') }}
