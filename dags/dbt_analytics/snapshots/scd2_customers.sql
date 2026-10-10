{% snapshot scd2_customers %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_key',
        strategy='check',
        check_cols=['address', 'phone_number', 'account_balance', 'market_segment']
    )
}}

select
    c_custkey as customer_key,
    c_name as customer_name,
    c_address as address,
    c_nationkey as nation_key,
    c_phone as phone_number,
    c_acctbal as account_balance,
    c_mktsegment as market_segment,
    c_comment as comment
from {{ source('tpch', 'customer') }}

{% endsnapshot %}
