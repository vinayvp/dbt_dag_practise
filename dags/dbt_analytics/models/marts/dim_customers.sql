{{
    config(
        materialized='table'
    )
}}

with customers as (
    select * from {{ ref('stg_tpch_customers') }}
),

nations as (
    select * from {{ ref('stg_tpch_nations') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['customers.customer_key']) }} as customer_pk,
    customers.customer_key,
    customers.customer_name,
    customers.address,
    customers.nation_key,
    coalesce(nations.nation_name, 'Unknown') as nation_name,
    customers.phone_number,
    customers.account_balance,
    customers.market_segment,
    customers.comment
from
    customers
left join
    nations
        on customers.nation_key = nations.nation_key
