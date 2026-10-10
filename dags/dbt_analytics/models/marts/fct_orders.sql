{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='order_key',
        on_schema_change='sync_all_columns'
    )
}}

with orders as (
    select
        order_key,
        customer_key,
        status_code,
        total_price,
        order_date
    from {{ ref('stg_tpch_orders') }}
    {% if is_incremental() %}
    -- Handle late-arriving records with a 3-day lookback window
    where order_date >= (select dateadd(day, -3, max(order_date)) from {{ this }})
    {% endif %}
),

customers as (
    select
        customer_pk,
        customer_key
    from {{ ref('dim_customers') }}
),

order_item_summary as (
    select
        order_key,
        gross_item_sales_amount,
        item_discount_amount
    from {{ ref('int_order_items_summary') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['orders.order_key']) }} as order_pk,
    customers.customer_pk,
    orders.order_key,
    orders.customer_key,
    orders.status_code,
    orders.total_price,
    orders.order_date,
    order_item_summary.gross_item_sales_amount,
    order_item_summary.item_discount_amount
from
    orders
join
    order_item_summary
        on orders.order_key = order_item_summary.order_key
left join
    customers
        on orders.customer_key = customers.customer_key