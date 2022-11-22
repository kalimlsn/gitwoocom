{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('orders_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        adapter.quote('id'),
        'total',
        object_to_string('_links'),
        adapter.quote('number'),
        'status',
        object_to_string('billing'),
        array_to_string('refunds'),
        adapter.quote('version'),
        'cart_tax',
        'currency',
        boolean_to_string('set_paid'),
        object_to_string('shipping'),
        'shop_url',
        'cart_hash',
        'date_paid',
        array_to_string('fee_lines'),
        array_to_string('meta_data'),
        'order_key',
        'parent_id',
        array_to_string('tax_lines'),
        'total_tax',
        array_to_string('line_items'),
        'created_via',
        'customer_id',
        array_to_string('coupon_lines'),
        'date_created',
        'discount_tax',
        'shipping_tax',
        'customer_note',
        'date_modified',
        'date_paid_gmt',
        'date_completed',
        'discount_total',
        'payment_method',
        array_to_string('shipping_lines'),
        'shipping_total',
        'transaction_id',
        'date_created_gmt',
        'date_modified_gmt',
        'date_completed_gmt',
        boolean_to_string('prices_include_tax'),
        'customer_ip_address',
        'customer_user_agent',
        'payment_method_title',
    ]) }} as _airbyte_orders_hashid,
    tmp.*
from {{ ref('orders_ab2') }} tmp
-- orders
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

