{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "public",
    tags = [ "nested" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('orders_line_items_ab3') }}
select
    _airbyte_orders_hashid,
    {{ adapter.quote('id') }},
    sku,
    {{ adapter.quote('name') }},
    price,
    taxes,
    total,
    quantity,
    subtotal,
    meta_data,
    tax_class,
    total_tax,
    product_id,
    subtotal_tax,
    variation_id,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_line_items_hashid
from {{ ref('orders_line_items_ab3') }}
-- line_items at orders/line_items from {{ ref('orders_scd') }}
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

