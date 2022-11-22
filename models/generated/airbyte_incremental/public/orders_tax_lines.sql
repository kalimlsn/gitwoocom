{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "public",
    tags = [ "nested" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('orders_tax_lines_ab3') }}
select
    _airbyte_orders_hashid,
    {{ adapter.quote('id') }},
    {{ adapter.quote('label') }},
    rate_id,
    compound,
    meta_data,
    rate_code,
    tax_total,
    shipping_tax_total,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_tax_lines_hashid
from {{ ref('orders_tax_lines_ab3') }}
-- tax_lines at orders/tax_lines from {{ ref('orders_scd') }}
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

