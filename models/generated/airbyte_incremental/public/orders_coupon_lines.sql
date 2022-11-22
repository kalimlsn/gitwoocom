{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "public",
    tags = [ "nested" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('orders_coupon_lines_ab3') }}
select
    _airbyte_orders_hashid,
    {{ adapter.quote('id') }},
    code,
    discount,
    meta_data,
    discount_tax,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_coupon_lines_hashid
from {{ ref('orders_coupon_lines_ab3') }}
-- coupon_lines at orders/coupon_lines from {{ ref('orders_scd') }}
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

