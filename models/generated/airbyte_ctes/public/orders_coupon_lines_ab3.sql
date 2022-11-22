{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_public",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('orders_coupon_lines_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        '_airbyte_orders_hashid',
        adapter.quote('id'),
        'code',
        'discount',
        array_to_string('meta_data'),
        'discount_tax',
    ]) }} as _airbyte_coupon_lines_hashid,
    tmp.*
from {{ ref('orders_coupon_lines_ab2') }} tmp
-- coupon_lines at orders/coupon_lines
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

