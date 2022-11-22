{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_public",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('orders_tax_lines_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        '_airbyte_orders_hashid',
        adapter.quote('id'),
        adapter.quote('label'),
        'rate_id',
        boolean_to_string('compound'),
        array_to_string('meta_data'),
        'rate_code',
        'tax_total',
        'shipping_tax_total',
    ]) }} as _airbyte_tax_lines_hashid,
    tmp.*
from {{ ref('orders_tax_lines_ab2') }} tmp
-- tax_lines at orders/tax_lines
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

