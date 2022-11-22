{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_public",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('orders_refunds_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        '_airbyte_orders_hashid',
        adapter.quote('id'),
        'total',
        'reason',
    ]) }} as _airbyte_refunds_hashid,
    tmp.*
from {{ ref('orders_refunds_ab2') }} tmp
-- refunds at orders/refunds
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

