{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_public",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ ref('orders_scd') }}
select
    _airbyte_orders_hashid,
    {{ json_extract_scalar('billing', ['city'], ['city']) }} as city,
    {{ json_extract_scalar('billing', ['email'], ['email']) }} as email,
    {{ json_extract_scalar('billing', ['phone'], ['phone']) }} as phone,
    {{ json_extract_scalar('billing', ['state'], ['state']) }} as {{ adapter.quote('state') }},
    {{ json_extract_scalar('billing', ['company'], ['company']) }} as company,
    {{ json_extract_scalar('billing', ['country'], ['country']) }} as country,
    {{ json_extract_scalar('billing', ['postcode'], ['postcode']) }} as postcode,
    {{ json_extract_scalar('billing', ['address_1'], ['address_1']) }} as address_1,
    {{ json_extract_scalar('billing', ['address_2'], ['address_2']) }} as address_2,
    {{ json_extract_scalar('billing', ['last_name'], ['last_name']) }} as last_name,
    {{ json_extract_scalar('billing', ['first_name'], ['first_name']) }} as first_name,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('orders_scd') }} as table_alias
-- billing at orders/billing
where 1 = 1
and billing is not null
{{ incremental_clause('_airbyte_emitted_at', this) }}

