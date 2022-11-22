{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_public",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ ref('customers_scd') }}
select
    _airbyte_customers_hashid,
    {{ json_extract_scalar('shipping', ['city'], ['city']) }} as city,
    {{ json_extract_scalar('shipping', ['state'], ['state']) }} as {{ adapter.quote('state') }},
    {{ json_extract_scalar('shipping', ['company'], ['company']) }} as company,
    {{ json_extract_scalar('shipping', ['country'], ['country']) }} as country,
    {{ json_extract_scalar('shipping', ['postcode'], ['postcode']) }} as postcode,
    {{ json_extract_scalar('shipping', ['address_1'], ['address_1']) }} as address_1,
    {{ json_extract_scalar('shipping', ['address_2'], ['address_2']) }} as address_2,
    {{ json_extract_scalar('shipping', ['last_name'], ['last_name']) }} as last_name,
    {{ json_extract_scalar('shipping', ['first_name'], ['first_name']) }} as first_name,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('customers_scd') }} as table_alias
-- shipping at customers/shipping
where 1 = 1
and shipping is not null
{{ incremental_clause('_airbyte_emitted_at', this) }}

