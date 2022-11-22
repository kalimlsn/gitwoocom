{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('public', '_airbyte_raw_customers') }}
select
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as {{ adapter.quote('id') }},
    {{ json_extract_scalar('_airbyte_data', ['role'], ['role']) }} as {{ adapter.quote('role') }},
    {{ json_extract_scalar('_airbyte_data', ['email'], ['email']) }} as email,
    {{ json_extract('table_alias', '_airbyte_data', ['_links'], ['_links']) }} as _links,
    {{ json_extract('table_alias', '_airbyte_data', ['billing'], ['billing']) }} as billing,
    {{ json_extract('table_alias', '_airbyte_data', ['shipping'], ['shipping']) }} as shipping,
    {{ json_extract_scalar('_airbyte_data', ['shop_url'], ['shop_url']) }} as shop_url,
    {{ json_extract_scalar('_airbyte_data', ['username'], ['username']) }} as username,
    {{ json_extract_scalar('_airbyte_data', ['last_name'], ['last_name']) }} as last_name,
    {{ json_extract_array('_airbyte_data', ['meta_data'], ['meta_data']) }} as meta_data,
    {{ json_extract_scalar('_airbyte_data', ['avatar_url'], ['avatar_url']) }} as avatar_url,
    {{ json_extract_scalar('_airbyte_data', ['first_name'], ['first_name']) }} as first_name,
    {{ json_extract_scalar('_airbyte_data', ['date_created'], ['date_created']) }} as date_created,
    {{ json_extract_scalar('_airbyte_data', ['date_modified'], ['date_modified']) }} as date_modified,
    {{ json_extract_scalar('_airbyte_data', ['date_created_gmt'], ['date_created_gmt']) }} as date_created_gmt,
    {{ json_extract_scalar('_airbyte_data', ['date_modified_gmt'], ['date_modified_gmt']) }} as date_modified_gmt,
    {{ json_extract_scalar('_airbyte_data', ['is_paying_customer'], ['is_paying_customer']) }} as is_paying_customer,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('public', '_airbyte_raw_customers') }} as table_alias
-- customers
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

