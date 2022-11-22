{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('public', '_airbyte_raw_orders') }}
select
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as {{ adapter.quote('id') }},
    {{ json_extract_scalar('_airbyte_data', ['total'], ['total']) }} as total,
    {{ json_extract('table_alias', '_airbyte_data', ['_links'], ['_links']) }} as _links,
    {{ json_extract_scalar('_airbyte_data', ['number'], ['number']) }} as {{ adapter.quote('number') }},
    {{ json_extract_scalar('_airbyte_data', ['status'], ['status']) }} as status,
    {{ json_extract('table_alias', '_airbyte_data', ['billing'], ['billing']) }} as billing,
    {{ json_extract_array('_airbyte_data', ['refunds'], ['refunds']) }} as refunds,
    {{ json_extract_scalar('_airbyte_data', ['version'], ['version']) }} as {{ adapter.quote('version') }},
    {{ json_extract_scalar('_airbyte_data', ['cart_tax'], ['cart_tax']) }} as cart_tax,
    {{ json_extract_scalar('_airbyte_data', ['currency'], ['currency']) }} as currency,
    {{ json_extract_scalar('_airbyte_data', ['set_paid'], ['set_paid']) }} as set_paid,
    {{ json_extract('table_alias', '_airbyte_data', ['shipping'], ['shipping']) }} as shipping,
    {{ json_extract_scalar('_airbyte_data', ['shop_url'], ['shop_url']) }} as shop_url,
    {{ json_extract_scalar('_airbyte_data', ['cart_hash'], ['cart_hash']) }} as cart_hash,
    {{ json_extract_scalar('_airbyte_data', ['date_paid'], ['date_paid']) }} as date_paid,
    {{ json_extract_array('_airbyte_data', ['fee_lines'], ['fee_lines']) }} as fee_lines,
    {{ json_extract_array('_airbyte_data', ['meta_data'], ['meta_data']) }} as meta_data,
    {{ json_extract_scalar('_airbyte_data', ['order_key'], ['order_key']) }} as order_key,
    {{ json_extract_scalar('_airbyte_data', ['parent_id'], ['parent_id']) }} as parent_id,
    {{ json_extract_array('_airbyte_data', ['tax_lines'], ['tax_lines']) }} as tax_lines,
    {{ json_extract_scalar('_airbyte_data', ['total_tax'], ['total_tax']) }} as total_tax,
    {{ json_extract_array('_airbyte_data', ['line_items'], ['line_items']) }} as line_items,
    {{ json_extract_scalar('_airbyte_data', ['created_via'], ['created_via']) }} as created_via,
    {{ json_extract_scalar('_airbyte_data', ['customer_id'], ['customer_id']) }} as customer_id,
    {{ json_extract_array('_airbyte_data', ['coupon_lines'], ['coupon_lines']) }} as coupon_lines,
    {{ json_extract_scalar('_airbyte_data', ['date_created'], ['date_created']) }} as date_created,
    {{ json_extract_scalar('_airbyte_data', ['discount_tax'], ['discount_tax']) }} as discount_tax,
    {{ json_extract_scalar('_airbyte_data', ['shipping_tax'], ['shipping_tax']) }} as shipping_tax,
    {{ json_extract_scalar('_airbyte_data', ['customer_note'], ['customer_note']) }} as customer_note,
    {{ json_extract_scalar('_airbyte_data', ['date_modified'], ['date_modified']) }} as date_modified,
    {{ json_extract_scalar('_airbyte_data', ['date_paid_gmt'], ['date_paid_gmt']) }} as date_paid_gmt,
    {{ json_extract_scalar('_airbyte_data', ['date_completed'], ['date_completed']) }} as date_completed,
    {{ json_extract_scalar('_airbyte_data', ['discount_total'], ['discount_total']) }} as discount_total,
    {{ json_extract_scalar('_airbyte_data', ['payment_method'], ['payment_method']) }} as payment_method,
    {{ json_extract_array('_airbyte_data', ['shipping_lines'], ['shipping_lines']) }} as shipping_lines,
    {{ json_extract_scalar('_airbyte_data', ['shipping_total'], ['shipping_total']) }} as shipping_total,
    {{ json_extract_scalar('_airbyte_data', ['transaction_id'], ['transaction_id']) }} as transaction_id,
    {{ json_extract_scalar('_airbyte_data', ['date_created_gmt'], ['date_created_gmt']) }} as date_created_gmt,
    {{ json_extract_scalar('_airbyte_data', ['date_modified_gmt'], ['date_modified_gmt']) }} as date_modified_gmt,
    {{ json_extract_scalar('_airbyte_data', ['date_completed_gmt'], ['date_completed_gmt']) }} as date_completed_gmt,
    {{ json_extract_scalar('_airbyte_data', ['prices_include_tax'], ['prices_include_tax']) }} as prices_include_tax,
    {{ json_extract_scalar('_airbyte_data', ['customer_ip_address'], ['customer_ip_address']) }} as customer_ip_address,
    {{ json_extract_scalar('_airbyte_data', ['customer_user_agent'], ['customer_user_agent']) }} as customer_user_agent,
    {{ json_extract_scalar('_airbyte_data', ['payment_method_title'], ['payment_method_title']) }} as payment_method_title,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('public', '_airbyte_raw_orders') }} as table_alias
-- orders
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

