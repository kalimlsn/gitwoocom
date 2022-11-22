{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "public",
    tags = [ "nested" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('orders_billing_ab3') }}
select
    _airbyte_orders_hashid,
    city,
    email,
    phone,
    {{ adapter.quote('state') }},
    company,
    country,
    postcode,
    address_1,
    address_2,
    last_name,
    first_name,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_billing_hashid
from {{ ref('orders_billing_ab3') }}
-- billing at orders/billing from {{ ref('orders_scd') }}
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

