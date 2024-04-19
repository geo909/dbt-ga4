with base_pseudonymous_users as (
    select {{ dbt_utils.star(from=ref('base_ga4__pseudonymous_users')) }}
    from {{ ref('base_ga4__pseudonymous_users') }}
)

select *
from base_pseudonymous_users