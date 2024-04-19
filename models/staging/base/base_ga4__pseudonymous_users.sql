{% set partitions_to_replace = ['current_date'] %}
{% for i in range(var('static_incremental_days')) %}
    {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
{% endfor %}

{{
    config(
        pre_hook="{{ ga4.combine_property_data_pseudonymous_users() }}" if var('combined_dataset', false) else "",
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by={
            "field": "occurrence_date_dt",
            "data_type": "date"
        }
    )
}}

with source as (
    select
        parse_date('%Y%m%d', occurrence_date) as occurrence_date_dt,
        *
    from {{ source('ga4', 'pseudonymous_users') }}
    where cast(left(_table_suffix, 8) as int64) >= {{var('start_date')}}
    {% if is_incremental() %}
        and parse_date('%Y%m%d', left(_table_suffix, 8)) in ({{ partitions_to_replace | join(',') }})
    {% endif %}
)

select * 
from source