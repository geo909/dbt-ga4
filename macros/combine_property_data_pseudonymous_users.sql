{%- macro combine_property_data_pseudonymous_users() -%}
    {{ return(adapter.dispatch('combine_property_data_pseudonymous_users', 'ga4')()) }}
{%- endmacro -%}

{% macro default__combine_property_data_pseudonymous_users() %}

    create schema if not exists `{{target.project}}.{{var('combined_dataset')}}`;

    {# If incremental, then use static_incremental_days variable to find earliest shard to copy. Otherwise use 'start_date' variable #}
    {% if not should_full_refresh() %}
        {{ log("Full refresh is off; using the static_incremental_days variable", info=True) }}
        {% set earliest_shard_to_retrieve = (modules.datetime.date.today() - modules.datetime.timedelta(days=var('static_incremental_days')))|string|replace("-", "")|int %}
    {% else %}
        {{ log("Full refresh is on", info=True) }}
        {% set earliest_shard_to_retrieve = var('start_date')|int %}
    {% endif %}
    
    {{ log("Earliest shard to retrieve for pseudonymous users is: " ~ earliest_shard_to_retrieve, info=True) }}

    {% for property_id in var('property_ids') %}
        {%- set schema_name = "analytics_" + property_id|string -%}
            {%- set relations = dbt_utils.get_relations_by_pattern(schema_pattern=schema_name, table_pattern='pseudonymous_users_%', database=var('source_project')) -%}
            {%- set sorted_relations = relations | sort(attribute='identifier') -%}

            {% for relation in sorted_relations %}
                {%- set relation_suffix = relation.identifier|replace('pseudonymous_users_', '') -%}
                
                {%- if relation_suffix|int >= earliest_shard_to_retrieve|int -%}
                    {{ log("Cloning " ~ "pseudonymous_users_" ~ relation_suffix ~ property_id ~ " into " ~ var('combined_dataset'), info=True) }}
                    create or replace table `{{target.project}}.{{var('combined_dataset')}}.pseudonymous_users_{{relation_suffix}}{{property_id}}` clone `{{var('source_project')}}.analytics_{{property_id}}.pseudonymous_users_{{relation_suffix}}`;
                {% endif %}

            {% endfor %}
    {% endfor %}
{% endmacro %}
