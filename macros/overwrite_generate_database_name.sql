{# Doc String:

This macro overwrites the built in generate_database_name() macro outlined here: https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-custom-databases

If the target name is not 'prod' then model will write to the 'DBT_DEV' database, otherwise, the macro behaves as usual.

#}

{% macro generate_database_name(custom_database_name=none, node=none) -%}
    {# if target name isnt prod then set target database to 'DBT_DEV' #}
    {%- if target.name != 'prod' -%}

        {%- set default_database = 'DBT_DEV' -%}

    {%- else -%}

        {%- set default_database = target.database -%}

    {%- endif -%}

    
    {%- if custom_database_name is none -%}

        {{ default_database }}

    {%- else -%}

        {{ custom_database_name | trim }}

    {%- endif -%}

{%- endmacro %}
