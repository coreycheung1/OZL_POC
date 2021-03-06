{# Macro to union nodes. Inputs: pbj instance, number of nodes, table name. #}

{%- macro union_node_sources(pbj, num_nodes, table) -%}

  {%- set sources = [] -%} {# initialise source list #}

  {%- for node in range(3, num_nodes + 1) -%}
    {%- do sources.append(source(pbj~'_node_'~node,  table)) -%} {# loop through nodes and append source 'relation' objects to source list #}
  {%- endfor -%}

  {%- for source in sources %} {# loop through list of relation objects and union #}
    select 
      '{{ source.schema }}' source, {# new field that gets the db schema from the relations class #}
      * 
    from {{ source }}

    {%- if not loop.last %} {# no union statement after last source #}
    union all
    {%- endif -%}

  {%- endfor -%}

{%- endmacro -%}