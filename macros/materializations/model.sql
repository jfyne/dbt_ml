{% macro drop_model(relation) %}
    {{
        adapter.dispatch(
            macro_name='drop_model',
            macro_namespace='dbt_ml'
        )
        (relation)
    }}
{% endmacro %}

{% macro default__drop_model(relation) %}
    {{ exceptions.raise_compiler_error("Dropping ML models is not implemented for the default adapter") }}
{% endmacro %}

{% macro bigquery__drop_model(relation) %}
    {% call statement('drop_relation') -%}
        drop {{ relation.type | default('model') }} if exists {{ relation }}
    {%- endcall %}
{% endmacro %}

{% macro model_options(ml_config, labels) %}

    {% set options -%}
        options(
            {%- for opt_key, opt_val in ml_config.items() -%}
                {%- if opt_val is sequence and (opt_val | first) is string and (opt_val | first).startswith('hparam_') -%}
                    {{ opt_key }}={{ opt_val[0] }}({{ opt_val[1:] | join(', ') }})
                {%- else -%}
                    {{ opt_key }}={{ (opt_val | tojson) if opt_val is string else opt_val }}
                {%- endif -%}
                {{ ',' if not loop.last }}
            {%- endfor -%}
        )
    {%- endset %}

    {%- do return(options) -%}
{%- endmacro -%}

{% macro model_transform(ml_transform) %}

    {% set transform -%}
        transform(
            {{ ', '.join(ml_transform) }}
        )
    {%- endset %}

    {%- do return(transform) -%}

{%- endmacro -%}

{% macro model_input_output(ml_input, ml_output) %}

    {% set io -%}
        input(
            {{ ', '.join(ml_input) }}
        )
        output(
            {{ ', '.join(ml_output) }}
        )
    {%- endset %}

    {%- do return(io) -%}

{%- endmacro -%}

{% macro model_remote(ml_remote) %}

    {% set remote -%}
        remote with connection `{{ml_remote}}`
    {%- endset %}

    {%- do return(remote) -%}

{%- endmacro -%}

{% macro create_model_as(relation, sql) -%}
    {{
        adapter.dispatch(
            macro_name='create_model_as',
            macro_namespace='dbt_ml'
        )
        (relation, sql)
    }}
{%- endmacro %}

{% macro default__create_model_as(relation, sql) %}
    {{ exceptions.raise_compiler_error("ML model creation is not implemented for the default adapter") }}
{% endmacro %}

{% macro bigquery__create_model_as(relation, sql) %}
    {%- set ml_transform = config.get('ml_transform', []) -%}
    {%- set ml_input = config.get('ml_input', []) -%}
    {%- set ml_output = config.get('ml_output', []) -%}
    {%- set ml_remote = config.get('ml_remote', none) -%}
    {%- set ml_config = config.get('ml_config', {}) -%}
    {%- set raw_labels = config.get('labels', {}) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none }}

    create or replace model {{ relation }}
    {{ dbt_ml.model_transform(ml_transform=ml_transform) if ml_transform|length > 0 }}
    {{ dbt_ml.model_input_output(ml_input=ml_input, ml_output=ml_output) if ml_input|length != 0 }}
    {{ dbt_ml.model_remote(ml_remote=ml_remote) if ml_remote is not none }}
    {{ dbt_ml.model_options(
        ml_config=ml_config,
        labels=raw_labels
    ) }}
    {%- if ml_config.get('MODEL_TYPE', ml_config.get('model_type', '')).lower() != 'tensorflow' -%}
    as (
        {{ sql }}
    );
    {%- endif -%}
{% endmacro %}

{% materialization model, adapter='bigquery' -%}
    {%- set identifier = model['alias'] -%}
    {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}    
    {%- set target_relation = api.Relation.create(database=database, schema=schema, identifier=identifier) -%}

    {{ run_hooks(pre_hooks) }}

    {% call statement('main') -%}
        {{ dbt_ml.create_model_as(target_relation, sql) }}
    {% endcall -%}

    {{ run_hooks(post_hooks) }}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
