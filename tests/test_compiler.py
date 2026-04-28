# tests/test_compiler.py
"""Testes unitários do SQLCompiler."""
import pytest
from dfg.compiler import SQLCompiler


@pytest.fixture
def compiler():
    return SQLCompiler(target_schema="public")


class TestCompile:
    def test_basic_select(self, compiler):
        result = compiler.compile("SELECT 1 AS id", "my_model")
        assert result["sql"] == "SELECT 1 AS id"
        assert result["depends_on"] == []
        assert result["config"] == {}

    def test_config_macro_extracted(self, compiler):
        sql = "{{ config(materialized='table') }}\nSELECT 1"
        result = compiler.compile(sql, "my_model")
        assert result["config"]["materialized"] == "table"
        # config() não aparece no SQL compilado
        assert "config" not in result["sql"]

    def test_ref_registers_dependency(self, compiler):
        sql = "SELECT * FROM {{ ref('stg_users') }}"
        result = compiler.compile(sql, "fct_orders")
        assert "stg_users" in result["depends_on"]
        assert "stg_users" in result["sql"]

    def test_multiple_refs(self, compiler):
        sql = "SELECT * FROM {{ ref('a') }} JOIN {{ ref('b') }} ON a.id = b.id"
        result = compiler.compile(sql, "fct")
        assert set(result["depends_on"]) == {"a", "b"}

    def test_target_schema_available(self, compiler):
        sql = "SELECT '{{ target_schema }}' AS s"
        result = compiler.compile(sql, "m")
        assert "public" in result["sql"]

    def test_syntax_error_raises(self, compiler):
        import jinja2
        with pytest.raises(jinja2.exceptions.TemplateSyntaxError):
            compiler.compile("SELECT {{ unclosed", "bad_model")


class TestParseSnapshot:
    def test_valid_snapshot(self, compiler):
        raw = """
{% snapshot snap_users %}
{{ config(unique_key='id', updated_at='updated_at') }}
SELECT * FROM my_table
{% endsnapshot %}
"""
        result = compiler.parse_snapshot(raw)
        assert result is not None
        assert result["snapshot_name"] == "snap_users"
        assert result["config"]["unique_key"] == "id"
        assert result["config"]["updated_at"] == "updated_at"
        assert "SELECT" in result["compiled_sql"]

    def test_no_snapshot_block_returns_none(self, compiler):
        assert compiler.parse_snapshot("SELECT 1") is None

    def test_snapshot_with_ref(self, compiler):
        raw = """
{% snapshot snap_orders %}
{{ config(unique_key='order_id', updated_at='updated_at') }}
SELECT * FROM {{ ref('stg_orders') }}
{% endsnapshot %}
"""
        result = compiler.parse_snapshot(raw)
        assert "stg_orders" in result["compiled_sql"]
