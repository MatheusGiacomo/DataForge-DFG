# tests/test_adapters.py
"""Testes de integração dos adaptadores usando DuckDB em memória."""
import pytest
import duckdb
from dfg.adapters.generic import GenericDBAPIAdapter
from dfg.adapters.factory import AdapterFactory


@pytest.fixture
def duckdb_adapter():
    adapter = GenericDBAPIAdapter(duckdb)
    adapter.connect({"type": "duckdb", "database": ":memory:"})
    yield adapter
    adapter.close()


class TestGenericAdapter:
    def test_connect_and_execute(self, duckdb_adapter):
        result = duckdb_adapter.execute("SELECT 42 AS answer")
        assert result[0][0] == 42

    def test_table_not_exists(self, duckdb_adapter):
        assert duckdb_adapter.check_table_exists("nonexistent_table") is False

    def test_table_exists_after_create(self, duckdb_adapter):
        duckdb_adapter.execute("CREATE TABLE t (id INTEGER)")
        assert duckdb_adapter.check_table_exists("t") is True

    def test_load_data_creates_table(self, duckdb_adapter):
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        duckdb_adapter.load_data("users", data)
        rows = duckdb_adapter.execute("SELECT COUNT(*) FROM users")
        assert rows[0][0] == 2

    def test_schema_evolution_adds_column(self, duckdb_adapter):
        duckdb_adapter.load_data("items", [{"id": 1, "name": "Widget"}])
        # Segunda carga com coluna nova
        duckdb_adapter.load_data("items", [{"id": 2, "name": "Gadget", "price": 9.99}])
        cols = [r[1] for r in duckdb_adapter.execute("PRAGMA table_info(items)")]
        assert "price" in cols

    def test_load_empty_data_noop(self, duckdb_adapter):
        # Não deve lançar exceção
        duckdb_adapter.load_data("empty_table", [])

    def test_close_idempotent(self, duckdb_adapter):
        duckdb_adapter.close()
        duckdb_adapter.close()  # Segunda chamada não deve errar

    def test_execute_after_close_raises(self, duckdb_adapter):
        duckdb_adapter.close()
        with pytest.raises(RuntimeError, match="não está conectado"):
            duckdb_adapter.execute("SELECT 1")


class TestAdapterFactory:
    def test_get_duckdb_adapter(self):
        adapter = AdapterFactory.get_adapter("duckdb")
        assert isinstance(adapter, GenericDBAPIAdapter)

    def test_unknown_type_raises(self):
        with pytest.raises(ValueError, match="não está configurado"):
            AdapterFactory.get_adapter("oracle")

    def test_alias_postgresql(self):
        pytest.importorskip("psycopg2", reason="psycopg2 não instalado")
        adapter = AdapterFactory.get_adapter("postgresql")
        assert isinstance(adapter, GenericDBAPIAdapter)
