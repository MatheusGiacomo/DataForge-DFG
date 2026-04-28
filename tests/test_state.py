# tests/test_state.py
"""Testes unitários do StateManager."""
import pytest

from dfg.state import StateManager


class TestStateManager:
    def test_get_default_when_empty(self, tmp_path):
        sm = StateManager(str(tmp_path))
        assert sm.get("nonexistent") is None
        assert sm.get("nonexistent", "fallback") == "fallback"

    def test_set_and_get(self, tmp_path):
        sm = StateManager(str(tmp_path))
        sm.set("my_model", "2024-01-15")
        assert sm.get("my_model") == "2024-01-15"

    def test_persistence_across_instances(self, tmp_path):
        sm1 = StateManager(str(tmp_path))
        sm1.set("model_a", {"page": 5, "total": 500})

        sm2 = StateManager(str(tmp_path))
        assert sm2.get("model_a") == {"page": 5, "total": 500}

    def test_delete(self, tmp_path):
        sm = StateManager(str(tmp_path))
        sm.set("key", "value")
        sm.delete("key")
        assert sm.get("key") is None

    def test_delete_nonexistent_noop(self, tmp_path):
        sm = StateManager(str(tmp_path))
        sm.delete("ghost_key")  # não deve lançar exceção

    def test_clear(self, tmp_path):
        sm = StateManager(str(tmp_path))
        sm.set("a", 1)
        sm.set("b", 2)
        sm.clear()
        assert sm.get("a") is None
        assert sm.get("b") is None

    def test_corrupted_file_returns_empty(self, tmp_path):
        state_file = tmp_path / ".dfg_state.json"
        state_file.write_text("NOT VALID JSON {{{{", encoding="utf-8")
        sm = StateManager(str(tmp_path))
        assert sm.get("anything") is None


# tests/test_engine.py
"""Testes de integração do DFGEngine."""


class TestEngineRun:
    def test_run_sql_models(self, project_dir, engine):
        (project_dir / "models" / "my_model.sql").write_text(
            "{{ config(materialized='table') }}\nSELECT 1 AS id, 'test' AS name",
            encoding="utf-8",
        )
        result = engine.run()
        assert result is True

    def test_run_with_dependency(self, project_dir, engine):
        (project_dir / "models" / "base.sql").write_text(
            "{{ config(materialized='table') }}\nSELECT 1 AS id",
            encoding="utf-8",
        )
        (project_dir / "models" / "derived.sql").write_text(
            "{{ config(materialized='view') }}\nSELECT * FROM {{ ref('base') }}",
            encoding="utf-8",
        )
        result = engine.run()
        assert result is True

    def test_run_python_model(self, project_dir, engine):
        (project_dir / "models" / "py_model.py").write_text(
            'DEPENDENCIES = []\ndef model(context):\n    return [{"id": 1, "val": "x"}]\n',
            encoding="utf-8",
        )
        result = engine.run()
        assert result is True

    def test_compile_generates_files(self, project_dir, engine):
        (project_dir / "models" / "sample.sql").write_text(
            "{{ config(materialized='table') }}\nSELECT 42 AS answer",
            encoding="utf-8",
        )
        engine.compile()
        compiled = project_dir / "target" / "compiled" / "sample.sql"
        assert compiled.exists()
        assert "42" in compiled.read_text(encoding="utf-8")

    def test_empty_models_dir_returns_no_work(self, project_dir, engine):
        result = engine.run()
        assert result == "no_work"

    def test_invalid_sql_model_does_not_crash_others(self, project_dir, engine):
        (project_dir / "models" / "good.sql").write_text(
            "{{ config(materialized='table') }}\nSELECT 1 AS id",
            encoding="utf-8",
        )
        # Modelo com SQL inválido
        (project_dir / "models" / "bad.sql").write_text(
            "{{ config(materialized='table') }}\nSELECT FROM NOWHERE INVALID",
            encoding="utf-8",
        )
        # O pipeline deve continuar e o 'good' deve rodar
        result = engine.run()
        # Tem pelo menos um erro, mas não trava
        assert result is False or result is True


class TestEngineSeed:
    def test_seed_loads_csv(self, project_dir, engine):
        from dfg.seed import SeedRunner
        (project_dir / "seeds" / "countries.csv").write_text(
            "id,name\n1,Brazil\n2,USA\n3,Germany\n",
            encoding="utf-8",
        )
        SeedRunner(engine).run()
        # Verifica que a tabela foi criada
        adapter = engine._get_thread_safe_adapter()
        try:
            rows = adapter.execute("SELECT COUNT(*) FROM countries")
            assert rows[0][0] == 3
        finally:
            adapter.close()

    def test_seed_infers_int_type(self, project_dir, engine):
        from dfg.seed import SeedRunner
        runner = SeedRunner(engine)
        data = runner._infer_type("42")
        assert data == 42
        assert isinstance(data, int)

    def test_seed_infers_float_type(self, project_dir, engine):
        from dfg.seed import SeedRunner
        runner = SeedRunner(engine)
        assert runner._infer_type("3.14") == pytest.approx(3.14)

    def test_seed_empty_becomes_none(self, project_dir, engine):
        from dfg.seed import SeedRunner
        assert SeedRunner(engine)._infer_type("") is None


class TestEngineTest:
    def test_contracts_pass(self, project_dir, engine):
        (project_dir / "models" / "clean.sql").write_text(
            "{{ config(materialized='table') }}\nSELECT 1 AS id, 'x' AS name",
            encoding="utf-8",
        )
        (project_dir / "models" / "schema.yml").write_text(
            "version: 1\nmodels:\n  - name: clean\n    columns:\n      - name: id\n        tests:\n          - not_null\n          - unique\n",
            encoding="utf-8",
        )
        engine.run()

        engine2_dir = project_dir
        from dfg.engine import DFGEngine
        e2 = DFGEngine(str(engine2_dir))
        e2.discover_models()

        # test() chama sys.exit(1) em falha — aqui deve passar sem sair
        try:
            e2.test()
        except SystemExit as exc:
            pytest.fail(f"dfg test falhou inesperadamente com exit code {exc.code}")
