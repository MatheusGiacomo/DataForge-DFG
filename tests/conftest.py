# tests/conftest.py
"""
Fixtures compartilhadas para o suite de testes do DataForge.

Usa DuckDB em memória para todos os testes que precisam de banco,
garantindo isolamento total e execução sem dependências externas.
"""
import os
import sys
import pytest

# Adiciona src/ ao path para os testes encontrarem o pacote dfg
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


@pytest.fixture
def project_dir(tmp_path):
    """
    Cria uma estrutura mínima de projeto DataForge em um diretório
    temporário isolado, usando DuckDB em arquivo para que múltiplas
    conexões possam compartilhar o mesmo estado.
    """
    # Pastas padrão
    (tmp_path / "models").mkdir()
    (tmp_path / "seeds").mkdir()
    (tmp_path / "snapshots").mkdir()
    (tmp_path / "logs").mkdir()
    (tmp_path / "target" / "compiled").mkdir(parents=True)

    # Configuração do projeto
    (tmp_path / "dfg_project.toml").write_text(
        '[project]\nname="test"\nprofile="test"\ntarget="dev"\nthreads=2\n',
        encoding="utf-8",
    )

    # DuckDB em arquivo para permitir múltiplas conexões nos testes
    db_path = str(tmp_path / "test.db").replace("\\", "/")
    (tmp_path / "profiles.toml").write_text(
        f'[test]\ntarget="dev"\n[test.outputs.dev]\ntype="duckdb"\ndatabase="{db_path}"\n',
        encoding="utf-8",
    )

    return tmp_path


@pytest.fixture
def engine(project_dir):
    """Instância de DFGEngine pronta para uso nos testes."""
    from dfg.engine import DFGEngine
    return DFGEngine(project_dir=str(project_dir))


@pytest.fixture
def adapter(engine):
    """Adaptador conectado ao banco de teste. Fecha a conexão ao final."""
    a = engine._get_thread_safe_adapter()
    yield a
    a.close()
