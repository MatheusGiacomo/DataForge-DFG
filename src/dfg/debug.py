# src/dfg/debug.py
"""
Comando 'dfg debug' do DataForge.

Realiza um diagnóstico completo do ambiente:
1. Exibe informações do ambiente (Python, diretório)
2. Verifica a existência dos arquivos de configuração
3. Valida o parsing das configurações
4. Testa a conexão com o banco de dados (SELECT 1)
"""
import os
import sys

from dfg.logging import logger


def debug_command(args) -> None:
    """
    Diagnóstico do ambiente DataForge.

    Encerra com sys.exit(1) se qualquer verificação crítica falhar.
    """
    # Importações tardias para evitar erros de circular import
    from dfg.adapters.factory import AdapterFactory
    from dfg.engine import DFGEngine

    logger.info("Iniciando diagnóstico do DataForge...")
    print("-" * 50)

    current_dir = os.getcwd()

    # ------------------------------------------------------------------
    # 1. Informações do Ambiente
    # ------------------------------------------------------------------
    logger.info(f"Diretório atual: {current_dir}")
    logger.info(f"Versão do Python: {sys.version.split()[0]}")

    # ------------------------------------------------------------------
    # 2. Verificação dos Arquivos de Configuração
    # ------------------------------------------------------------------
    project_file = os.path.join(current_dir, "dfg_project.toml")
    profiles_file = os.path.join(current_dir, "profiles.toml")

    if not os.path.exists(project_file):
        logger.error(
            "'dfg_project.toml' não encontrado. "
            "Você está na raiz do projeto? Execute 'dfg init' se necessário."
        )
        sys.exit(1)

    if not os.path.exists(profiles_file):
        logger.error("'profiles.toml' não encontrado na raiz do projeto.")
        sys.exit(1)

    logger.success("Arquivos de configuração encontrados.")

    # ------------------------------------------------------------------
    # 3. Validação das Configurações
    # ------------------------------------------------------------------
    try:
        engine = DFGEngine(project_dir=current_dir)
        target_name = engine.config["project"]["target"]
        target_config = engine.config["targets"][target_name]
        logger.success(f"Configuração carregada. Profile ativo: '{target_name}'.")
    except Exception as e:
        logger.error(f"Erro ao ler as configurações: {e}")
        sys.exit(1)

    # ------------------------------------------------------------------
    # 4. Teste de Conexão com o Banco
    # ------------------------------------------------------------------
    db_type = target_config.get("type", "desconhecido")
    logger.info(f"Tentando conectar ao banco do tipo '{db_type}'...")

    adapter = None
    try:
        adapter = AdapterFactory.get_adapter(db_type)
        adapter.connect(target_config)

        result = adapter.execute("SELECT 1 AS ping")
        if result and result[0][0] == 1:
            logger.success("Conexão com o banco de dados: OK (SELECT 1 retornou 1).")
        else:
            logger.warn("Conexão estabelecida, mas o resultado de 'SELECT 1' foi inesperado.")

    except Exception as e:
        logger.error(f"Falha ao conectar no banco de dados: {e}")
        sys.exit(1)
    finally:
        if adapter is not None:
            adapter.close()

    # ------------------------------------------------------------------
    # Resultado Final
    # ------------------------------------------------------------------
    print("-" * 50)
    logger.success("Tudo certo! O DataForge está pronto para a forja. 🔥")
