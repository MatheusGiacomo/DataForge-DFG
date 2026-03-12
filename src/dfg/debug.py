# src/dfg/debug.py
import sys
import os
from dfg.logger import logger
from dfg.engine import DFGEngine
from dfg.adapters.factory import AdapterFactory

def debug_command(args):
    logger.info("Iniciando diagnóstico do DFG (Data Forge)...")
    print("-" * 50)
    
    current_dir = os.getcwd()
    
    # 1. Checagem de Ambiente
    logger.info(f"Diretório atual: {current_dir}")
    logger.info(f"Versão do Python: {sys.version.split()[0]}")
    
    # 2. Checagem de Arquivos
    project_file = os.path.join(current_dir, "dfg_project.toml")
    profiles_file = os.path.join(current_dir, "profiles.toml")
    
    if not os.path.exists(project_file):
        logger.error("Falha: 'dfg_project.toml' não encontrado. Você está na raiz do projeto?")
        sys.exit(1)
    if not os.path.exists(profiles_file):
        logger.error("Falha: 'profiles.toml' não encontrado na raiz do projeto.")
        sys.exit(1)
        
    logger.success("Arquivos de configuração encontrados.")

    # 3. Teste de Parsing e Configuração
    try:
        engine = DFGEngine(project_dir=current_dir)
        target_name = engine.config["project"]["target"]
        target_config = engine.config["targets"][target_name]
        logger.success(f"Configuração carregada. Profile ativo: '{target_name}'")
    except Exception as e:
        logger.error(f"Erro ao ler as configurações: {e}")
        sys.exit(1)

    # 4. Teste de Conexão com o Banco (A prova de fogo)
    try:
        db_type = target_config.get("type")
        logger.info(f"Tentando conectar ao banco do tipo '{db_type}'...")
        
        adapter = AdapterFactory.get_adapter(db_type)
        adapter.connect(target_config)
        
        # Faz um "Ping" no banco
        result = adapter.execute("SELECT 1 AS ping")
        if result:
            logger.success("Conexão com o banco de dados estabelecida com sucesso!")
        else:
            logger.warn("Conexão estabelecida, mas o teste de 'SELECT 1' falhou.")
            
    except Exception as e:
        logger.error(f"Falha ao conectar no banco de dados: {e}")
        sys.exit(1)

    print("-" * 50)
    logger.success("Tudo certo! O Data Forge está pronto para a forja.")