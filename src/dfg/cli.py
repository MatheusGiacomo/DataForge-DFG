# src/dfg/cli.py (Trecho Atualizado)
import argparse
import sys
import os
from dfg.logging import logger
from dfg.initialization import init_command
from dfg.debug import debug_command
from dfg.docs import docs_command
from dfg.compile import compile_command

def run_command(args):
    from dfg.engine import DFGEngine
    engine = DFGEngine(project_dir=os.getcwd())
    engine.run()

def ingest_command(args):
    from dfg.engine import DFGEngine
    engine = DFGEngine(project_dir=os.getcwd())
    # O método ingest() no engine retorna True/False para controle de erro
    if not engine.ingest():
        sys.exit(1)

def transform_command(args):
    from dfg.engine import DFGEngine
    engine = DFGEngine(project_dir=os.getcwd())
    if not engine.transform():
        sys.exit(1)

def test_command(args):
    from dfg.engine import DFGEngine
    engine = DFGEngine(project_dir=os.getcwd())
    engine.test()

# Mapeamento atualizado com os novos comandos
COMMANDS = {
    "init": init_command,
    "ingest": ingest_command,       # Novo: Foco em Python/APIs
    "transform": transform_command, # Novo: Foco em SQL/Modelagem
    "run": run_command,             # Orquestra os dois acima
    "test": test_command,
    "debug": debug_command,
    "docs": docs_command,
    "compile": compile_command
}

def main():
    parser = argparse.ArgumentParser(description="Data Forge (DFG)", prog="dfg")
    subparsers = parser.add_subparsers(dest="command", help="Comandos disponíveis")
    
    # Adicione as definições dos novos subparsers
    subparsers.add_parser("init", help="Inicializa um novo projeto DFG")
    
    # Comandos de Execução
    subparsers.add_parser("ingest", help="Executa apenas a ingestão de dados (Extract & Load)")
    subparsers.add_parser("transform", help="Executa apenas as transformações (Models SQL)")
    subparsers.add_parser("run", help="Executa o pipeline completo (Ingest + Transform)")
    
    # Outros Comandos
    subparsers.add_parser("test", help="Testa a integridade dos modelos")
    subparsers.add_parser("debug", help="Verifica as configurações e conexão")
    subparsers.add_parser("docs", help="Gera a documentação e linhagem")
    subparsers.add_parser("compile", help="Gera os arquivos SQL finais (Dry Run)")

    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(0)
        
    handler = COMMANDS.get(args.command)
    if handler:
        try:
            handler(args)
        except Exception as e:
            logger.error(f"Erro fatal: {e}")
            sys.exit(1)

if __name__ == "__main__":
    main()