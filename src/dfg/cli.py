# src/dfg/cli.py
import argparse
import sys
import os
from dfg.logging import logger
from dfg.initialization import init_command
from dfg.debug import debug_command
from dfg.docs import docs_command
from dfg.compile import compile_command

# --- Handlers de Comando ---

def run_command(args):
    from dfg.engine import DFGEngine
    engine = DFGEngine(project_dir=os.getcwd())
    if not engine.run():
        sys.exit(1)

def ingest_command(args):
    from dfg.engine import DFGEngine
    engine = DFGEngine(project_dir=os.getcwd())
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

def log_command(args):
    from dfg.log_search import LogSearcher
    
    # Identifica se algum filtro de comando foi acionado
    cmd_filter = None
    possible_commands = ["run", "ingest", "transform", "test", "compile"]
    
    for cmd in possible_commands:
        if getattr(args, cmd):
            cmd_filter = cmd
            break
            
    searcher = LogSearcher(project_dir=os.getcwd())
    searcher.search(log_id=args.log_id, command_filter=cmd_filter, dump=args.dump)

# --- Mapeamento de Comandos ---

COMMANDS = {
    "init": init_command,
    "ingest": ingest_command,
    "transform": transform_command,
    "run": run_command,
    "test": test_command,
    "debug": debug_command,
    "docs": docs_command,
    "compile": compile_command,
    "log": log_command  # Adicionado o novo handler
}

def main():
    parser = argparse.ArgumentParser(description="Data Forge (DFG)", prog="dfg")
    subparsers = parser.add_subparsers(dest="command", help="Comandos disponíveis")
    
    # Comandos de Setup e Execução
    subparsers.add_parser("init", help="Inicializa um novo projeto DFG")
    subparsers.add_parser("ingest", help="Executa apenas a ingestão de dados (Extract & Load)")
    subparsers.add_parser("transform", help="Executa apenas as transformações (Models SQL)")
    subparsers.add_parser("run", help="Executa o pipeline completo (Ingest + Transform)")
    subparsers.add_parser("test", help="Testa a integridade dos modelos")
    subparsers.add_parser("debug", help="Verifica as configurações e conexão")
    subparsers.add_parser("docs", help="Gera a documentação e linhagem")
    subparsers.add_parser("compile", help="Gera os arquivos SQL finais (Dry Run)")
    
    # Novo Subparser: Log
    log_parser = subparsers.add_parser("log", help="Busca registros no log diário da Data Forge")
    log_parser.add_argument("log_id", help="ID do dia (ex: 220326DFG)")
    
    # Filtros de comando (Mutuamente exclusivos)
    cmd_group = log_parser.add_mutually_exclusive_group()
    cmd_group.add_argument("--run", action="store_true", help="Filtra apenas as execuções completas (run)")
    cmd_group.add_argument("--ingest", action="store_true", help="Filtra apenas comandos de ingestão")
    cmd_group.add_argument("--transform", action="store_true", help="Filtra apenas comandos de transformação")
    cmd_group.add_argument("--test", action="store_true", help="Filtra apenas comandos de teste")
    cmd_group.add_argument("--compile", action="store_true", help="Filtra apenas comandos de compilação")
    
    # Flag para exportar para arquivo de texto
    log_parser.add_argument("-d", "--dump", action="store_true", help="Exporta o resultado da busca para um arquivo .txt")

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




#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#