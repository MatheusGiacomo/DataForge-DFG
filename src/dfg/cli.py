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
    # O motor agora gera artefatos internamente durante o run
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
    """
    Motor de busca de logs potente.
    Permite filtrar por ID e comando específico, além de exportar para .txt.
    """
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
    "docs": docs_command,    # O handler em docs.py agora trata a flag --serve
    "compile": compile_command,
    "log": log_command 
}

def main():
    parser = argparse.ArgumentParser(description="Data Forge (DFG)", prog="dfg")
    subparsers = parser.add_subparsers(dest="command", help="Comandos disponíveis")
    
    # --- Configurações de Subparsers ---

    subparsers.add_parser("init", help="Inicializa um novo projeto DFG")
    subparsers.add_parser("ingest", help="Executa apenas a ingestão de dados (Extract & Load)")
    subparsers.add_parser("transform", help="Executa apenas as transformações (Models SQL)")
    subparsers.add_parser("run", help="Executa o pipeline completo e gera artefatos de observabilidade")
    subparsers.add_parser("test", help="Testa a integridade dos modelos")
    subparsers.add_parser("debug", help="Verifica as configurações e conexão")
    subparsers.add_parser("compile", help="Gera os arquivos SQL finais e o manifest.json (Dry Run)")
    
    # Documentação e Grafo Visual
    docs_parser = subparsers.add_parser("docs", help="Gera a documentação e o grafo de linhagem (DAG)")
    docs_parser.add_argument("--serve", action="store_true", help="Inicia o servidor local para visualizar o grafo interativo")
    
    # Busca de Logs
    log_parser = subparsers.add_parser("log", help="Busca registros detalhados no log diário")
    log_parser.add_argument("log_id", help="ID do dia (ex: 220326DFG)")
    
    cmd_group = log_parser.add_mutually_exclusive_group()
    cmd_group.add_argument("--run", action="store_true", help="Filtra apenas registros de 'run'")
    cmd_group.add_argument("--ingest", action="store_true", help="Filtra apenas registros de 'ingest'")
    cmd_group.add_argument("--transform", action="store_true", help="Filtra apenas registros de 'transform'")
    cmd_group.add_argument("--test", action="store_true", help="Filtra apenas registros de 'test'")
    cmd_group.add_argument("--compile", action="store_true", help="Filtra apenas registros de 'compile'")
    
    log_parser.add_argument("-d", "--dump", action="store_true", help="Exporta a busca para um arquivo .txt (ID.txt)")

    # --- Execução ---

    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(0)
        
    handler = COMMANDS.get(args.command)
    if handler:
        try:
            handler(args)
        except Exception as e:
            logger.error(f"Erro fatal durante a execução do comando '{args.command}': {e}")
            sys.exit(1)

if __name__ == "__main__":
    main()