# src/dfg/cli.py (Trecho Atualizado)
import argparse
import sys
import os
from dfg.logger import logger
from dfg.initialization import init_command
from dfg.debug import debug_command # <-- Nova importação
from dfg.docs import docs_command
from dfg.compile import compile_command

def run_command(args):
    from dfg.engine import DFGEngine
    engine = DFGEngine(project_dir=os.getcwd())
    engine.run()

def test_command(args):
    from dfg.engine import DFGEngine
    engine = DFGEngine(project_dir=os.getcwd())
    engine.test() # <-- Chama a nossa nova rotina de Data Contracts

COMMANDS = {
    "init": init_command,
    "run": run_command,
    "test": test_command,
    "debug": debug_command,
    "docs": docs_command,
    "compile": compile_command
}

def main():
    parser = argparse.ArgumentParser(description="Data Forge (DFG)", prog="dfg")
    subparsers = parser.add_subparsers(dest="command", help="Comandos disponíveis")
    
    subparsers.add_parser("init", help="Inicializa um novo projeto DFG")
    subparsers.add_parser("run", help="Executa os modelos do projeto DFG")
    subparsers.add_parser("test", help="Testa a integridade dos modelos (Data Contracts)")
    subparsers.add_parser("debug", help="Verifica as configurações e conexão com o banco")
    subparsers.add_parser("docs", help="Gera a documentação e linhagem dos modelos")
    subparsers.add_parser("compile", help="Gera os arquivos SQL finais sem executá-los no banco (Dry Run)")
    
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