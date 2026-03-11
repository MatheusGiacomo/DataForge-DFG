import argparse
import os
import sys
from dfg.engine import DFGEngine

def main():
    parser = argparse.ArgumentParser(
        description="Data Forge (DFG) - Ferramenta de ELT baseada em Python.",
        prog="dfg"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Comandos disponíveis")

    # Comando: dfg run
    subparsers.add_parser("run", help="Executa os modelos do projeto DFG")
    
    # Comando: dfg test (Agora no lugar certo)
    subparsers.add_parser("test", help="Testa os modelos do projeto DFG")
    
    args = parser.parse_args()
    current_dir = os.getcwd()

    if args.command == "run":
        try:
            engine = DFGEngine(project_dir=current_dir)
            engine.run()
        except Exception as e:
            print(f"\n[ERRO FATAL] {e}")
            sys.exit(1)
    elif args.command == "test":
        print("Comando 'test' ainda não implementado. Em breve!")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()