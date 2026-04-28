# src/dfg/cli.py
"""
Interface de Linha de Comando (CLI) do DataForge.

Ponto de entrada único para todos os comandos. O logger é configurado
dentro de cada handler para garantir que funcione corretamente
mesmo antes da DFGEngine ser instanciada.

Comandos disponíveis:
    dfg init        — Inicializa estrutura do projeto
    dfg run         — Executa o pipeline completo
    dfg ingest      — Executa apenas modelos Python (Extract & Load)
    dfg transform   — Executa apenas modelos SQL (Transform)
    dfg test        — Valida contratos de dados
    dfg compile     — Compila modelos e gera manifest.json (Dry Run)
    dfg seed        — Carrega arquivos CSV estáticos no banco
    dfg snapshot    — Executa snapshots SCD Tipo 2
    dfg docs        — Gera documentação com grafo de linhagem
    dfg debug       — Diagnóstico do ambiente
    dfg log         — Busca registros no log diário
"""
import argparse
import os
import sys

# ------------------------------------------------------------------
# Handlers de Comando
# ------------------------------------------------------------------


def _handle_init(args) -> None:
    from dfg.initialization import init_command
    init_command(args)


def _handle_run(args) -> None:
    from dfg.engine import DFGEngine
    engine = DFGEngine(project_dir=os.getcwd())
    result = engine.run()
    if result is False:
        sys.exit(1)


def _handle_ingest(args) -> None:
    from dfg.engine import DFGEngine
    engine = DFGEngine(project_dir=os.getcwd())
    if not engine.ingest():
        sys.exit(1)


def _handle_transform(args) -> None:
    from dfg.engine import DFGEngine
    engine = DFGEngine(project_dir=os.getcwd())
    if not engine.transform():
        sys.exit(1)


def _handle_test(args) -> None:
    from dfg.engine import DFGEngine
    engine = DFGEngine(project_dir=os.getcwd())
    engine.test()


def _handle_compile(args) -> None:
    from dfg.engine import DFGEngine
    from dfg.logging import logger
    try:
        engine = DFGEngine(project_dir=os.getcwd())
        engine.compile()
    except Exception as e:
        logger.error(f"Erro ao compilar projeto: {e}")
        sys.exit(1)


def _handle_seed(args) -> None:
    from dfg.engine import DFGEngine
    from dfg.logging import logger
    from dfg.seed import SeedRunner
    try:
        engine = DFGEngine(project_dir=os.getcwd())
        runner = SeedRunner(engine)
        runner.run()
    except Exception as e:
        logger.error(f"Erro ao executar seeds: {e}")
        sys.exit(1)


def _handle_snapshot(args) -> None:
    from dfg.engine import DFGEngine
    from dfg.logging import logger
    try:
        engine = DFGEngine(project_dir=os.getcwd())
        engine.snapshots()
    except Exception as e:
        logger.error(f"Erro ao executar snapshots: {e}")
        sys.exit(1)


def _handle_docs(args) -> None:
    from dfg.docs import docs_command
    docs_command(args)


def _handle_debug(args) -> None:
    from dfg.debug import debug_command
    debug_command(args)


def _handle_log(args) -> None:
    from dfg.log_search import LogSearcher

    # Determina o filtro de comando a partir dos flags mutuamente exclusivos
    possible_commands = ["run", "ingest", "transform", "test", "compile", "docs", "snapshot", "seed"]
    cmd_filter = next(
        (cmd for cmd in possible_commands if getattr(args, cmd, False)),
        None,
    )

    searcher = LogSearcher(project_dir=os.getcwd())
    searcher.search(log_id=args.log_id, command_filter=cmd_filter, dump=args.dump)


# ------------------------------------------------------------------
# Mapeamento de comandos
# ------------------------------------------------------------------

_COMMANDS = {
    "init": _handle_init,
    "run": _handle_run,
    "ingest": _handle_ingest,
    "transform": _handle_transform,
    "test": _handle_test,
    "compile": _handle_compile,
    "seed": _handle_seed,
    "snapshot": _handle_snapshot,
    "docs": _handle_docs,
    "debug": _handle_debug,
    "log": _handle_log,
}


# ------------------------------------------------------------------
# Configuração do parser de argumentos
# ------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="DataForge (DFG) — Motor de ELT em Python puro",
        prog="dfg",
    )
    sub = parser.add_subparsers(dest="command", metavar="comando")

    # Comandos simples (sem argumentos extras)
    sub.add_parser("init", help="Inicializa um novo projeto DataForge")
    sub.add_parser("run", help="Executa o pipeline completo (ingest + transform)")
    sub.add_parser("ingest", help="Executa apenas a ingestão de dados (modelos Python)")
    sub.add_parser("transform", help="Executa apenas as transformações (modelos SQL)")
    sub.add_parser("test", help="Valida os contratos de dados dos modelos")
    sub.add_parser("compile", help="Compila os modelos e gera manifest.json (Dry Run)")
    sub.add_parser("seed", help="Carrega arquivos CSV estáticos no banco de dados")
    sub.add_parser("snapshot", help="Executa snapshots SCD Tipo 2")
    sub.add_parser("debug", help="Diagnóstico do ambiente e conexão com o banco")

    # dfg docs [--serve]
    docs_parser = sub.add_parser(
        "docs",
        help="Gera documentação HTML com o grafo de linhagem (DAG)",
    )
    docs_parser.add_argument(
        "--serve",
        action="store_true",
        help="Inicia um servidor local para visualizar o grafo interativo",
    )

    # dfg log LOG_ID [--run|--ingest|...] [-d]
    log_parser = sub.add_parser(
        "log",
        help="Busca registros no arquivo de log diário",
    )
    log_parser.add_argument(
        "log_id",
        help="ID da sessão no formato DDMMYYDFG (ex: 150426DFG)",
    )

    cmd_group = log_parser.add_mutually_exclusive_group()
    cmd_group.add_argument("--run", action="store_true", help="Filtra registros do comando 'run'")
    cmd_group.add_argument("--ingest", action="store_true", help="Filtra registros do comando 'ingest'")
    cmd_group.add_argument("--transform", action="store_true", help="Filtra registros do comando 'transform'")
    cmd_group.add_argument("--test", action="store_true", help="Filtra registros do comando 'test'")
    cmd_group.add_argument("--compile", action="store_true", help="Filtra registros do comando 'compile'")
    cmd_group.add_argument("--docs", action="store_true", help="Filtra registros do comando 'docs'")
    cmd_group.add_argument("--snapshot", action="store_true", help="Filtra registros do comando 'snapshot'")
    cmd_group.add_argument("--seed", action="store_true", help="Filtra registros do comando 'seed'")

    log_parser.add_argument(
        "-d", "--dump",
        action="store_true",
        help="Exporta o resultado da busca para um arquivo .txt",
    )

    return parser


# ------------------------------------------------------------------
# Ponto de entrada
# ------------------------------------------------------------------


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    handler = _COMMANDS.get(args.command)
    if not handler:
        parser.print_help()
        sys.exit(1)

    try:
        handler(args)
    except KeyboardInterrupt:
        print("\nOperação interrompida pelo usuário.")
        sys.exit(130)
    except SystemExit:
        raise
    except Exception as e:
        # Tenta usar o logger se disponível; caso contrário, usa print
        try:
            from dfg.logging import logger
            logger.error(f"Erro fatal durante '{args.command}': {e}")
        except Exception:
            print(f"[error] Erro fatal durante '{args.command}': {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
