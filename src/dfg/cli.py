# src/dfg/cli.py
"""
Interface de Linha de Comando (CLI) do DataForge.

Comandos:
    dfg init        — Inicializa estrutura do projeto
    dfg run         — Executa o pipeline completo
    dfg ingest      — Executa apenas modelos Python
    dfg transform   — Executa apenas modelos SQL
    dfg test        — Valida contratos de dados
    dfg compile     — Compila modelos e análises (Dry Run)
    dfg seed        — Carrega CSVs estáticos no banco
    dfg snapshot    — Executa snapshots SCD Tipo 2
    dfg docs        — Gera documentação HTML com grafo de linhagem
    dfg lineage     — Exibe linhagem de dados no terminal (v0.4.0)
    dfg diff        — Compara a última execução com a anterior (v0.4.0)
    dfg debug       — Diagnóstico do ambiente
    dfg log         — Busca registros no log diário

Flags adicionados:
    --select        — Seleção de modelos (v0.3.0)
    --target        — Sobrescreve o ambiente ativo (v0.3.0)
    --var           — Variáveis de runtime para templates (v0.3.0)
    --dry-run       — Simula execução sem tocar no banco (v0.4.0)
"""
import argparse
import os
import sys


# ------------------------------------------------------------------
# Helpers de instanciação
# ------------------------------------------------------------------

def _get_engine(args):
    """Instancia DFGEngine com suporte a --target e --var."""
    from dfg.engine import DFGEngine
    return DFGEngine(
        project_dir=os.getcwd(),
        override_target=getattr(args, "target", None),
        runtime_vars=_parse_vars(getattr(args, "var", None) or []),
    )


def _parse_vars(var_list: list[str]) -> dict:
    """Converte lista de 'chave=valor' em dicionário."""
    result = {}
    for item in var_list:
        if "=" not in item:
            print(f"[error] --var '{item}' inválido. Use: --var chave=valor", file=sys.stderr)
            sys.exit(1)
        key, _, value = item.partition("=")
        result[key.strip()] = value.strip()
    return result


def _get_select(args) -> list[str] | None:
    select = getattr(args, "select", None)
    return select if select else None


def _get_dry_run(args) -> bool:
    return getattr(args, "dry_run", False)


# ------------------------------------------------------------------
# Handlers
# ------------------------------------------------------------------

def _handle_init(args) -> None:
    from dfg.initialization import init_command
    init_command(args)


def _handle_run(args) -> None:
    result = _get_engine(args).run(select=_get_select(args), dry_run=_get_dry_run(args))
    if result is False:
        sys.exit(1)


def _handle_ingest(args) -> None:
    result = _get_engine(args).ingest(select=_get_select(args), dry_run=_get_dry_run(args))
    if result is False:
        sys.exit(1)


def _handle_transform(args) -> None:
    result = _get_engine(args).transform(select=_get_select(args), dry_run=_get_dry_run(args))
    if result is False:
        sys.exit(1)


def _handle_test(args) -> None:
    _get_engine(args).test(select=_get_select(args))


def _handle_compile(args) -> None:
    from dfg.logging import logger
    try:
        _get_engine(args).compile()
    except Exception as e:
        logger.error(f"Erro ao compilar projeto: {e}")
        sys.exit(1)


def _handle_seed(args) -> None:
    from dfg.logging import logger
    from dfg.seed import SeedRunner
    try:
        SeedRunner(_get_engine(args)).run()
    except Exception as e:
        logger.error(f"Erro ao executar seeds: {e}")
        sys.exit(1)


def _handle_snapshot(args) -> None:
    from dfg.logging import logger
    try:
        _get_engine(args).snapshots()
    except Exception as e:
        logger.error(f"Erro ao executar snapshots: {e}")
        sys.exit(1)


def _handle_docs(args) -> None:
    from dfg.docs import docs_command
    docs_command(args)


def _handle_lineage(args) -> None:
    """
    Exibe o grafo de linhagem no terminal.

    Sem --model: exibe o grafo completo em camadas topológicas.
    Com --model:  exibe upstream e downstream do modelo especificado.
    """
    from dfg.lineage import LineageRenderer
    engine = _get_engine(args)
    model  = getattr(args, "model", None)
    output = LineageRenderer.from_engine(engine, model=model)
    print(output)


def _handle_diff(args) -> None:
    """Compara a última execução com a anterior."""
    from dfg.diff import run_diff
    output = run_diff(os.getcwd())
    print(output)


def _handle_debug(args) -> None:
    from dfg.debug import debug_command
    debug_command(args)


def _handle_log(args) -> None:
    from dfg.log_search import LogSearcher
    possible = ["run", "ingest", "transform", "test", "compile", "docs", "snapshot", "seed"]
    cmd_filter = next((c for c in possible if getattr(args, c, False)), None)
    LogSearcher(project_dir=os.getcwd()).search(
        log_id=args.log_id, command_filter=cmd_filter, dump=args.dump
    )


# ------------------------------------------------------------------
# Mapeamento
# ------------------------------------------------------------------

_COMMANDS = {
    "init":      _handle_init,
    "run":       _handle_run,
    "ingest":    _handle_ingest,
    "transform": _handle_transform,
    "test":      _handle_test,
    "compile":   _handle_compile,
    "seed":      _handle_seed,
    "snapshot":  _handle_snapshot,
    "docs":      _handle_docs,
    "lineage":   _handle_lineage,
    "diff":      _handle_diff,
    "debug":     _handle_debug,
    "log":       _handle_log,
}


# ------------------------------------------------------------------
# Parser helpers
# ------------------------------------------------------------------

def _add_execution_args(parser: argparse.ArgumentParser, include_dry_run: bool = True) -> None:
    """Adiciona --select, --target, --var e opcionalmente --dry-run."""
    parser.add_argument(
        "--select",
        nargs="+",
        metavar="SELETOR",
        help=(
            "Seleciona modelos para execução. "
            "Sintaxe: 'modelo', '+modelo' (ancestrais), 'modelo+' (descendentes), "
            "'tag:nome'. Múltiplos seletores são combinados por união."
        ),
    )
    parser.add_argument(
        "--target",
        metavar="AMBIENTE",
        help="Sobrescreve o ambiente ativo do dfg_project.toml (ex: prod, staging).",
    )
    parser.add_argument(
        "--var",
        action="append",
        metavar="CHAVE=VALOR",
        help=(
            "Define variáveis de runtime para templates SQL via {{ var('nome') }}. "
            "Pode ser repetido: --var data=2024-01-01 --var limite=100"
        ),
    )
    if include_dry_run:
        parser.add_argument(
            "--dry-run",
            action="store_true",
            dest="dry_run",
            help=(
                "Simula a execução: exibe o que seria executado "
                "sem fazer nenhuma alteração no banco de dados."
            ),
        )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="DataForge (DFG) — Motor de ELT em Python puro",
        prog="dfg",
    )
    sub = parser.add_subparsers(dest="command", metavar="comando")

    # Comandos simples (sem flags extras)
    sub.add_parser("init",    help="Inicializa um novo projeto DataForge")
    sub.add_parser("seed",    help="Carrega arquivos CSV estáticos no banco")
    sub.add_parser("snapshot",help="Executa snapshots SCD Tipo 2")
    sub.add_parser("debug",   help="Diagnóstico do ambiente e conexão com o banco")
    sub.add_parser("diff",    help="Compara a última execução com a anterior")

    # Comandos de execução com flags completos
    for cmd, help_text in [
        ("run",       "Executa o pipeline completo (ingest + transform)"),
        ("ingest",    "Executa apenas a ingestão de dados (modelos Python)"),
        ("transform", "Executa apenas as transformações (modelos SQL)"),
    ]:
        p = sub.add_parser(cmd, help=help_text)
        _add_execution_args(p, include_dry_run=True)

    # test e compile: --select/--target/--var mas sem --dry-run
    for cmd, help_text in [
        ("test",    "Valida os contratos de dados dos modelos"),
        ("compile", "Compila os modelos e análises, gera manifest.json"),
    ]:
        p = sub.add_parser(cmd, help=help_text)
        _add_execution_args(p, include_dry_run=False)

    # dfg lineage [--model NOME]
    lineage_p = sub.add_parser(
        "lineage",
        help="Exibe o grafo de linhagem de dados no terminal",
    )
    lineage_p.add_argument(
        "--model",
        metavar="NOME",
        help=(
            "Exibe upstream e downstream de um modelo específico. "
            "Sem este flag, exibe o grafo completo do projeto."
        ),
    )
    _add_execution_args(lineage_p, include_dry_run=False)

    # dfg docs [--serve]
    docs_p = sub.add_parser(
        "docs",
        help="Gera documentação HTML com o grafo de linhagem (DAG)",
    )
    docs_p.add_argument(
        "--serve",
        action="store_true",
        help="Inicia um servidor local para visualizar o grafo interativo",
    )

    # dfg log LOG_ID [--run|...] [-d]
    log_p = sub.add_parser("log", help="Busca registros no arquivo de log diário")
    log_p.add_argument("log_id", help="ID da sessão no formato DDMMYYDFG (ex: 150426DFG)")

    cmd_group = log_p.add_mutually_exclusive_group()
    for c in ["run", "ingest", "transform", "test", "compile", "docs", "snapshot", "seed"]:
        cmd_group.add_argument(f"--{c}", action="store_true", help=f"Filtra registros do comando '{c}'")

    log_p.add_argument("-d", "--dump", action="store_true", help="Exporta para arquivo .txt")

    return parser


# ------------------------------------------------------------------
# Ponto de entrada
# ------------------------------------------------------------------

def main() -> None:
    parser  = _build_parser()
    args    = parser.parse_args()

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
        try:
            from dfg.logging import logger
            logger.error(f"Erro fatal durante '{args.command}': {e}")
        except Exception:
            print(f"[error] Erro fatal durante '{args.command}': {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()