# src/dfg/diff.py
"""
Comparador de Execuções do DataForge (v0.4.0).

Compara dois arquivos ``run_results.json`` e exibe uma tabela de diferenças
no terminal, destacando regressões, recuperações, modelos novos e removidos.

O DataForge salva automaticamente o resultado anterior como
``run_results.prev.json`` antes de cada nova execução, de forma que
``dfg diff`` sempre compara a última com a penúltima execução.

Exemplo de saída:

    Comparação de Execuções
    ════════════════════════════════════════════════════════════════

    MODELO               ANTES           AGORA           VARIAÇÃO
    ─────────────────────────────────────────────────────────────
    stg_orders           ✓  0.23s       ✓  0.31s       +34.8% ↑
    fct_revenue          ✓  0.12s       ✗  FALHOU       REGREDIU ⚠
    ingest_clientes      ✗  FALHOU      ✓  1.45s        RECUPEROU ✓
    mart_kpis            —              ✓  0.05s        NOVO
    stg_clientes         ✓  0.08s       —               REMOVIDO

    Resumo: 1 regressão ⚠ | 1 recuperação ✓ | 1 novo | 1 removido
    Tempo total: anterior 0.43s → atual 1.81s (+320.9%)
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field


# ── Cores ANSI ──────────────────────────────────────────────────────────────

class _C:
    RESET  = "\033[0m"
    BOLD   = "\033[1m"
    DIM    = "\033[2m"
    GREEN  = "\033[92m"
    RED    = "\033[91m"
    YELLOW = "\033[93m"
    CYAN   = "\033[96m"
    GRAY   = "\033[90m"
    ORANGE = "\033[33m"


# =============================================================================
# Estruturas de dados
# =============================================================================


@dataclass
class ModelRun:
    """Representa o resultado de um único modelo em uma execução."""
    model: str
    status: str        # success | error | skipped
    execution_time: float = 0.0
    rows: int = 0
    error: str = ""


@dataclass
class DiffRow:
    """Representa uma linha da tabela de comparação."""
    model: str
    before: ModelRun | None
    after: ModelRun | None
    classification: str  # regression | recovery | new | removed | faster | slower | unchanged


@dataclass
class DiffReport:
    """Resultado completo da comparação entre duas execuções."""
    rows: list[DiffRow] = field(default_factory=list)
    before_command: str = ""
    after_command: str = ""
    before_generated_at: str = ""
    after_generated_at: str = ""

    @property
    def regressions(self) -> list[DiffRow]:
        return [r for r in self.rows if r.classification == "regression"]

    @property
    def recoveries(self) -> list[DiffRow]:
        return [r for r in self.rows if r.classification == "recovery"]

    @property
    def new_models(self) -> list[DiffRow]:
        return [r for r in self.rows if r.classification == "new"]

    @property
    def removed_models(self) -> list[DiffRow]:
        return [r for r in self.rows if r.classification == "removed"]

    @property
    def before_total_time(self) -> float:
        return sum(r.before.execution_time for r in self.rows if r.before)

    @property
    def after_total_time(self) -> float:
        return sum(r.after.execution_time for r in self.rows if r.after)


# =============================================================================
# Motor de Comparação
# =============================================================================


class DiffEngine:
    """
    Compara dois arquivos run_results.json e produz um DiffReport.

    Classificações possíveis por modelo:

    ``regression``  — estava passando, agora falha
    ``recovery``    — estava falhando, agora passa
    ``new``         — não existia antes, existe agora
    ``removed``     — existia antes, não existe mais
    ``faster``      — passou nas duas, mas ficou >= 15% mais rápido
    ``slower``      — passou nas duas, mas ficou >= 15% mais lento
    ``unchanged``   — mesmo status e tempo similar (delta < 15%)
    """

    _PERF_THRESHOLD = 0.15  # 15% de variação para classificar como faster/slower

    @classmethod
    def compare(cls, before: dict, after: dict) -> DiffReport:
        """
        Compara dois dicionários run_results.json e retorna um DiffReport.
        """
        report = DiffReport(
            before_command=before.get("metadata", {}).get("command", "?"),
            after_command=after.get("metadata", {}).get("command", "?"),
            before_generated_at=before.get("metadata", {}).get("generated_at", ""),
            after_generated_at=after.get("metadata", {}).get("generated_at", ""),
        )

        before_map = {
            r["model"]: ModelRun(
                model=r["model"],
                status=r.get("status", "unknown"),
                execution_time=float(r.get("execution_time", 0)),
                rows=int(r.get("rows", 0)),
                error=r.get("error", ""),
            )
            for r in before.get("results", [])
            if r.get("status") != "skipped"
        }

        after_map = {
            r["model"]: ModelRun(
                model=r["model"],
                status=r.get("status", "unknown"),
                execution_time=float(r.get("execution_time", 0)),
                rows=int(r.get("rows", 0)),
                error=r.get("error", ""),
            )
            for r in after.get("results", [])
            if r.get("status") != "skipped"
        }

        all_models = sorted(set(before_map) | set(after_map))

        for model in all_models:
            b = before_map.get(model)
            a = after_map.get(model)
            report.rows.append(DiffRow(
                model=model,
                before=b,
                after=a,
                classification=cls._classify(b, a),
            ))

        return report

    @classmethod
    def _classify(cls, before: ModelRun | None, after: ModelRun | None) -> str:
        """Classifica a mudança de um modelo entre as duas execuções."""
        if before is None:
            return "new"
        if after is None:
            return "removed"

        b_ok = before.status == "success"
        a_ok = after.status == "success"

        if b_ok and not a_ok:
            return "regression"
        if not b_ok and a_ok:
            return "recovery"

        if b_ok and a_ok and before.execution_time > 0:
            delta = (after.execution_time - before.execution_time) / before.execution_time
            if delta <= -cls._PERF_THRESHOLD:
                return "faster"
            if delta >= cls._PERF_THRESHOLD:
                return "slower"

        return "unchanged"


# =============================================================================
# Renderizador de Terminal
# =============================================================================


class DiffRenderer:
    """Renderiza um DiffReport como tabela colorida no terminal."""

    # Larguras de coluna (em caracteres)
    _W_MODEL  = 28
    _W_BEFORE = 18
    _W_AFTER  = 18
    _W_DELTA  = 22

    # Mapeamento de classificação → rótulo e cor
    _CLASSIFICATION_STYLE = {
        "regression": (_C.RED    + _C.BOLD, "REGREDIU ⚠"),
        "recovery":   (_C.GREEN  + _C.BOLD, "RECUPEROU ✓"),
        "new":        (_C.CYAN            , "NOVO"),
        "removed":    (_C.GRAY            , "REMOVIDO"),
        "faster":     (_C.GREEN           , "MAIS RÁPIDO ↑"),
        "slower":     (_C.YELLOW          , "MAIS LENTO ↓"),
        "unchanged":  (_C.GRAY            , "—"),
    }

    def render(self, report: DiffReport) -> str:
        lines: list[str] = []
        sep_wide = "═" * (self._W_MODEL + self._W_BEFORE + self._W_AFTER + self._W_DELTA + 6)
        sep_thin = "─" * (self._W_MODEL + self._W_BEFORE + self._W_AFTER + self._W_DELTA + 6)

        # ── Cabeçalho ──────────────────────────────────────────────────
        lines.append(f"\n{_C.BOLD}Comparação de Execuções{_C.RESET}")
        lines.append(f"{_C.GRAY}{sep_wide}{_C.RESET}")

        if report.before_generated_at:
            lines.append(
                f"{_C.GRAY}  Anterior: {report.before_command} "
                f"({report.before_generated_at[:19]}){_C.RESET}"
            )
        if report.after_generated_at:
            lines.append(
                f"{_C.GRAY}  Atual:    {report.after_command} "
                f"({report.after_generated_at[:19]}){_C.RESET}"
            )
        lines.append("")

        # ── Cabeçalho da tabela ────────────────────────────────────────
        header = (
            f"  {_C.BOLD}"
            f"{'MODELO':<{self._W_MODEL}}"
            f"{'ANTERIOR':<{self._W_BEFORE}}"
            f"{'ATUAL':<{self._W_AFTER}}"
            f"{'VARIAÇÃO':<{self._W_DELTA}}"
            f"{_C.RESET}"
        )
        lines.append(header)
        lines.append(f"  {_C.GRAY}{sep_thin}{_C.RESET}")

        # ── Linhas da tabela ───────────────────────────────────────────
        for row in report.rows:
            lines.append(self._format_row(row))

        lines.append(f"  {_C.GRAY}{sep_thin}{_C.RESET}")

        # ── Sumário ────────────────────────────────────────────────────
        lines.append(self._format_summary(report))

        return "\n".join(lines)

    def _format_status(self, run: ModelRun | None) -> str:
        """Formata o status de um modelo em uma célula da tabela."""
        if run is None:
            return f"{_C.GRAY}{'—':<{self._W_BEFORE}}{_C.RESET}"

        if run.status == "success":
            time_str = f"{run.execution_time:.3f}s"
            return f"{_C.GREEN}✓ {time_str:<{self._W_BEFORE - 2}}{_C.RESET}"
        else:
            return f"{_C.RED}✗ {'FALHOU':<{self._W_BEFORE - 2}}{_C.RESET}"

    def _format_delta(self, row: DiffRow) -> str:
        """Formata a coluna de variação com cor e símbolo."""
        color, label = self._CLASSIFICATION_STYLE.get(row.classification, (_C.GRAY, "?"))

        # Para mudanças de performance, adiciona o percentual
        if row.classification in ("faster", "slower") and row.before and row.after:
            if row.before.execution_time > 0:
                pct = (row.after.execution_time - row.before.execution_time) / row.before.execution_time * 100
                sign = "+" if pct > 0 else ""
                label = f"{sign}{pct:.1f}% {label}"

        return f"{color}{label:<{self._W_DELTA}}{_C.RESET}"

    def _format_row(self, row: DiffRow) -> str:
        """Formata uma linha completa da tabela de comparação."""
        model_cell = f"{row.model:<{self._W_MODEL}}"
        before_cell = self._format_status(row.before)
        after_cell = self._format_status(row.after)

        # Destaca o nome para regressões
        if row.classification == "regression":
            model_cell = f"{_C.RED}{_C.BOLD}{row.model:<{self._W_MODEL}}{_C.RESET}"
        elif row.classification == "recovery":
            model_cell = f"{_C.GREEN}{_C.BOLD}{row.model:<{self._W_MODEL}}{_C.RESET}"

        return f"  {model_cell}{before_cell}{after_cell}{self._format_delta(row)}"

    def _format_summary(self, report: DiffReport) -> str:
        """Formata o sumário executivo após a tabela."""
        parts: list[str] = []

        if report.regressions:
            parts.append(f"{_C.RED}{_C.BOLD}{len(report.regressions)} regressão(ões) ⚠{_C.RESET}")
        if report.recoveries:
            parts.append(f"{_C.GREEN}{len(report.recoveries)} recuperação(ões) ✓{_C.RESET}")
        if report.new_models:
            parts.append(f"{_C.CYAN}{len(report.new_models)} novo(s){_C.RESET}")
        if report.removed_models:
            parts.append(f"{_C.GRAY}{len(report.removed_models)} removido(s){_C.RESET}")

        summary_line = "  " + " | ".join(parts) if parts else f"  {_C.GREEN}Sem mudanças relevantes.{_C.RESET}"

        # Variação de tempo total
        bt = report.before_total_time
        at = report.after_total_time
        if bt > 0 and at > 0:
            delta_pct = (at - bt) / bt * 100
            sign = "+" if delta_pct > 0 else ""
            time_color = _C.RED if delta_pct > 20 else (_C.GREEN if delta_pct < -10 else _C.GRAY)
            time_line = (
                f"  {_C.GRAY}Tempo total: "
                f"{bt:.3f}s → {at:.3f}s "
                f"({time_color}{sign}{delta_pct:.1f}%{_C.GRAY}){_C.RESET}"
            )
        else:
            time_line = ""

        lines = [summary_line]
        if time_line:
            lines.append(time_line)
        lines.append("")
        return "\n".join(lines)


# =============================================================================
# API de alto nível
# =============================================================================


def load_run_result(path: str) -> dict | None:
    """Lê um arquivo run_results.json. Retorna None se não existir."""
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def run_diff(project_dir: str) -> str:
    """
    Compara a execução atual com a anterior e retorna o relatório formatado.

    Arquivos lidos:
        target/run_results.json       — execução atual
        target/run_results.prev.json  — execução anterior

    Retorna uma string com a tabela formatada ou uma mensagem de erro.
    """
    target_dir = os.path.join(project_dir, "target")
    current_path = os.path.join(target_dir, "run_results.json")
    prev_path = os.path.join(target_dir, "run_results.prev.json")

    current = load_run_result(current_path)
    previous = load_run_result(prev_path)

    if not current:
        return (
            f"{_C.YELLOW}run_results.json não encontrado em target/. "
            f"Execute 'dfg run' primeiro.{_C.RESET}"
        )

    if not previous:
        return (
            f"{_C.YELLOW}run_results.prev.json não encontrado em target/.\n"
            f"O comparativo fica disponível a partir da segunda execução.{_C.RESET}"
        )

    report = DiffEngine.compare(previous, current)
    return DiffRenderer().render(report)