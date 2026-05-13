# src/dfg/lineage.py
"""
Renderizador de Linhagem de Dados no Terminal (v0.4.0).

Exibe o grafo de dependências do projeto DataForge como árvore ASCII
diretamente no terminal, sem necessidade de abrir o browser.

Modos de exibição:
    dfg lineage                — exibe o grafo completo do projeto
    dfg lineage --model nome   — exibe upstream + downstream de um modelo

Exemplo de saída (modelo específico):

    Linhagem de: fct_revenue
    ════════════════════════════════════════════════

    UPSTREAM (o que fct_revenue precisa)
      ingest_pedidos        [python]
      └── stg_pedidos       [sql/table]
               └── fct_revenue  [sql/table]  ◄ você está aqui

    DOWNSTREAM (quem depende de fct_revenue)
      fct_revenue  [sql/table]  ◄ você está aqui
      └── mart_kpis  [sql/table]

Exemplo de saída (grafo completo):

    DataForge — Grafo de Linhagem Completo (5 modelos)
    ════════════════════════════════════════════════

    ● ingest_pedidos       [python]
    ● ingest_clientes      [python]
         │
         ▼
    ○ stg_pedidos          [sql/table]
    ○ stg_clientes         [sql/table]
         │
         ▼
    ○ fct_revenue          [sql/table]
"""
from __future__ import annotations

import json
import os
from collections import defaultdict


# ── Cores ANSI ─────────────────────────────────────────────────────────────

class _C:
    RESET   = "\033[0m"
    BOLD    = "\033[1m"
    DIM     = "\033[2m"
    BLUE    = "\033[94m"
    GREEN   = "\033[92m"
    ORANGE  = "\033[33m"
    CYAN    = "\033[96m"
    GRAY    = "\033[90m"
    YELLOW  = "\033[93m"
    RED     = "\033[91m"


# ── Configuração visual ─────────────────────────────────────────────────────

_TYPE_COLORS = {
    "python": _C.BLUE,
    "sql":    _C.GREEN,
}

_NODE_SYMBOLS = {
    "python": "●",  # sólido = dados vivos (ingestão)
    "sql":    "○",  # vazio  = transformação
}


# =============================================================================
# Renderizador Principal
# =============================================================================


class LineageRenderer:
    """
    Renderiza o grafo de linhagem do DataForge em ASCII no terminal.

    Parâmetros
    ----------
    project_dir : str
        Diretório raiz do projeto. Usado para ler o manifest.json.
    """

    def __init__(self, project_dir: str):
        self.project_dir = project_dir
        self._nodes: dict = {}
        self._deps: dict[str, list[str]] = {}
        self._reverse_deps: dict[str, set[str]] = defaultdict(set)

    # ------------------------------------------------------------------
    # Carregamento de dados
    # ------------------------------------------------------------------

    def _load_manifest(self) -> bool:
        """
        Carrega o manifest.json do diretório target/.

        Retorna True em caso de sucesso, False se o arquivo não existir.
        """
        manifest_path = os.path.join(self.project_dir, "target", "manifest.json")
        if not os.path.exists(manifest_path):
            return False

        with open(manifest_path, encoding="utf-8") as f:
            manifest = json.load(f)

        self._nodes = manifest.get("nodes", {})
        self._deps = manifest.get("dependencies", {})

        # Constrói mapa reverso para navegação downstream
        for model, upstream in self._deps.items():
            for dep in upstream:
                self._reverse_deps[dep].add(model)

        return True

    def _load_from_engine(self, engine) -> None:
        """Carrega dados diretamente do engine (quando manifest não existe)."""
        engine.discover_models()
        self._deps = engine.dependencies_map

        for name, info in engine.models_registry.items():
            mat = info.get("config", {}).get("materialized", "memory")
            self._nodes[name] = {
                "type": info.get("type", "sql"),
                "materialized": mat,
                "description": info.get("config", {}).get("description", ""),
                "depends_on": self._deps.get(name, []),
            }

        for model, upstream in self._deps.items():
            for dep in upstream:
                self._reverse_deps[dep].add(model)

    # ------------------------------------------------------------------
    # Topologia
    # ------------------------------------------------------------------

    def _topological_levels(self) -> list[list[str]]:
        """
        Agrupa modelos por nível topológico para exibição em camadas.

        Nível 0 = raízes (sem dependências)
        Nível N = modelos cujas dependências estão no nível N-1
        """
        in_degree: dict[str, int] = {n: 0 for n in self._nodes}
        for deps in self._deps.values():
            for dep in deps:
                if dep in in_degree:
                    in_degree[dep] = in_degree.get(dep, 0)

        # Reconstrói in_degree corretamente
        in_degree = {n: 0 for n in self._nodes}
        for model, upstream in self._deps.items():
            for dep in upstream:
                if model in in_degree:
                    in_degree[model] += 1

        levels: list[list[str]] = []
        remaining = set(self._nodes.keys())

        while remaining:
            # Modelos com in_degree 0 (prontos para este nível)
            current_level = sorted(
                n for n in remaining if in_degree.get(n, 0) == 0
            )
            if not current_level:
                # Ciclo detectado ou dependência externa — adiciona o que sobrou
                current_level = sorted(remaining)

            levels.append(current_level)
            remaining -= set(current_level)

            # Reduz in_degree dos descendentes
            for node in current_level:
                for downstream in self._reverse_deps.get(node, set()):
                    in_degree[downstream] = max(0, in_degree.get(downstream, 1) - 1)

        return levels

    def _get_ancestors(self, model: str, visited: set | None = None) -> list[str]:
        """Retorna ancestrais em ordem topológica (BFS reverso)."""
        if visited is None:
            visited = set()
        ancestors = []
        for dep in self._deps.get(model, []):
            if dep not in visited and dep in self._nodes:
                visited.add(dep)
                ancestors.extend(self._get_ancestors(dep, visited))
                ancestors.append(dep)
        return ancestors

    def _get_descendants(self, model: str, visited: set | None = None) -> list[str]:
        """Retorna descendentes em ordem topológica (BFS direto)."""
        if visited is None:
            visited = set()
        descendants = []
        for child in sorted(self._reverse_deps.get(model, set())):
            if child not in visited and child in self._nodes:
                visited.add(child)
                descendants.append(child)
                descendants.extend(self._get_descendants(child, visited))
        return descendants

    # ------------------------------------------------------------------
    # Formatação de nós
    # ------------------------------------------------------------------

    def _format_node(
        self,
        name: str,
        highlight: bool = False,
        indent: int = 0,
        connector: str = "",
    ) -> str:
        """Formata um nó do DAG com cor, tipo e materialização."""
        info = self._nodes.get(name, {})
        model_type = info.get("type", "sql")
        materialized = info.get("materialized", "")
        mat_label = f"/{materialized}" if materialized and materialized != "memory" else ""

        color = _TYPE_COLORS.get(model_type, _C.RESET)
        symbol = _NODE_SYMBOLS.get(model_type, "○")

        type_badge = f"{_C.DIM}[{model_type}{mat_label}]{_C.RESET}"
        node_label = f"{_C.BOLD}{name}{_C.RESET}" if highlight else name
        highlight_marker = f"  {_C.ORANGE}{_C.BOLD}◄ você está aqui{_C.RESET}" if highlight else ""

        prefix = " " * indent + connector
        return (
            f"{prefix}{color}{symbol}{_C.RESET} "
            f"{node_label}  {type_badge}{highlight_marker}"
        )

    # ------------------------------------------------------------------
    # Renderização: modelo específico
    # ------------------------------------------------------------------

    def render_model(self, model_name: str) -> str:
        """
        Renderiza a linhagem completa de um modelo específico.

        Exibe a cadeia upstream (dependências transitivas) e downstream
        (quem depende deste modelo, transitivamente).
        """
        if model_name not in self._nodes:
            available = ", ".join(sorted(self._nodes.keys()))
            return (
                f"{_C.RED}Modelo '{model_name}' não encontrado.{_C.RESET}\n"
                f"Modelos disponíveis: {available}"
            )

        lines: list[str] = []
        sep = "═" * 50

        lines.append(f"\n{_C.BOLD}Linhagem de: {_C.CYAN}{model_name}{_C.RESET}")
        lines.append(f"{_C.GRAY}{sep}{_C.RESET}")

        # ── Upstream ───────────────────────────────────────────────────
        ancestors = self._get_ancestors(model_name)

        lines.append(f"\n{_C.YELLOW}{_C.BOLD}UPSTREAM{_C.RESET} — o que '{model_name}' consome\n")

        if not ancestors:
            lines.append(f"  {_C.GRAY}(sem dependências — este é um modelo raiz){_C.RESET}\n")
        else:
            for i, ancestor in enumerate(ancestors):
                is_last = (i == len(ancestors) - 1)
                connector = "  └── " if is_last else "  ├── "
                lines.append(self._format_node(ancestor, indent=0, connector=connector))

        # O próprio modelo, como ponto focal
        lines.append(self._format_node(model_name, highlight=True, connector="  └── "))

        # ── Downstream ─────────────────────────────────────────────────
        descendants = self._get_descendants(model_name)

        lines.append(f"\n{_C.YELLOW}{_C.BOLD}DOWNSTREAM{_C.RESET} — quem consome '{model_name}'\n")

        # O próprio modelo, ponto de partida
        lines.append(self._format_node(model_name, highlight=True, connector="  "))

        if not descendants:
            lines.append(
                f"  {_C.GRAY}(sem dependentes — este é um modelo folha){_C.RESET}"
            )
        else:
            for i, desc in enumerate(descendants):
                is_last = (i == len(descendants) - 1)
                connector = "  └── " if is_last else "  ├── "
                lines.append(self._format_node(desc, indent=0, connector=connector))

        lines.append("")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Renderização: grafo completo
    # ------------------------------------------------------------------

    def render_full(self) -> str:
        """
        Renderiza o grafo completo do projeto em camadas topológicas.

        Modelos no mesmo nível são exibidos lado a lado (mesmo nível de
        indentação). As setas indicam a direção do fluxo de dados.
        """
        if not self._nodes:
            return f"{_C.YELLOW}Nenhum modelo encontrado. Execute 'dfg compile' primeiro.{_C.RESET}"

        levels = self._topological_levels()
        total = sum(len(lvl) for lvl in levels)
        sep = "═" * 52

        lines: list[str] = []
        lines.append(
            f"\n{_C.BOLD}DataForge — Grafo de Linhagem Completo "
            f"({total} modelo(s)){_C.RESET}"
        )
        lines.append(f"{_C.GRAY}{sep}{_C.RESET}\n")

        for level_idx, level in enumerate(levels):
            if level_idx > 0:
                # Fluxo entre níveis
                lines.append(f"  {_C.GRAY}│{_C.RESET}")
                lines.append(f"  {_C.GRAY}▼{_C.RESET}")

            for name in level:
                info = self._nodes.get(name, {})
                model_type = info.get("type", "sql")
                materialized = info.get("materialized", "")
                mat_label = f"/{materialized}" if materialized and materialized != "memory" else ""
                color = _TYPE_COLORS.get(model_type, _C.RESET)
                symbol = _NODE_SYMBOLS.get(model_type, "○")

                deps = self._deps.get(name, [])
                dep_str = (
                    f"  {_C.GRAY}← {', '.join(deps)}{_C.RESET}" if deps else ""
                )
                lines.append(
                    f"  {color}{symbol}{_C.RESET} "
                    f"{_C.BOLD}{name:<30}{_C.RESET} "
                    f"{_C.DIM}[{model_type}{mat_label}]{_C.RESET}"
                    f"{dep_str}"
                )

        # Legenda
        lines.append(f"\n{_C.GRAY}{'─' * 52}")
        lines.append(
            f"  {_C.BLUE}● Python{_C.RESET} (ingestão)   "
            f"{_C.GREEN}○ SQL{_C.RESET} (transformação){_C.GRAY}"
        )
        lines.append(f"{'─' * 52}{_C.RESET}\n")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------

    @classmethod
    def from_engine(cls, engine, model: str | None = None) -> str:
        """
        Renderiza a linhagem carregando dados diretamente do engine.

        Método de conveniência que encapsula load + render.
        """
        renderer = cls(engine.project_dir)

        if not renderer._load_manifest():
            renderer._load_from_engine(engine)

        if model:
            return renderer.render_model(model)
        return renderer.render_full()