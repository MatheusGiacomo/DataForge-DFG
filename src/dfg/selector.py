# src/dfg/selector.py
"""
Motor de Seleção do DAG do DataForge (v0.3.0).

Implementa a sintaxe de seleção inspirada no dbt para o comando
`dfg run --select`, permitindo execução parcial e direcionada do pipeline.

Sintaxe suportada:
    model_name          — modelo exato
    +model_name         — modelo + todos os seus ancestrais (dependências)
    model_name+         — modelo + todos os seus descendentes
    +model_name+        — modelo + ancestrais + descendentes
    tag:nome_da_tag     — todos os modelos com a tag especificada

Múltiplos seletores são separados por espaço (união):
    dfg run --select stg_pedidos fct_resumo+
    dfg run --select +fct_clientes tag:staging

Casos de uso:
    # Rodar apenas um modelo específico
    dfg run --select stg_clientes

    # Rodar um modelo e tudo que depende dele
    dfg run --select stg_pedidos+

    # Rodar um modelo e todas as suas dependências
    dfg run --select +fct_kpis_diarios

    # Rodar todos os modelos com a tag 'staging'
    dfg run --select tag:staging

    # Combinar seletores
    dfg run --select +fct_vendas tag:seed
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field


# =============================================================================
# Representação de um seletor individual
# =============================================================================


@dataclass
class Selector:
    """
    Representa um único token de seleção após parsing.

    Atributos
    ----------
    raw : str
        String original do seletor (ex: "+fct_vendas+", "tag:staging").
    kind : str
        Tipo do seletor: "model" ou "tag".
    name : str
        Nome do modelo ou da tag (sem os prefixos/sufixos de sintaxe).
    include_ancestors : bool
        True se o seletor começa com "+".
    include_descendants : bool
        True se o seletor termina com "+".
    """

    raw: str
    kind: str            # "model" | "tag"
    name: str
    include_ancestors: bool = False
    include_descendants: bool = False


# =============================================================================
# Parser de expressões de seleção
# =============================================================================


class SelectorParser:
    """
    Converte uma lista de strings de seleção em objetos `Selector`.

    Exemplos
    --------
    >>> SelectorParser.parse(["+fct_vendas", "tag:staging"])
    [Selector(kind='model', name='fct_vendas', include_ancestors=True, ...),
     Selector(kind='tag', name='staging', ...)]
    """

    _TAG_PATTERN = re.compile(r"^tag:(\w+)$")
    _MODEL_PATTERN = re.compile(r"^(\+?)(\w+)(\+?)$")

    @classmethod
    def parse(cls, selectors: list[str]) -> list[Selector]:
        """
        Faz o parsing de uma lista de strings de seleção.

        Parâmetros
        ----------
        selectors : list[str]
            Lista de strings de seleção (ex: ["+fct_vendas+", "tag:staging"]).

        Retorna
        -------
        list[Selector]
            Lista de objetos Selector prontos para resolução.

        Levanta
        -------
        ValueError
            Se um token de seleção não puder ser interpretado.
        """
        result = []
        for raw in selectors:
            token = raw.strip()
            if not token:
                continue

            # Tag selector: tag:nome
            tag_match = cls._TAG_PATTERN.match(token)
            if tag_match:
                result.append(Selector(
                    raw=raw,
                    kind="tag",
                    name=tag_match.group(1),
                ))
                continue

            # Model selector: [+]nome[+]
            model_match = cls._MODEL_PATTERN.match(token)
            if model_match:
                prefix, name, suffix = model_match.groups()
                result.append(Selector(
                    raw=raw,
                    kind="model",
                    name=name,
                    include_ancestors=bool(prefix),
                    include_descendants=bool(suffix),
                ))
                continue

            raise ValueError(
                f"Seletor inválido: '{raw}'. "
                f"Formatos válidos: 'modelo', '+modelo', 'modelo+', '+modelo+', 'tag:nome'."
            )

        return result


# =============================================================================
# Resolvedor de seletores contra o DAG
# =============================================================================


class SelectorResolver:
    """
    Resolve seletores contra o grafo de dependências do projeto.

    Recebe o `dependencies_map` (nome → lista de dependências) e o
    `models_registry` (nome → info) e produz o conjunto de modelos
    que devem ser executados.

    Parâmetros
    ----------
    dependencies_map : dict[str, list[str]]
        Mapa de dependências do DAG (upstream).
        Ex: {"fct_vendas": ["stg_pedidos", "stg_clientes"]}
    models_registry : dict[str, dict]
        Registry completo de modelos com suas configurações.
    """

    def __init__(
        self,
        dependencies_map: dict[str, list[str]],
        models_registry: dict[str, dict],
    ):
        self._deps = dependencies_map
        self._registry = models_registry
        # Constrói o mapa reverso (downstream) uma única vez
        self._reverse_deps: dict[str, set[str]] = self._build_reverse_deps()

    def _build_reverse_deps(self) -> dict[str, set[str]]:
        """Constrói o mapa de dependências reversas (modelo → quem depende dele)."""
        reverse: dict[str, set[str]] = {name: set() for name in self._deps}
        for model, deps in self._deps.items():
            for dep in deps:
                if dep in reverse:
                    reverse[dep].add(model)
        return reverse

    def resolve(self, selectors: list[Selector]) -> set[str]:
        """
        Resolve uma lista de seletores e retorna o conjunto de modelos selecionados.

        Múltiplos seletores são combinados via UNIÃO — um modelo é incluído
        se satisfizer QUALQUER um dos seletores.

        Parâmetros
        ----------
        selectors : list[Selector]
            Seletores já parseados.

        Retorna
        -------
        set[str]
            Conjunto com os nomes de todos os modelos selecionados.

        Levanta
        -------
        ValueError
            Se um modelo ou tag referenciado não existir no projeto.
        """
        selected: set[str] = set()

        for selector in selectors:
            if selector.kind == "tag":
                nodes = self._resolve_tag(selector.name)
            else:
                nodes = self._resolve_model(selector)

            selected |= nodes

        return selected

    def _resolve_model(self, selector: Selector) -> set[str]:
        """Resolve um seletor de modelo, incluindo ancestrais e/ou descendentes."""
        name = selector.name

        if name not in self._registry:
            available = ", ".join(sorted(self._registry.keys()))
            raise ValueError(
                f"Modelo '{name}' não encontrado no projeto. "
                f"Modelos disponíveis: {available}"
            )

        nodes: set[str] = {name}

        if selector.include_ancestors:
            nodes |= self._get_ancestors(name)

        if selector.include_descendants:
            nodes |= self._get_descendants(name)

        return nodes

    def _resolve_tag(self, tag: str) -> set[str]:
        """Resolve todos os modelos que possuem uma determinada tag."""
        tagged = {
            name
            for name, info in self._registry.items()
            if tag in info.get("config", {}).get("tags", [])
        }

        if not tagged:
            from dfg.logging import logger
            logger.warn(
                f"Nenhum modelo encontrado com a tag '{tag}'. "
                f"Verifique se a tag está definida no schema.yml."
            )

        return tagged

    def _get_ancestors(self, model: str) -> set[str]:
        """
        Retorna todos os ancestrais de um modelo (suas dependências transitivas).

        Usa BFS (Breadth-First Search) para garantir que todos os níveis
        de dependência sejam cobertos.
        """
        ancestors: set[str] = set()
        queue = list(self._deps.get(model, []))

        while queue:
            dep = queue.pop(0)
            if dep not in ancestors and dep in self._registry:
                ancestors.add(dep)
                queue.extend(self._deps.get(dep, []))

        return ancestors

    def _get_descendants(self, model: str) -> set[str]:
        """
        Retorna todos os descendentes de um modelo (quem depende dele transitivamente).

        Usa BFS no grafo reverso de dependências.
        """
        descendants: set[str] = set()
        queue = list(self._reverse_deps.get(model, set()))

        while queue:
            dep = queue.pop(0)
            if dep not in descendants and dep in self._registry:
                descendants.add(dep)
                queue.extend(self._reverse_deps.get(dep, set()))

        return descendants


# =============================================================================
# API de alto nível
# =============================================================================


def resolve_selection(
    select_args: list[str] | None,
    dependencies_map: dict[str, list[str]],
    models_registry: dict[str, dict],
) -> set[str] | None:
    """
    Função de conveniência que faz parsing e resolução em uma única chamada.

    Parâmetros
    ----------
    select_args : list[str] | None
        Lista de strings de seleção do CLI. None = selecionar todos.
    dependencies_map : dict
        Mapa de dependências do DAG.
    models_registry : dict
        Registry completo de modelos.

    Retorna
    -------
    set[str] | None
        Conjunto de modelos selecionados, ou None se nenhuma seleção foi feita.
    """
    if not select_args:
        return None

    selectors = SelectorParser.parse(select_args)
    resolver = SelectorResolver(dependencies_map, models_registry)
    return resolver.resolve(selectors)