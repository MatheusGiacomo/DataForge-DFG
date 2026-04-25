# src/dfg/dag.py
"""
Resolvedor de Grafo Acíclico Dirigido (DAG) do DataForge.

Utiliza o ``graphlib.TopologicalSorter`` da stdlib do Python 3.9+
para determinar a ordem de execução respeitando as dependências
entre modelos declaradas via {{ ref('nome') }}.
"""
from graphlib import CycleError, TopologicalSorter


class DAGResolver:
    """
    Resolve a ordem topológica de execução dos modelos.

    Parâmetros
    ----------
    models_metadata : dict
        Dicionário onde a chave é o nome do modelo e o valor é uma
        lista de nomes dos modelos dos quais ele depende.

        Exemplo::

            {
                'fct_sales': ['stg_users', 'stg_products'],
                'stg_users': [],
                'stg_products': [],
            }
    """

    def __init__(self, models_metadata: dict):
        self._metadata = models_metadata

    def get_execution_order(self) -> list[str]:
        """
        Retorna a lista de modelos na ordem correta de execução.

        Modelos sem dependências aparecem primeiro. Dentro de um mesmo
        "nível" de dependência, a ordem é determinística mas não garantida
        (pode variar entre versões do Python).

        Levanta
        -------
        Exception
            Se uma dependência circular for detectada (A depende de B
            que depende de A), com a descrição do ciclo.
        """
        try:
            sorter = TopologicalSorter(self._metadata)
            return list(sorter.static_order())
        except CycleError as e:
            raise RuntimeError(
                f"Dependência circular detectada no DAG: {e}. "
                f"Verifique as chamadas ref() nos seus modelos."
            ) from e