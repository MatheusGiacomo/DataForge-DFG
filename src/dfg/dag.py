# src/dfg/dag.py
from graphlib import TopologicalSorter, CycleError

class DAGResolver:
    def __init__(self, models_metadata: dict):
        """
        models_metadata: um dicionário onde a chave é o nome do modelo
        e o valor é uma lista de nomes de modelos dos quais ele depende.
        Ex: {'fct_sales': ['stg_users', 'stg_products']}
        """
        self.metadata = models_metadata
        self.sorter = TopologicalSorter(self.metadata)

    def get_execution_order(self):
        """Retorna uma lista com a ordem topológica de execução."""
        try:
            # O static_order() retorna um gerador com a ordem correta
            return list(self.sorter.static_order())
        except CycleError as e:
            # O Python detecta automaticamente se o usuário criou uma 
            # dependência circular (A depende de B, que depende de A)
            raise Exception(f"Erro de Dependência Circular detectado: {e}")