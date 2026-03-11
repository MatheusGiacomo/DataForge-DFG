# src/dfg/adapters/base.py
from abc import ABC, abstractmethod

class BaseAdapter(ABC):
    @abstractmethod
    def connect(self, config: dict):
        """Estabelece a conexão com o banco."""
        pass

    @abstractmethod
    def execute(self, sql: str):
        """Executa um comando SQL puro."""
        pass

    @abstractmethod
    def load_data(self, table_name: str, data: list, schema: str = "public"):
        """Carrega uma lista de dicionários para uma tabela."""
        pass