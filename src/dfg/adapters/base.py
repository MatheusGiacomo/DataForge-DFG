# src/dfg/adapters/base.py
"""
Interface abstrata para todos os adaptadores de banco de dados do DataForge.

Todo novo adaptador deve herdar de BaseAdapter e implementar os
métodos abstratos abaixo para garantir compatibilidade com o motor.
"""
from abc import ABC, abstractmethod


class BaseAdapter(ABC):

    @abstractmethod
    def connect(self, config: dict) -> None:
        """Estabelece a conexão com o banco de dados a partir de um dicionário de configuração."""

    @abstractmethod
    def execute(self, sql: str) -> list | None:
        """
        Executa um comando SQL.

        Retorna uma lista de tuplas para consultas SELECT,
        ou None para comandos DDL/DML (CREATE, INSERT, UPDATE, DELETE).
        """

    @abstractmethod
    def load_data(self, table_name: str, data: list[dict], schema: str = "public") -> None:
        """
        Carrega uma lista de dicionários em uma tabela do banco.

        Realiza schema evolution automática se a tabela já existir.
        """

    @abstractmethod
    def close(self) -> None:
        """Fecha a conexão com o banco de dados e libera recursos."""

    def check_table_exists(self, table_name: str) -> bool:
        """
        Verifica se uma tabela existe no banco.

        Implementação padrão via SELECT com tratamento de exceção.
        Subclasses podem sobrescrever com queries mais eficientes.
        """
        try:
            self.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
            return True
        except Exception:
            return False
