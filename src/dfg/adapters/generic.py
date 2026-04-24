# src/dfg/adapters/generic.py
"""
Adaptador genérico PEP 249 (DB-API 2.0) do DataForge.

Suporta qualquer driver que siga a especificação DB-API 2.0:
DuckDB, PostgreSQL (psycopg2), MySQL (mysql-connector-python), SQLite, etc.

Nota sobre gerenciamento de cursores:
    Nem todos os drivers suportam `with conn.cursor() as cur:`.
    SQLite (stdlib) e versões antigas do DuckDB não implementam
    o protocolo de context manager no cursor. Portanto, usamos
    o padrão try/finally para garantir o fechamento seguro em
    qualquer driver.
"""
from dfg.adapters.base import BaseAdapter
from dfg.logging import logger


class GenericDBAPIAdapter(BaseAdapter):
    """
    Adaptador universal para drivers DB-API 2.0.

    Inclui:
    - Detecção automática de placeholder (%s vs ?)
    - Schema evolution (auto-migration de colunas novas)
    - Suporte a DuckDB, PostgreSQL, MySQL e SQLite
    """

    # Mapeamento de tipos Python nativos para tipos SQL portáveis
    TYPE_MAP: dict = {
        str: "TEXT",
        int: "INTEGER",
        float: "DOUBLE PRECISION",
        bool: "BOOLEAN",
    }

    def __init__(self, driver_module):
        self.driver = driver_module
        self.conn = None

    # ------------------------------------------------------------------
    # Helpers internos
    # ------------------------------------------------------------------

    @property
    def _driver_name(self) -> str:
        return self.driver.__name__.lower()

    @property
    def _is_duckdb(self) -> bool:
        return "duckdb" in self._driver_name

    @property
    def _is_sqlite(self) -> bool:
        return "sqlite" in self._driver_name

    @property
    def _placeholder(self) -> str:
        """Placeholder para parâmetros de query (DB-API 2.0 paramstyle)."""
        return "?" if (self._is_sqlite or self._is_duckdb) else "%s"

    def _cursor(self):
        """Cria e retorna um novo cursor. Não usa context manager para compatibilidade universal."""
        return self.conn.cursor()

    def _get_sql_type(self, value) -> str:
        return self.TYPE_MAP.get(type(value), "TEXT")

    # ------------------------------------------------------------------
    # BaseAdapter — implementação
    # ------------------------------------------------------------------

    def connect(self, config: dict) -> None:
        """
        Conecta ao banco de dados.

        Para DuckDB: aceita apenas `database` (caminho do arquivo ou ':memory:').
        Para outros: repassa todos os parâmetros (exceto chaves internas do DFG).
        """
        internal_keys = {"type", "schema"}
        params = {k: v for k, v in config.items() if k not in internal_keys}

        try:
            if self._is_duckdb:
                db_path = params.get("database", ":memory:")
                self.conn = self.driver.connect(database=db_path)
            else:
                self.conn = self.driver.connect(**params)

            # Habilita autocommit quando o driver suporta (evita transações abertas)
            if hasattr(self.conn, "autocommit"):
                try:
                    self.conn.autocommit = True
                except Exception:
                    pass  # Alguns drivers não permitem setar após conexão

            logger.info(f"Conexão estabelecida via '{self._driver_name}'.")
        except Exception as e:
            logger.error(f"Falha ao conectar via '{self._driver_name}': {e}")
            raise

    def execute(self, sql: str) -> list | None:
        """Executa SQL e retorna resultados (SELECT) ou None (DDL/DML)."""
        if self.conn is None:
            raise RuntimeError("Adaptador não está conectado. Chame connect() primeiro.")

        cursor = self._cursor()
        try:
            cursor.execute(sql)
            try:
                return cursor.fetchall()
            except Exception:
                # Comandos DDL/DML (CREATE, UPDATE, DELETE, …) não retornam linhas
                return None
        finally:
            try:
                cursor.close()
            except Exception:
                pass

    def close(self) -> None:
        """Fecha a conexão e libera recursos."""
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
            finally:
                self.conn = None

    def check_table_exists(self, table_name: str) -> bool:
        """Verifica a existência de uma tabela de forma segura."""
        try:
            self.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Schema Evolution
    # ------------------------------------------------------------------

    def _get_columns_in_db(self, table_name: str) -> list[str]:
        """Retorna as colunas existentes na tabela do banco."""
        if self._is_sqlite:
            rows = self.execute(f"PRAGMA table_info({table_name})")
            return [row[1] for row in rows] if rows else []

        if self._is_duckdb:
            rows = self.execute(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_name = '{table_name.lower()}'"
            )
            return [row[0] for row in rows] if rows else []

        # PostgreSQL / MySQL
        rows = self.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name = '{table_name.lower()}'"
        )
        return [row[0] for row in rows] if rows else []

    def sync_schema(self, table_name: str, data: list[dict]) -> None:
        """
        Auto-migração de schema (schema evolution).

        - Se a tabela não existir: cria com os tipos inferidos do primeiro registro.
        - Se existir: adiciona colunas novas via ALTER TABLE.
        """
        if not data:
            return

        sample_row = data[0]

        try:
            existing_cols = self._get_columns_in_db(table_name)
        except Exception:
            existing_cols = []

        if not existing_cols:
            cols_def = ", ".join(
                f"{col} {self._get_sql_type(val)}" for col, val in sample_row.items()
            )
            self.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({cols_def})")
            logger.info(f"Tabela '{table_name}' criada.")
        else:
            existing_lower = {c.lower() for c in existing_cols}
            for col, val in sample_row.items():
                if col.lower() not in existing_lower:
                    col_type = self._get_sql_type(val)
                    logger.warn(f"Evoluindo schema: adicionando coluna '{col}' ({col_type}) em '{table_name}'.")
                    self.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} {col_type}")

    # ------------------------------------------------------------------
    # Carga de Dados
    # ------------------------------------------------------------------

    def load_data(self, table_name: str, data: list[dict], schema: str = "public") -> None:
        """
        Carrega dados em uma tabela via INSERT em lote (executemany).

        Realiza sync_schema antes da carga para garantir que todas
        as colunas necessárias existam.
        """
        if not data:
            return

        self.sync_schema(table_name, data)

        cols = list(data[0].keys())
        placeholders = ", ".join([self._placeholder] * len(cols))
        col_list = ", ".join(cols)
        query = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"
        values = [tuple(row.values()) for row in data]

        cursor = self._cursor()
        try:
            cursor.executemany(query, values)
            # Faz commit explícito para drivers sem autocommit
            if not getattr(self.conn, "autocommit", False):
                self.conn.commit()
        finally:
            try:
                cursor.close()
            except Exception:
                pass