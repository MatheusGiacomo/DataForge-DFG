# src/dfg/adapters/generic.py
from dfg.adapters.base import BaseAdapter
from dfg.logger import logger

class GenericDBAPIAdapter(BaseAdapter):
    def __init__(self, driver_module):
        self.driver = driver_module
        self.conn = None
        # Mapeamento de tipos Python para SQL
        self.TYPE_MAP = {
            str: "TEXT",
            int: "INTEGER",
            float: "DOUBLE PRECISION",
            bool: "BOOLEAN"
        }

    def connect(self, config: dict):
        # 1. Filtramos as chaves internas do DFG
        internal_keys = ['type', 'schema']
        params = {k: v for k, v in config.items() if k not in internal_keys}
        
        try:
            # Especialização para DuckDB: ele é um banco de arquivo, 
            # não aceita host/port. Se for duckdb, pegamos apenas o 'database'.
            if "duckdb" in self.driver.__name__:
                db_path = params.get("database", ":memory:")
                self.conn = self.driver.connect(database=db_path)
            else:
                # Para Postgres/MySQL, passamos todos os parâmetros (host, port, user, etc)
                self.conn = self.driver.connect(**params)
            
            if hasattr(self.conn, 'autocommit'):
                self.conn.autocommit = True
                
            logger.info(f"Conexão estabelecida via {self.driver.__name__}")
        except Exception as e:
            logger.error(f"Falha na conexão: {e}")
            raise

    def execute(self, sql: str):
        """Implementação obrigatória do método abstrato."""
        with self.conn.cursor() as cur:
            cur.execute(sql)
            try:
                return cur.fetchall()
            except:
                # Caso o comando não retorne linhas (ex: CREATE, UPDATE)
                return None

    def _get_sql_type(self, value):
        return self.TYPE_MAP.get(type(value), "TEXT")

    def _get_columns_in_db(self, table_name):
        """Inspeciona as colunas existentes."""
        if "sqlite" in self.driver.__name__:
            cursor = self.conn.execute(f"PRAGMA table_info({table_name})")
            return [row[1] for row in cursor.fetchall()]
        
        # Consulta padrão Information Schema
        query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name.lower()}'
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            return [row[0] for row in cur.fetchall()]

    def sync_schema(self, table_name, data):
        """Lógica de Auto-migration / Schema Evolution."""
        if not data: return
        
        sample_row = data[0]
        try:
            existing_cols = self._get_columns_in_db(table_name)
        except Exception:
            existing_cols = []

        with self.conn.cursor() as cur:
            if not existing_cols:
                # Criar tabela nova
                cols_def = [f"{k} {self._get_sql_type(v)}" for k, v in sample_row.items()]
                cur.execute(f"CREATE TABLE {table_name} ({', '.join(cols_def)})")
                logger.info(f"Tabela '{table_name}' forjada do zero.")
            else:
                # Evoluir tabela existente
                for key, value in sample_row.items():
                    if key.lower() not in [c.lower() for c in existing_cols]:
                        logger.warn(f"Evoluindo schema: adicionando coluna '{key}' em '{table_name}'")
                        col_type = self._get_sql_type(value)
                        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {key} {col_type}")

    def load_data(self, table_name: str, data: list, schema: str = "public"):
        if not data: return
        
        self.sync_schema(table_name, data)
        
        cols = data[0].keys()
        # Identifica placeholder: ? (DuckDB/SQLite) ou %s (Postgres/MySQL)
        placeholder = "?" if "sqlite" in self.driver.__name__ or "duckdb" in self.driver.__name__ else "%s"
        
        query = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({', '.join([placeholder]*len(cols))})"
        values = [tuple(d.values()) for d in data]

        with self.conn.cursor() as cur:
            cur.executemany(query, values)
        
        # Garante o commit para drivers que não suportam autocommit nativo
        if not getattr(self.conn, 'autocommit', False):
            self.conn.commit()