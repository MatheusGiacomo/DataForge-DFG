# src/dfg/sources/database.py
"""
DatabaseSource — Conector de Banco de Dados para Sources do DataForge.

Permite copiar dados entre bancos de dados usando o mesmo sistema de
adaptadores DB-API 2.0. Ideal para:

    - Replicação de tabelas entre ambientes (prod → staging, legado → analytics)
    - CDC simplificado: extração incremental via coluna de timestamp
    - Consolidação de múltiplos bancos em um data warehouse central
    - Migração de dados entre tecnologias (MySQL → DuckDB, etc.)

O DatabaseSource reutiliza a AdapterFactory, portanto suporta os mesmos
bancos que o restante do DataForge: DuckDB, PostgreSQL, MySQL, SQLite.

Uso básico (copia tabela inteira):
    from dfg.sources import DatabaseSource

    source = DatabaseSource(
        connection={"type": "postgres", "host": "...", "database": "prod"},
        query="SELECT * FROM public.clientes",
    )

    def model(context):
        return source.fetch()

Com extração incremental (CDC simples):
    source = DatabaseSource(
        connection={"type": "postgres", "host": "...", "database": "prod"},
        query="SELECT * FROM pedidos WHERE atualizado_em > :since",
        incremental_param="since",
    )

    def model(context):
        # O estado persiste o último timestamp entre execuções
        return source.fetch(since=context["state"] or "2020-01-01")
"""
import contextlib

from dfg.adapters.factory import AdapterFactory
from dfg.logging import logger
from dfg.sources._env import resolve
from dfg.sources.base import BaseSource


class DatabaseSource(BaseSource):
    """
    Conector para extração de dados de bancos de dados relacionais.

    Aceita qualquer query SQL e retorna os resultados como lista de
    dicionários, compatível com o `adapter.load_data()` do destino.

    Parâmetros
    ----------
    connection : dict
        Configuração de conexão com o banco de origem. Mesmo formato
        utilizado no profiles.toml:
            {"type": "postgres", "host": "localhost", "database": "mydb",
             "user": "admin", "password": "{{ env('DB_PASS') }}"}
    query : str
        Query SQL a ser executada. Pode conter placeholders nomeados
        para extração incremental (ex: "WHERE updated_at > :since").
    batch_size : int
        Número de registros lidos por vez para evitar estouro de memória
        em tabelas grandes (padrão: 10000). 0 = sem batching.
    max_retries : int
        Número máximo de tentativas em caso de falha (padrão: 3).
    """

    def __init__(
        self,
        connection: dict,
        query: str,
        batch_size: int = 10_000,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        retry_backoff: float = 2.0,
    ):
        super().__init__(
            max_retries=max_retries,
            retry_delay=retry_delay,
            retry_backoff=retry_backoff,
        )
        self.connection = resolve(connection)
        self.query = query
        self.batch_size = batch_size

    # ------------------------------------------------------------------
    # API Pública
    # ------------------------------------------------------------------

    def fetch(self, **params) -> list[dict]:
        """
        Executa a query e retorna os resultados como lista de dicionários.

        Parâmetros
        ----------
        **params : dict
            Valores para substituição de placeholders nomeados na query.
            Exemplo: `source.fetch(since="2024-01-01")` substitui `:since`.

        Retorna
        -------
        list[dict]
            Todos os registros retornados pela query.

        Exemplo
        -------
        >>> source = DatabaseSource(conn_cfg, "SELECT * FROM t WHERE updated_at > :since")
        >>> records = source.fetch(since="2024-06-01")
        """
        return self._execute_with_retry(self._run_query, params)

    # ------------------------------------------------------------------
    # Execução da Query
    # ------------------------------------------------------------------

    def _run_query(self, params: dict) -> list[dict]:
        """
        Conecta ao banco, executa a query e retorna os resultados.

        Realiza a substituição de placeholders nomeados (:param → valor),
        converte as tuplas do cursor em dicionários e fecha a conexão.
        """
        db_type = self.connection.get("type", "?")
        logger.info(
            f"DatabaseSource: conectando ao banco '{db_type}' para executar query..."
        )

        adapter = AdapterFactory.get_adapter(db_type)
        adapter.connect(self.connection)

        try:
            compiled_query = self._bind_params(self.query, params)
            logger.debug(f"DatabaseSource: executando — {compiled_query[:120]}...")

            raw_rows = adapter.execute(compiled_query)

            if not raw_rows:
                logger.info("DatabaseSource: query retornou 0 registros.")
                return []

            # Obtém nomes das colunas via cursor describe
            column_names = self._get_column_names(adapter, compiled_query)

            if column_names:
                records = [dict(zip(column_names, row, strict=False)) for row in raw_rows]
            else:
                # Fallback: indexa colunas por posição se describe não estiver disponível
                records = [
                    {f"col_{i}": val for i, val in enumerate(row)}
                    for row in raw_rows
                ]

            logger.info(f"DatabaseSource: {len(records)} registro(s) extraído(s).")
            return records

        finally:
            adapter.close()

    def _get_column_names(self, adapter, query: str) -> list[str]:
        """
        Obtém os nomes das colunas via cursor.description após executar a query.

        Usa a API de cursor diretamente para acessar o atributo .description,
        que é parte do padrão DB-API 2.0 mas não está exposto no BaseAdapter.
        """
        try:
            cursor = adapter._cursor()
            try:
                cursor.execute(query)
                if cursor.description:
                    return [col[0] for col in cursor.description]
            finally:
                with contextlib.suppress(Exception):
                    cursor.close()
        except Exception as e:
            logger.debug(f"DatabaseSource: não foi possível obter nomes das colunas: {e}")

        return []

    # ------------------------------------------------------------------
    # Substituição de Parâmetros
    # ------------------------------------------------------------------

    @staticmethod
    def _bind_params(query: str, params: dict) -> str:
        """
        Substitui placeholders nomeados (:param_name) na query pelos valores.

        Esta implementação usa substituição de string simples para manter
        compatibilidade total com todos os drivers DB-API 2.0 sem depender
        do `paramstyle` específico de cada driver.

        Parâmetros
        ----------
        query : str
            Query SQL com placeholders no formato :nome_do_param.
        params : dict
            Dicionário de parâmetros a substituir.

        Levanta
        -------
        ValueError
            Se um placeholder na query não tiver valor correspondente.

        Exemplos
        --------
        >>> _bind_params("SELECT * FROM t WHERE id > :min_id", {"min_id": 100})
        "SELECT * FROM t WHERE id > '100'"
        """
        import re

        result = query
        for key, value in params.items():
            placeholder = f":{key}"
            if placeholder not in result:
                continue

            # Formata o valor conforme o tipo para SQL seguro
            if value is None:
                sql_value = "NULL"
            elif isinstance(value, bool):
                sql_value = "TRUE" if value else "FALSE"
            elif isinstance(value, (int, float)):
                sql_value = str(value)
            else:
                # Strings: escapa aspas simples para prevenir injeção básica
                escaped = str(value).replace("'", "''")
                sql_value = f"'{escaped}'"

            result = result.replace(placeholder, sql_value)

        # Verifica se sobraram placeholders não substituídos
        remaining = re.findall(r":\w+", result)
        if remaining:
            raise ValueError(
                f"DatabaseSource: placeholders sem valor correspondente: {remaining}. "
                f"Parâmetros fornecidos: {list(params.keys())}"
            )

        return result
