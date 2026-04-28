# src/dfg/snapshot.py
"""
Motor de Snapshots SCD Tipo 2 (Slowly Changing Dimensions) do DataForge.

Implementa o padrão de "dois caminhos":
- Carga Inicial: cria a tabela de histórico com colunas de controle.
- Merge (SCD2): invalida registros desatualizados e insere as novas versões.

Colunas de controle adicionadas automaticamente:
    dfg_valid_from  TIMESTAMP  — início do período de validade do registro
    dfg_valid_to    TIMESTAMP  — fim do período (NULL = registro atual)
    dfg_is_active   BOOLEAN    — TRUE para o registro mais recente de cada chave
"""
from datetime import UTC, datetime

from dfg.logging import logger


class SnapshotRunner:
    """
    Executa a lógica de SCD Tipo 2 para um snapshot específico.

    Parâmetros
    ----------
    engine : DFGEngine
        Instância do motor principal (para acesso à fábrica de adaptadores).
    """

    # Nome do alias interno usado nas sub-queries para evitar colisão
    _SOURCE_ALIAS = "__dfg_src"
    _TARGET_ALIAS = "__dfg_tgt"

    def __init__(self, engine):
        self.engine = engine
        # Carimbo de tempo UTC único para toda a execução deste runner
        self.current_timestamp: str = datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%S")

    # ------------------------------------------------------------------
    # Orquestrador principal
    # ------------------------------------------------------------------

    def run_snapshot(
        self,
        snapshot_name: str,
        parsed_config: dict,
        compiled_source_sql: str,
    ) -> bool:
        """
        Executa o snapshot no banco de dados.

        Parâmetros
        ----------
        snapshot_name : str
            Nome da tabela de histórico que será criada/atualizada.
        parsed_config : dict
            Configuração extraída pelo SQLCompiler (unique_key, updated_at, …).
        compiled_source_sql : str
            SQL da query SELECT já compilado (Jinja resolvido).

        Retorna
        -------
        bool: True em caso de sucesso, False em caso de erro.
        """
        unique_key = parsed_config.get("unique_key")
        updated_at = parsed_config.get("updated_at")

        if not unique_key:
            logger.error(
                f"Snapshot '{snapshot_name}': configuração 'unique_key' ausente. "
                f"Adicione {{ config(unique_key='id', updated_at='updated_at') }} ao snapshot."
            )
            return False

        if not updated_at:
            logger.error(
                f"Snapshot '{snapshot_name}': configuração 'updated_at' ausente. "
                f"Adicione {{ config(unique_key='id', updated_at='updated_at') }} ao snapshot."
            )
            return False

        logger.info(f"Iniciando snapshot '{snapshot_name}'...")
        adapter = self.engine._get_thread_safe_adapter()

        try:
            table_exists = adapter.check_table_exists(snapshot_name)

            if not table_exists:
                logger.info(
                    f"Tabela '{snapshot_name}' não encontrada. "
                    f"Executando carga inicial do snapshot."
                )
                self._run_initial_load(adapter, snapshot_name, compiled_source_sql, unique_key, updated_at)
            else:
                logger.info(
                    f"Tabela '{snapshot_name}' encontrada. "
                    f"Aplicando SCD Tipo 2 (Merge)."
                )
                self._run_scd2_merge(adapter, snapshot_name, compiled_source_sql, unique_key, updated_at)

            logger.success(f"Snapshot '{snapshot_name}' concluído com sucesso.")
            return True

        except Exception as e:
            logger.error(f"Erro ao executar snapshot '{snapshot_name}': {e}")
            return False
        finally:
            adapter.close()

    # ------------------------------------------------------------------
    # Carga Inicial
    # ------------------------------------------------------------------

    def _run_initial_load(
        self,
        adapter,
        target_table: str,
        source_sql: str,
        unique_key: str,
        updated_at: str,
    ) -> None:
        """
        Cria a tabela de histórico e insere todos os registros da fonte
        com colunas de controle SCD2 inicializadas.

        Estratégia:
            CREATE TABLE AS SELECT ... com colunas dfg_* adicionadas.
        """
        ts = self.current_timestamp
        sql = f"""
CREATE TABLE {target_table} AS
SELECT
    {self._SOURCE_ALIAS}.*,
    '{ts}'::TIMESTAMP  AS dfg_valid_from,
    NULL::TIMESTAMP    AS dfg_valid_to,
    TRUE               AS dfg_is_active,
    '{ts}'::TIMESTAMP  AS dfg_updated_at
FROM (
    {source_sql}
) AS {self._SOURCE_ALIAS}
""".strip()
        adapter.execute(sql)

    # ------------------------------------------------------------------
    # Merge SCD Tipo 2
    # ------------------------------------------------------------------

    def _run_scd2_merge(
        self,
        adapter,
        target_table: str,
        source_sql: str,
        unique_key: str,
        updated_at: str,
    ) -> None:
        """
        Aplica a lógica de SCD Tipo 2 em duas etapas atômicas:

        1. Invalida registros da tabela de histórico que sofreram
           alteração na fonte (fecha a janela de tempo).
        2. Insere as versões mais recentes como novos registros ativos.

        Compatível com DuckDB e PostgreSQL.
        """
        ts = self.current_timestamp
        tmp_table = f"{target_table}__dfg_scd2_tmp"

        # Passo 0: Cria tabela temporária com os dados atuais da fonte
        adapter.execute(f"DROP TABLE IF EXISTS {tmp_table}")
        adapter.execute(f"""
CREATE TABLE {tmp_table} AS
SELECT * FROM (
    {source_sql}
) AS {self._SOURCE_ALIAS}
""".strip())

        # Passo 1: Invalida registros desatualizados no histórico
        # (fecha dfg_valid_to e seta dfg_is_active = FALSE)
        update_sql = f"""
UPDATE {target_table} AS {self._TARGET_ALIAS}
SET
    dfg_valid_to    = '{ts}',
    dfg_is_active   = FALSE,
    dfg_updated_at  = '{ts}'
FROM {tmp_table} AS {self._SOURCE_ALIAS}
WHERE
    {self._TARGET_ALIAS}.{unique_key} = {self._SOURCE_ALIAS}.{unique_key}
    AND {self._TARGET_ALIAS}.dfg_is_active = TRUE
    AND {self._TARGET_ALIAS}.{updated_at} < {self._SOURCE_ALIAS}.{updated_at}
""".strip()
        adapter.execute(update_sql)

        # Passo 2: Insere as versões novas/alteradas como registros ativos
        insert_sql = f"""
INSERT INTO {target_table}
SELECT
    {self._SOURCE_ALIAS}.*,
    '{ts}'::TIMESTAMP AS dfg_valid_from,
    NULL::TIMESTAMP   AS dfg_valid_to,
    TRUE              AS dfg_is_active,
    '{ts}'::TIMESTAMP AS dfg_updated_at
FROM {tmp_table} AS {self._SOURCE_ALIAS}
WHERE NOT EXISTS (
    SELECT 1
    FROM {target_table} AS {self._TARGET_ALIAS}
    WHERE
        {self._TARGET_ALIAS}.{unique_key} = {self._SOURCE_ALIAS}.{unique_key}
        AND {self._TARGET_ALIAS}.dfg_is_active = TRUE
        AND {self._TARGET_ALIAS}.{updated_at} = {self._SOURCE_ALIAS}.{updated_at}
)
""".strip()
        adapter.execute(insert_sql)

        # Limpeza da tabela temporária
        adapter.execute(f"DROP TABLE IF EXISTS {tmp_table}")