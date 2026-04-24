# src/dfg/snapshot.py
from datetime import datetime
from dfg.logging import logger

class SnapshotRunner:
    def __init__(self, engine):
        self.engine = engine
        self.current_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    def run_snapshot(self, snapshot_name, parsed_config, compiled_source_sql):
        """
        Orquestra a execução do snapshot no banco de dados.
        """
        target_table = snapshot_name
        unique_key = parsed_config.get('unique_key')
        updated_at = parsed_config.get('updated_at')

        if not unique_key or not updated_at:
            logger.error(f"Snapshot '{snapshot_name}' falhou: 'unique_key' ou 'updated_at' ausentes na config.")
            return False

        logger.info(f"Executando snapshot: {snapshot_name}...")
        
        # Verifica com o adapter se a tabela já existe
        table_exists = self.engine.adapter.check_table_exists(target_table)

        try:
            if not table_exists:
                logger.info(f"Tabela {target_table} não encontrada. Criando snapshot inicial.")
                sql = self._build_initial_snapshot_sql(target_table, compiled_source_sql, unique_key, updated_at)
            else:
                logger.info(f"Tabela {target_table} encontrada. Aplicando SCD Tipo 2 (Merge).")
                sql = self._build_scd2_merge_sql(target_table, compiled_source_sql, unique_key, updated_at)

            # Executa a query final no banco
            self.engine.adapter.execute(sql)
            logger.info(f"Snapshot {snapshot_name} concluído com sucesso.")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao executar snapshot {snapshot_name}: {e}")
            return False

    def _build_initial_snapshot_sql(self, target_table, source_sql, unique_key, updated_at):
        # ... (Insira aqui o código do _build_initial_snapshot_sql que enviei antes) ...
        pass

    def _build_scd2_merge_sql(self, target_table, source_sql, unique_key, updated_at):
        # ... (Insira aqui o código do _build_scd2_merge_sql que enviei antes - Item 3) ...
        pass