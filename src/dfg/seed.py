# src/dfg/seed.py
"""
Motor de Seeds do DataForge.

Lê arquivos .csv da pasta seeds/ e os materializa como tabelas no banco
de dados usando uma estratégia Drop & Replace (idempotente).

Características:
- Inferência automática de tipos (int, float, str)
- Remoção de BOM (Byte Order Mark) de arquivos Excel via utf-8-sig
- Limpeza de espaços em branco nos valores
- Cada CSV vira uma tabela com o mesmo nome do arquivo (sem extensão)
"""
import csv
import os
import time

from dfg.logging import logger


class SeedRunner:
    """
    Carrega arquivos CSV estáticos no banco de dados.

    Parâmetros
    ----------
    engine : DFGEngine
        Instância do motor principal (para acesso ao adaptador e às configurações).
    """

    def __init__(self, engine):
        self.engine = engine
        self.seeds_dir = os.path.join(engine.project_dir, "seeds")

    # ------------------------------------------------------------------
    # Inferência de tipos
    # ------------------------------------------------------------------

    @staticmethod
    def _infer_type(value: str):
        """
        Converte strings do CSV para tipos Python nativos.

        Ordem de prioridade: int → float → str.
        Strings vazias são convertidas para None.
        """
        if value is None:
            return None

        stripped = value.strip()
        if not stripped:
            return None

        try:
            return int(stripped)
        except ValueError:
            pass

        try:
            return float(stripped)
        except ValueError:
            pass

        return stripped

    # ------------------------------------------------------------------
    # Execução principal
    # ------------------------------------------------------------------

    def run(self) -> None:
        """
        Itera sobre todos os arquivos .csv em seeds/ e os carrega no banco.

        Para cada arquivo:
        1. Lê e converte os tipos de dados
        2. Faz DROP TABLE IF EXISTS (com ou sem CASCADE dependendo do banco)
        3. Cria a tabela via load_data (que chama sync_schema internamente)
        """
        if not os.path.exists(self.seeds_dir):
            with self.engine.print_lock:
                logger.warn("Diretório 'seeds/' não encontrado. Operação ignorada.")
            return

        csv_files = sorted(f for f in os.listdir(self.seeds_dir) if f.endswith(".csv"))

        if not csv_files:
            with self.engine.print_lock:
                logger.info("Nenhum arquivo CSV encontrado na pasta 'seeds/'.")
            return

        start_time = time.time()
        target_name = self.engine.config["project"]["target"]
        target_schema = self.engine.config["targets"][target_name].get("schema", "public")

        adapter = self.engine._get_thread_safe_adapter()

        with self.engine.print_lock:
            logger.info(f"Iniciando plantio de {len(csv_files)} seed(s)...")

        success_count = 0
        try:
            for filename in csv_files:
                table_name = filename[:-4]  # Remove a extensão .csv
                filepath = os.path.join(self.seeds_dir, filename)

                with self.engine.print_lock:
                    logger.forge(f"Semeando '{table_name}'...")

                try:
                    # Lê o CSV convertendo tipos automaticamente
                    with open(filepath, encoding="utf-8-sig") as f:
                        reader = csv.DictReader(f)
                        data = [
                            {k.strip(): self._infer_type(v) for k, v in row.items()}
                            for row in reader
                        ]

                    if not data:
                        with self.engine.print_lock:
                            logger.warn(f"Arquivo '{filename}' está vazio. Pulando.")
                        continue

                    # Drop idempotente (tenta com CASCADE, cai para simples em caso de erro)
                    try:
                        adapter.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
                    except Exception:
                        adapter.execute(f"DROP TABLE IF EXISTS {table_name}")

                    adapter.load_data(table_name=table_name, data=data, schema=target_schema)

                    with self.engine.print_lock:
                        logger.success(
                            f"✓ Seed '{table_name}' plantada com sucesso ({len(data)} linha(s))."
                        )
                    success_count += 1

                except Exception as e:
                    with self.engine.print_lock:
                        logger.error(f"Erro ao processar seed '{filename}': {e}")

        finally:
            adapter.close()

        elapsed = time.time() - start_time
        with self.engine.print_lock:
            logger.info(
                f"Seeds: {success_count}/{len(csv_files)} carregada(s) "
                f"com sucesso em {elapsed:.3f}s."
            )