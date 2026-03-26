# src/dfg/seed.py
import os
import csv
import time
from dfg.logging import logger

class SeedRunner:
    def __init__(self, engine):
        """
        Recebe a instância do DFGEngine para herdar as configurações 
        de diretório, perfis e o adaptador do banco de dados.
        """
        self.engine = engine
        self.project_dir = engine.project_dir
        self.seeds_dir = os.path.join(self.project_dir, "seeds")

    def _infer_type(self, value: str):
        """Tenta inferir o tipo do dado vindo do CSV."""
        if not value or value.strip() == "":
            return None
            
        value = value.strip()
        try: return int(value)
        except ValueError: pass
            
        try: return float(value)
        except ValueError: pass
            
        return value

    def run(self):
        """
        Lê arquivos .csv e os materializa no banco (Drop and Replace).
        """
        if not os.path.exists(self.seeds_dir):
            with self.engine.print_lock:
                logger.warn("Diretório 'seeds' não encontrado. Operação ignorada.")
            return

        csv_files = [f for f in os.listdir(self.seeds_dir) if f.endswith('.csv')]
        if not csv_files:
            with self.engine.print_lock:
                logger.info("Nenhum arquivo CSV encontrado na pasta 'seeds'.")
            return

        start_time = time.time()
        
        # Reaproveita o adaptador seguro do motor principal
        adapter = self.engine._get_thread_safe_adapter()
        target_schema = self.engine.config["targets"][self.engine.config["project"]["target"]].get("schema", "public")
        
        with self.engine.print_lock:
            logger.info(f"Iniciando plantio de {len(csv_files)} seed(s)...")

        try:
            for filename in csv_files:
                table_name = filename[:-4]
                filepath = os.path.join(self.seeds_dir, filename)
                
                with self.engine.print_lock:
                    logger.forge(f"Semeando [{table_name}]...")
                
                with open(filepath, mode='r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    data = [{k: self._infer_type(v) for k, v in row.items()} for row in reader]

                if not data:
                    with self.engine.print_lock:
                        logger.warn(f" [AVISO] O arquivo {filename} está vazio. Pulando.")
                    continue

                try:
                    adapter.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
                except Exception:
                    adapter.execute(f"DROP TABLE IF EXISTS {table_name};")
                
                adapter.load_data(table_name=table_name, data=data, schema=target_schema)

                with self.engine.print_lock:
                    logger.success(f" ✓ Seed '{table_name}' plantada com sucesso ({len(data)} linhas).")

        except Exception as e:
            with self.engine.print_lock:
                logger.error(f"Erro fatal durante o carregamento da seed: {e}")
        finally:
            if hasattr(adapter, 'close'): 
                adapter.close()

        with self.engine.print_lock:
            logger.info(f"Processamento de seeds concluído em {(time.time() - start_time):.3f}s.")