# src/dfg/engine.py
import tomllib
import importlib.util
import os
import sys
import yaml
import time
import threading
import graphlib
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

from dfg.adapters.factory import AdapterFactory
from dfg.logging import logger
from dfg.state import StateManager 
from dfg.artifacts import ArtifactManager
from dfg.compiler import SQLCompiler  # <-- Nosso novo compilador Jinja
from dfg.snapshot import SnapshotRunner

class DFGEngine:
    def __init__(self, project_dir: str):
        # 1. Caminhos Fundamentais
        self.project_dir = project_dir
        self.models_dir = os.path.join(self.project_dir, "models")
        self.snapshots_dir = os.path.join(self.project_dir, "snapshots")
        self.seeds_dir = os.path.join(self.project_dir, "seeds")
        
        # 2. Inicialização de Core Services
        logger.setup(self.project_dir)
        self.artifact_manager = ArtifactManager(self.project_dir)
        self.state_manager = StateManager(self.project_dir)
        self.config = self._load_config()
        
        # 3. Motores de Tradução e Execução
        # Centralizamos o compilador aqui para que todos os comandos usem o mesmo motor Jinja
        self.compiler = SQLCompiler() 
        self.snapshot_runner = SnapshotRunner(self)
        
        # 4. Registros de Estado do DAG
        self.models_registry = {}
        self.dependencies_map = {}
        
        # 5. Concorrência e Thread-Safety
        # Locks para garantir integridade no console e no cache de dados
        self.print_lock = threading.Lock()
        self.cache_lock = threading.Lock()

        # 6. Adaptador de Banco de Dados (Opcional: carregar aqui ou sob demanda)
        # self.adapter = self._init_adapter()
        
    def snapshots(self):
        """
        Orquestrador principal para processar todos os arquivos de snapshot.
        """
        logger.info("Iniciando processamento de Snapshots (SCD Type 2)...")
        
        # 1. Verificar se a pasta de snapshots existe
        if not os.path.exists(self.snapshots_dir):
            logger.warning(f"Diretório de snapshots não encontrado em: {self.snapshots_dir}")
            return

        # 2. Instanciar o executor de snapshots
        runner = SnapshotRunner(self)
        
        # 3. Listar todos os arquivos .sql na pasta snapshots/
        snapshot_files = [f for f in os.listdir(self.snapshots_dir) if f.endswith(".sql")]
        
        if not snapshot_files:
            logger.info("Nenhum arquivo de snapshot encontrado para processar.")
            return

        success_count = 0
        for file_name in snapshot_files:
            file_path = os.path.join(self.snapshots_dir, file_name)
            
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    raw_sql = f.read()

                # 4. Parsing: O Compilador extrai config e limpa o SQL
                snapshot_data = self.compiler.parse_snapshot(raw_sql)
                
                if not snapshot_data:
                    logger.error(f"Arquivo {file_name} não possui um bloco '{{% snapshot %}}' válido.")
                    continue

                # 5. Execução: O Runner aplica a lógica de SCD2 no banco
                success = runner.run_snapshot(
                    snapshot_name=snapshot_data["snapshot_name"],
                    parsed_config=snapshot_data["config"],
                    compiled_source_sql=snapshot_data["compiled_sql"]
                )

                if success:
                    success_count += 1

            except Exception as e:
                logger.error(f"Falha crítica ao processar snapshot {file_name}: {e}")

        logger.info(f"Processamento concluído: {success_count}/{len(snapshot_files)} snapshots executados.")

    def _load_config(self) -> dict:
        project_toml_path = os.path.join(self.project_dir, "dfg_project.toml")
        profiles_toml_path = os.path.join(self.project_dir, "profiles.toml")
        
        if not os.path.exists(project_toml_path):
            raise FileNotFoundError("Arquivo dfg_project.toml não encontrado.")
            
        with open(project_toml_path, "rb") as f: config = tomllib.load(f)
        with open(profiles_toml_path, "rb") as f: profiles = tomllib.load(f)
            
        profile_name = config["project"]["profile"]
        target_name = config["project"]["target"]
        
        try:
            credentials = profiles[profile_name]["outputs"][target_name]
            config["targets"] = {target_name: credentials}
        except KeyError:
            raise ValueError(f"Target '{target_name}' não encontrado no profile '{profile_name}'")
            
        return config

    def _get_thread_safe_adapter(self):
        """
        SÊNIOR: Cada thread precisa de sua própria conexão para evitar 
        colisão de cursores no banco de dados durante execução paralela.
        """
        target_name = self.config["project"]["target"]
        target_config = self.config["targets"][target_name]
        
        adapter = AdapterFactory.get_adapter(target_config["type"])
        adapter.connect(target_config)
        return adapter

    def discover_models(self):
        """
        SÊNIOR: Descoberta em duas fases.
        Fase 1: Mapeia arquivos de execução (.sql, .py) e compila templates Jinja.
        Fase 2: Enriquece com metadados e contratos (.yml).
        """
        if self.models_registry: return 

        with self.print_lock:
            logger.forge("Escaneando modelos e metadados...")
            
        # Instancia o compilador Jinja com o schema alvo do projeto
        target_schema = self.config["targets"][self.config["project"]["target"]].get("schema", "public")
        jinja_compiler = SQLCompiler(target_schema)
        
        # --- FASE 1: Identificação de Executáveis (.py e .sql) ---
        for filename in os.listdir(self.models_dir):
            if filename == "__init__.py": continue
            filepath = os.path.join(self.models_dir, filename)
            
            # Caso 1: Modelos em Python (Ingestão)
            if filename.endswith(".py"):
                model_name = filename[:-3]
                
                spec = importlib.util.spec_from_file_location(model_name, filepath)
                module = importlib.util.module_from_spec(spec)
                sys.modules[model_name] = module 
                spec.loader.exec_module(module)
                
                self.models_registry[model_name] = {
                    "type": "python", 
                    "func": module.model, 
                    "config": {"contract": getattr(module, 'CONTRACT', {})} 
                }
                self.dependencies_map[model_name] = getattr(module, 'DEPENDENCIES', [])

            # Caso 2: Modelos em SQL (Transformação)
            elif filename.endswith(".sql"):
                model_name = filename[:-4]
                with open(filepath, "r", encoding="utf-8") as f: raw_sql = f.read()
                
                # SÊNIOR: Compilação Jinja substitui a Regex antiga!
                try:
                    compilation = jinja_compiler.compile(raw_sql, model_name)
                except Exception as e:
                    with self.print_lock:
                        logger.error(f"Falha na compilação do modelo '{model_name}': {e}")
                    continue # Pula este modelo em caso de erro de sintaxe
                
                # Garante valores padrões no config extraído
                model_config = compilation["config"]
                if "materialized" not in model_config:
                    model_config["materialized"] = "table"
                if "contract" not in model_config:
                    model_config["contract"] = {}

                self.models_registry[model_name] = {
                    "type": "sql", 
                    "raw": raw_sql,
                    "compiled": compilation["sql"], # O SQL pronto para rodar
                    "config": model_config 
                }
                self.dependencies_map[model_name] = compilation["depends_on"]

        # --- FASE 2: Enriquecimento via YAML (Metadados e Testes) ---
        for filename in os.listdir(self.models_dir):
            if filename.endswith((".yml", ".yaml")):
                yaml_path = os.path.join(self.models_dir, filename)
                with open(yaml_path, "r", encoding="utf-8") as f:
                    try:
                        metadata = yaml.safe_load(f)
                        if not metadata or "models" not in metadata: continue
                        
                        for m_meta in metadata["models"]:
                            name = m_meta.get("name")
                            if name in self.models_registry:
                                self.models_registry[name]["config"]["description"] = m_meta.get("description", "")
                                
                                if "columns" in m_meta:
                                    contract = {}
                                    for col in m_meta["columns"]:
                                        col_name = col.get("name")
                                        if "tests" in col:
                                            contract[col_name] = col["tests"]
                                    
                                    self.models_registry[name]["config"]["contract"] = contract
                                    
                        with self.print_lock:
                            logger.success(f"Metadados carregados de: {filename}")
                    except Exception as e:
                        with self.print_lock:
                            logger.error(f"Erro ao processar arquivo YAML {filename}: {e}")

        with self.print_lock:
            logger.info(f"DAG carregado: {len(self.models_registry)} modelos identificados.")

    def _execute_node(self, model_name, filter_type, context_cache):
        """Executa um modelo individualmente de forma isolada (Worker)"""
        model_info = self.models_registry[model_name]
        
        if filter_type and model_info["type"] != filter_type:
            return {"model": model_name, "status": "skipped", "execution_time": 0}

        start_model = time.time()
        adapter = self._get_thread_safe_adapter()
        rows_affected = 0
        
        try:
            # --- EXECUÇÃO SQL ---
            if model_info["type"] == "sql":
                mat_type = model_info["config"].get("materialized", "table").upper()
                unique_key = model_info["config"].get("unique_key")
                
                # Pega o SQL do Jinja, já processado e limpo!
                compiled_sql = model_info["compiled"]
                
                if mat_type == "INCREMENTAL":
                    with self.print_lock: logger.forge(f"Processando [INCREMENTAL] '{model_name}'...")
                    
                    tmp_table = f"{model_name}__dfg_tmp"
                    adapter.execute(f"DROP TABLE IF EXISTS {tmp_table} CASCADE;")
                    adapter.execute(f"CREATE TABLE {tmp_table} AS \n{compiled_sql}")
                    
                    try:
                        adapter.execute(f"SELECT 1 FROM {model_name} LIMIT 1")
                        table_exists = True
                    except Exception:
                        table_exists = False
                        
                    if not table_exists:
                        adapter.execute(f"CREATE TABLE {model_name} AS SELECT * FROM {tmp_table}")
                    else:
                        if unique_key:
                            adapter.execute(f"DELETE FROM {model_name} WHERE {unique_key} IN (SELECT {unique_key} FROM {tmp_table})")
                        adapter.execute(f"INSERT INTO {model_name} SELECT * FROM {tmp_table}")
                        
                    adapter.execute(f"DROP TABLE IF EXISTS {tmp_table} CASCADE;")
                    
                else:
                    with self.print_lock: logger.forge(f"Materializando [{mat_type}] '{model_name}'...")
                    adapter.execute(f"DROP VIEW IF EXISTS {model_name} CASCADE;")
                    adapter.execute(f"DROP TABLE IF EXISTS {model_name} CASCADE;")
                    adapter.execute(f"CREATE {mat_type} {model_name} AS \n{compiled_sql}")
                    
            # --- EXECUÇÃO PYTHON ---
            else:
                with self.print_lock: logger.forge(f"Extraindo/Ingerindo [PYTHON] '{model_name}'...")
                
                context = {
                    "config": self.config,
                    "ref": lambda name: context_cache.get(name),
                    "state": self.state_manager.get(model_name),
                    "set_state": lambda val, m=model_name: self.state_manager.set(m, val)
                }
                
                data = model_info["func"](context)
                
                with self.cache_lock:
                    context_cache[model_name] = data
                
                if data:
                    rows_affected = len(data)
                    target_schema = self.config["targets"][self.config["project"]["target"]].get("schema", "public")
                    adapter.load_data(table_name=model_name, data=data, schema=target_schema)

            execution_time = round(time.time() - start_model, 3)
            with self.print_lock: logger.success(f"✓ '{model_name}' concluído ({execution_time}s).")
            
            if hasattr(adapter, 'close'): adapter.close()
            
            return {"model": model_name, "status": "success", "execution_time": execution_time, "rows": rows_affected}
            
        except Exception as e:
            with self.print_lock: logger.error(f"✗ Erro crítico em '{model_name}': {e}")
            if hasattr(adapter, 'close'): adapter.close()
            return {"model": model_name, "status": "error", "execution_time": round(time.time() - start_model, 3), "error": str(e)}

    def _execute_dag(self, filter_type=None, command_name="run"):
        self.discover_models()
        self.artifact_manager.save_manifest(self.models_registry, self.dependencies_map)
        
        max_workers = self.config.get("project", {}).get("threads", 4)
        ts = graphlib.TopologicalSorter(self.dependencies_map)
        ts.prepare()
        
        run_results_log = []
        context_cache = {}
        has_errors = False
        success_count = 0

        with self.print_lock:
            logger.info(f"Iniciando pool de execução ({max_workers} threads alocadas).")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures_map = {}
            
            while ts.is_active():
                ready_nodes = ts.get_ready()
                
                for node in ready_nodes:
                    fut = executor.submit(self._execute_node, node, filter_type, context_cache)
                    futures_map[fut] = node
                    
                if not futures_map:
                    break 
                    
                done, _ = wait(futures_map.keys(), return_when=FIRST_COMPLETED)
                
                for fut in done:
                    node = futures_map.pop(fut)
                    try:
                        result = fut.result()
                        run_results_log.append(result)
                        
                        if result["status"] in ["success", "skipped"]:
                            if result["status"] == "success": success_count += 1
                            ts.done(node) 
                        else:
                            has_errors = True
                    except Exception as e:
                        has_errors = True
                        with self.print_lock: logger.error(f"Erro na thread do modelo {node}: {e}")

        self.artifact_manager.save_run_results(command_name, run_results_log)
        if has_errors: return False
        return True if success_count > 0 else "no_work"

    def ingest(self): return self._execute_dag(filter_type="python", command_name="ingest")
    def transform(self): return self._execute_dag(filter_type="sql", command_name="transform")

    def run(self):
        start_run = time.time()
        with self.print_lock: logger.info(f"Iniciando Pipeline no ambiente: {self.config['project']['target'].upper()}")
        
        result = self._execute_dag(filter_type=None, command_name="run")
        
        if result is True:
            with self.print_lock: logger.success(f"--- Forja finalizada com sucesso em {(time.time() - start_run):.3f}s! ---")
        return result

    def test(self):
        """Executa os contratos de dados"""
        from dfg.logging import logger
        import sys
        
        adapter = self._get_thread_safe_adapter()
        self.discover_models()
        logger.info("\n--- Iniciando Validação de Contratos ---")
        erros = 0
        
        try:
            for model_name, model_info in self.models_registry.items():
                contract = None
                
                if model_info["type"] == "python":
                    module = sys.modules.get(model_name)
                    contract = getattr(module, 'CONTRACT', None) if module else None
                elif model_info["type"] == "sql":
                    contract = model_info.get("config", {}).get("contract", None)
                
                if not contract:
                    logger.warn(f"Modelo '{model_name}': Sem contrato definido. Pulando.")
                    continue
                    
                logger.forge(f"Testando '{model_name}'...")
                try:
                    res = adapter.execute(f"SELECT COUNT(*) FROM {model_name}")
                    if not res or res[0][0] == 0: 
                        logger.warn(f"  [AVISO] Tabela '{model_name}' está vazia.")
                    
                    for coluna, testes in contract.items():
                        for teste in testes:
                            if teste == "not_null":
                                nulos = adapter.execute(f"SELECT COUNT(*) FROM {model_name} WHERE {coluna} IS NULL")[0][0]
                                if nulos > 0:
                                    logger.error(f"  [FALHA] {model_name}.{coluna}: {nulos} registros nulos!")
                                    erros += 1
                            elif teste == "unique":
                                dups = adapter.execute(f"SELECT COUNT(*) FROM (SELECT {coluna} FROM {model_name} GROUP BY {coluna} HAVING COUNT(*) > 1) AS s")[0][0]
                                if dups > 0:
                                    logger.error(f"  [FALHA] {model_name}.{coluna}: Possui duplicatas!")
                                    erros += 1
                except Exception as e:
                    logger.error(f" Erro ao executar teste no banco para {model_name}: {e}")
                    erros += 1

        finally:
            if hasattr(adapter, 'close'):
                adapter.close()

        if erros > 0: 
            logger.error(f"Falha na validação: {erros} erro(s) encontrados.")
            sys.exit(1)
            
        logger.success("Todos os contratos validados com sucesso!")

    def compile(self):
        """Gera SQLs compilados e o manifest.json"""
        from dfg.logging import logger
        import os
        
        logger.info("Compilando modelos e gerando manifest.json...")
        self.discover_models()
        
        self.artifact_manager.save_manifest(self.models_registry, self.dependencies_map)
        
        compiled_dir = os.path.join(self.project_dir, "target", "compiled")
        os.makedirs(compiled_dir, exist_ok=True)
        
        for name, info in self.models_registry.items():
            if info["type"] == "sql":
                mat_type = info.get("config", {}).get("materialized", "table").upper()
                
                # Pegamos o SQL já compilado pelo Jinja na fase de descoberta
                compiled_sql = info["compiled"]
                
                with open(os.path.join(compiled_dir, f"{name}.sql"), "w", encoding="utf-8") as f:
                    f.write(f"-- Materialização: {mat_type}\n{compiled_sql}")
                logger.success(f"Compilado: {name}.sql")