# src/dfg/engine.py
import tomllib
import importlib.util
import os
import sys
import re
import yaml
import time
import threading
import graphlib
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

from dfg.adapters.factory import AdapterFactory
from dfg.logging import logger
from dfg.state import StateManager 
from dfg.artifacts import ArtifactManager

# Regex refatorado para capturar múltiplos argumentos no config
CONFIG_BLOCK_PATTERN = re.compile(r"\{\{\s*config\((.*?)\)\s*\}\}", re.IGNORECASE)
KWARG_PATTERN = re.compile(r"(\w+)\s*=\s*['\"]([^'\"]+)['\"]")
REF_PATTERN = re.compile(r"\{\{\s*ref\('([^']+)'\)\s*\}\}")

class DFGEngine:
    def __init__(self, project_dir: str):
        self.project_dir = project_dir
        
        logger.setup(self.project_dir)
        self.artifact_manager = ArtifactManager(self.project_dir)
        
        self.config = self._load_config()
        self.models_dir = os.path.join(self.project_dir, "models")
        self.models_registry = {}
        self.dependencies_map = {}
        self.state_manager = StateManager(self.project_dir)
        
        # Locks para garantir thread-safety no console e no cache de dados
        self.print_lock = threading.Lock()
        self.cache_lock = threading.Lock()

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
        Fase 1: Mapeia arquivos de execução (.sql, .py).
        Fase 2: Enriquece com metadados e contratos (.yml).
        """
        if self.models_registry: return 

        with self.print_lock:
            logger.forge("Escaneando modelos e metadados...")
        
        # --- FASE 1: Identificação de Executáveis (.py e .sql) ---
        for filename in os.listdir(self.models_dir):
            if filename == "__init__.py": continue
            filepath = os.path.join(self.models_dir, filename)
            
            # Caso 1: Modelos em Python (Ingestão)
            if filename.endswith(".py"):
                model_name = filename[:-3]
                
                # Lógica de Importação Dinâmica
                spec = importlib.util.spec_from_file_location(model_name, filepath)
                module = importlib.util.module_from_spec(spec)
                sys.modules[model_name] = module 
                spec.loader.exec_module(module)
                
                # Registra o modelo e tenta pegar o contrato definido no código (fallback)
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
                
                # Extração de Configuração Interna {{ config(...) }}
                model_config = {"materialized": "table", "contract": {}}
                config_match = CONFIG_BLOCK_PATTERN.search(raw_sql)
                if config_match:
                    kwargs = KWARG_PATTERN.findall(config_match.group(1))
                    for k, v in kwargs: model_config[k] = v

                clean_sql = CONFIG_BLOCK_PATTERN.sub("", raw_sql).strip()
                deps = REF_PATTERN.findall(clean_sql)
                
                self.models_registry[model_name] = {
                    "type": "sql", 
                    "raw": clean_sql,
                    "config": model_config 
                }
                self.dependencies_map[model_name] = deps

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
                                # Injeta descrição para o comando 'docs'
                                self.models_registry[name]["config"]["description"] = m_meta.get("description", "")
                                
                                # Converte o formato do YAML para o dicionário de testes do Engine
                                if "columns" in m_meta:
                                    contract = {}
                                    for col in m_meta["columns"]:
                                        col_name = col.get("name")
                                        if "tests" in col:
                                            contract[col_name] = col["tests"]
                                    
                                    # O YAML tem precedência sobre o contrato definido no .sql ou .py
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
                compiled_sql = re.sub(REF_PATTERN, r"\1", model_info["raw"])
                
                if mat_type == "INCREMENTAL":
                    with self.print_lock: logger.forge(f"Processando [INCREMENTAL] '{model_name}'...")
                    
                    # Estratégia Idempotente: Tabela Temporária -> Merge/Delete -> Insert
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
            
            # Fecha a conexão limpa
            if hasattr(adapter, 'close'): adapter.close()
            
            return {"model": model_name, "status": "success", "execution_time": execution_time, "rows": rows_affected}
            
        except Exception as e:
            with self.print_lock: logger.error(f"✗ Erro crítico em '{model_name}': {e}")
            if hasattr(adapter, 'close'): adapter.close()
            return {"model": model_name, "status": "error", "execution_time": round(time.time() - start_model, 3), "error": str(e)}

    def _execute_dag(self, filter_type=None, command_name="run"):
        self.discover_models()
        self.artifact_manager.save_manifest(self.models_registry, self.dependencies_map)
        
        # Lê a quantidade de threads do toml, padrão é 4
        max_workers = self.config.get("project", {}).get("threads", 4)
        
        # Gerenciador de Grafo Nativo do Python
        ts = graphlib.TopologicalSorter(self.dependencies_map)
        ts.prepare()
        
        run_results_log = []
        context_cache = {}
        has_errors = False
        success_count = 0

        with self.print_lock:
            logger.info(f"Iniciando pool de execução ({max_workers} threads alocadas).")

        # Orquestrador Paralelo
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures_map = {}
            
            while ts.is_active():
                ready_nodes = ts.get_ready()
                
                for node in ready_nodes:
                    fut = executor.submit(self._execute_node, node, filter_type, context_cache)
                    futures_map[fut] = node
                    
                if not futures_map:
                    break # Impasse ou conclusão
                    
                # Espera a primeira thread terminar para liberar seus dependentes
                done, _ = wait(futures_map.keys(), return_when=FIRST_COMPLETED)
                
                for fut in done:
                    node = futures_map.pop(fut)
                    try:
                        result = fut.result()
                        run_results_log.append(result)
                        
                        if result["status"] in ["success", "skipped"]:
                            if result["status"] == "success": success_count += 1
                            ts.done(node) # Libera os nós filhos no DAG
                        else:
                            has_errors = True
                            # Se falhou, NÃO chamamos ts.done(node). 
                            # Isso bloqueia inteligentemente qualquer modelo dependente.
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

    # ... (métodos test e compile permanecem idênticos, apenas garanta que chamam self.discover_models() e usam self._get_thread_safe_adapter() se mexerem no banco)

    def test(self):
        """Executa os contratos de dados"""
        from dfg.logging import logger
        import sys
        
        # AJUSTE 1: Usar a conexão thread-safe e garantir que ela feche
        adapter = self._get_thread_safe_adapter()
        self.discover_models()
        logger.info("\n--- Iniciando Validação de Contratos ---")
        erros = 0
        
        try:
            for model_name, model_info in self.models_registry.items():
                contract = None
                
                # AJUSTE 2: Lógica dividida para suportar Python e preparar terreno para SQL
                if model_info["type"] == "python":
                    module = sys.modules.get(model_name)
                    contract = getattr(module, 'CONTRACT', None) if module else None
                elif model_info["type"] == "sql":
                    # Por enquanto, pegamos do config se existir (ex: {{ config(contract={'id': ['not_null']}) }})
                    # O ideal no futuro é ler de um arquivo .yml padrão do mercado
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
            # AJUSTE 3: Higiene de recursos
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
        import re
        
        logger.info("Compilando modelos e gerando manifest.json...")
        self.discover_models()
        
        self.artifact_manager.save_manifest(self.models_registry, self.dependencies_map)
        
        compiled_dir = os.path.join(self.project_dir, "target", "compiled")
        os.makedirs(compiled_dir, exist_ok=True)
        
        for name, info in self.models_registry.items():
            if info["type"] == "sql":
                # AJUSTE SÊNIOR: Acessando a chave de forma segura através do dicionário de config
                mat_type = info.get("config", {}).get("materialized", "table").upper()
                
                # Aproveitamos para compilar usando a REF_PATTERN que já definimos no topo do engine.py
                # (Certifique-se de que REF_PATTERN está importada ou acessível aqui)
                compiled_sql = re.sub(r"\{\{\s*ref\('([^']+)'\)\s*\}\}", r"\1", info["raw"])
                
                with open(os.path.join(compiled_dir, f"{name}.sql"), "w", encoding="utf-8") as f:
                    f.write(f"-- Materialização: {mat_type}\n{compiled_sql}")
                logger.success(f"Compilado: {name}.sql")