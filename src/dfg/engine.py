import tomllib
import importlib.util
import os
import sys
import re
import time
from dfg.dag import DAGResolver
from dfg.adapters.factory import AdapterFactory
from dfg.logging import logger
from dfg.state import StateManager 

# Regex para capturar os padrões do dbt
CONFIG_PATTERN = re.compile(r"\{\{\s*config\(\s*materialized\s*=\s*'([^']+)'\s*\)\s*\}\}")
REF_PATTERN = re.compile(r"\{\{\s*ref\('([^']+)'\)\s*\}\}")

class DFGEngine:
    def __init__(self, project_dir: str):
        self.project_dir = project_dir
        
        # Inicializa o logger centralizado
        logger.setup(self.project_dir)
        
        self.config = self._load_config()
        self.models_dir = os.path.join(self.project_dir, "models")
        self.models_registry = {}
        self.dependencies_map = {}
        self.state_manager = StateManager(self.project_dir)
        self.adapter = None # Será inicializado sob demanda

    def _load_config(self) -> dict:
        project_toml_path = os.path.join(self.project_dir, "dfg_project.toml")
        profiles_toml_path = os.path.join(self.project_dir, "profiles.toml")
        
        if not os.path.exists(project_toml_path):
            raise FileNotFoundError("Arquivo dfg_project.toml não encontrado. Rode 'dfg init' primeiro.")
            
        if not os.path.exists(profiles_toml_path):
            raise FileNotFoundError("Arquivo profiles.toml não encontrado na raiz do projeto.")

        with open(project_toml_path, "rb") as f:
            config = tomllib.load(f)
            
        with open(profiles_toml_path, "rb") as f:
            profiles = tomllib.load(f)
            
        profile_name = config["project"]["profile"]
        target_name = config["project"]["target"]
        
        try:
            credentials = profiles[profile_name]["outputs"][target_name]
            config["targets"] = {target_name: credentials}
        except KeyError:
            raise ValueError(f"Target '{target_name}' não encontrado no profile '{profile_name}'")
            
        logger.success(f"Configuração carregada. Profile: '{profile_name}', Target: '{target_name}'")
        return config

    def _get_adapter(self):
        """Helper para garantir conexão com o banco sem repetir código"""
        if self.adapter:
            return self.adapter
            
        target_name = self.config["project"]["target"]
        target_config = self.config["targets"][target_name]
        
        logger.debug(f"Estabelecendo conexão via driver: '{target_config.get('type')}'...")
        self.adapter = AdapterFactory.get_adapter(target_config["type"])
        self.adapter.connect(target_config)
        return self.adapter

    def discover_models(self):
        """Escaneia arquivos e popula o registro e mapa de dependências"""
        if self.models_registry: return # Evita re-escanear se já carregado

        logger.forge("Escaneando diretório de modelos...")
        start_time = time.time()
        
        for filename in os.listdir(self.models_dir):
            if filename == "__init__.py": continue
            filepath = os.path.join(self.models_dir, filename)
            
            # --- MODELOS PYTHON (Ingestão) ---
            if filename.endswith(".py"):
                model_name = filename[:-3]
                spec = importlib.util.spec_from_file_location(model_name, filepath)
                module = importlib.util.module_from_spec(spec)
                sys.modules[model_name] = module 
                spec.loader.exec_module(module)
                
                self.models_registry[model_name] = {"type": "python", "func": module.model}
                self.dependencies_map[model_name] = getattr(module, 'DEPENDENCIES', [])

            # --- MODELOS SQL (Transformação) ---
            elif filename.endswith(".sql"):
                model_name = filename[:-4]
                with open(filepath, "r", encoding="utf-8") as f:
                    raw_sql = f.read()
                
                config_match = CONFIG_PATTERN.search(raw_sql)
                materialization = config_match.group(1) if config_match else "table"
                clean_sql = CONFIG_PATTERN.sub("", raw_sql).strip()
                deps = REF_PATTERN.findall(clean_sql)
                
                self.models_registry[model_name] = {
                    "type": "sql", 
                    "raw": clean_sql,
                    "materialized": materialization 
                }
                self.dependencies_map[model_name] = deps

        logger.success(f"Modelos carregados em {(time.time() - start_time):.3f}s.")

    def _execute_dag(self, filter_type=None):
        """
        Lógica central de execução que percorre o DAG.
        filter_type: 'python' para ingestão, 'sql' para transformação, None para todos.
        """
        self.discover_models()
        adapter = self._get_adapter()
        resolver = DAGResolver(self.dependencies_map)
        execution_order = resolver.get_execution_order()
        
        results_cache = {}
        success_count = 0

        for model_name in execution_order:
            model_info = self.models_registry.get(model_name)
            if not model_info: continue
            
            # Se pedimos apenas um tipo (ingest ou transform), pulamos os outros
            if filter_type and model_info["type"] != filter_type:
                continue

            start_model = time.time()
            
            # --- EXECUÇÃO SQL ---
            if model_info["type"] == "sql":
                mat_type = model_info["materialized"].upper()
                compiled_sql = re.sub(REF_PATTERN, r"\1", model_info["raw"])
                
                logger.forge(f"Materializando SQL '{model_name}' ({mat_type})...")
                drop_queries = [f"DROP VIEW IF EXISTS {model_name} CASCADE;", f"DROP TABLE IF EXISTS {model_name} CASCADE;"]
                create_query = f"CREATE {mat_type} {model_name} AS \n{compiled_sql}"
                
                try:
                    for drop in drop_queries: adapter.execute(drop)
                    adapter.execute(create_query)
                    logger.success(f"Modelo SQL '{model_name}' pronto ({(time.time() - start_model):.3f}s).")
                    success_count += 1
                except Exception as e:
                    logger.error(f"Erro no SQL {model_name}: {e}")
                    return False
                    
            # --- EXECUÇÃO PYTHON ---
            else:
                logger.forge(f"Executando Ingestão Python '{model_name}'...")
                context = {
                    "config": self.config,
                    "ref": lambda name: results_cache.get(name),
                    "state": self.state_manager.get(model_name),
                    "set_state": lambda val, m=model_name: self.state_manager.set(m, val)
                }
                
                try:
                    data = model_info["func"](context)
                    results_cache[model_name] = data
                    
                    if data:
                        target_schema = self.config["targets"][self.config["project"]["target"]].get("schema", "public")
                        adapter.load_data(table_name=model_name, data=data, schema=target_schema)
                        logger.success(f"Ingestão '{model_name}' finalizada: {len(data)} registros ({(time.time() - start_model):.3f}s).")
                    else:
                        logger.info(f"Ingestão '{model_name}' sem novos dados.")
                    success_count += 1
                except Exception as e:
                    logger.error(f"Erro na Ingestão {model_name}: {e}")
                    return False

        return True if success_count > 0 else "no_work"

    def ingest(self):
        """Comando: dfg ingest"""
        logger.info("Iniciando fase isolada de INGESTÃO (Modelos Python)...")
        return self._execute_dag(filter_type="python")

    def transform(self):
        """Comando: dfg transform"""
        logger.info("Iniciando fase isolada de TRANSFORMAÇÃO (Modelos SQL)...")
        return self._execute_dag(filter_type="sql")

    def run(self):
        """Comando: dfg run (Orquestração Completa)"""
        start_run = time.time()
        logger.info(f"Iniciando Pipeline Completo no ambiente: {self.config['project']['target'].upper()}")
        
        if not self.ingest():
            return False
            
        if not self.transform():
            return False
            
        logger.success(f"--- Forja finalizada em {(time.time() - start_run):.3f}s! ---")
        return True

    def test(self):
        """Executa os contratos de dados"""
        adapter = self._get_adapter()
        self.discover_models()
        logger.info("\n--- Iniciando Validação de Contratos ---")
        erros = 0
        
        for model_name, model_info in self.models_registry.items():
            # Tenta pegar contrato do módulo Python associado
            module = sys.modules.get(model_name)
            contract = getattr(module, 'CONTRACT', None) if module else None
            
            if not contract:
                logger.warn(f"Modelo '{model_name}': Sem contrato definido. Pulando.")
                continue
                
            logger.forge(f"Testando '{model_name}'...")
            try:
                # Validação básica de existência
                res = adapter.execute(f"SELECT COUNT(*) FROM {model_name}")
                if not res or res[0][0] == 0: logger.warn(f"  [AVISO] '{model_name}' está vazia.")
                
                for coluna, testes in contract.items():
                    for teste in testes:
                        if teste == "not_null":
                            nulos = adapter.execute(f"SELECT COUNT(*) FROM {model_name} WHERE {coluna} IS NULL")[0][0]
                            if nulos > 0:
                                logger.error(f"  [FALHA] {model_name}.{coluna}: {nulos} nulos!")
                                erros += 1
                        elif teste == "unique":
                            dups = adapter.execute(f"SELECT COUNT(*) FROM (SELECT {coluna} FROM {model_name} GROUP BY {coluna} HAVING COUNT(*) > 1) AS s")[0][0]
                            if dups > 0:
                                logger.error(f"  [FALHA] {model_name}.{coluna}: Duplicatas detectadas!")
                                erros += 1
            except Exception as e:
                logger.error(f" Erro ao testar {model_name}: {e}")
                erros += 1

        if erros > 0: sys.exit(1)
        logger.success("Todos os contratos validados!")

    def compile(self):
        """Gera SQLs compilados na pasta target/"""
        logger.info("Compilando modelos (Dry Run)...")
        self.discover_models()
        compiled_dir = os.path.join(self.project_dir, "target", "compiled")
        os.makedirs(compiled_dir, exist_ok=True)
        
        for name, info in self.models_registry.items():
            if info["type"] == "sql":
                compiled_sql = re.sub(REF_PATTERN, r"\1", info["raw"])
                with open(os.path.join(compiled_dir, f"{name}.sql"), "w", encoding="utf-8") as f:
                    f.write(f"-- Materialização: {info['materialized'].upper()}\n{compiled_sql}")
                logger.success(f"Compilado: {name}.sql")

#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#