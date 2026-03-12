import tomllib
import importlib.util
import os
import sys
import re
from dfg.dag import DAGResolver
from dfg.adapters.factory import AdapterFactory
from dfg.logger import logger
from dfg.state import StateManager 

# Regex para capturar os padrões do dbt
CONFIG_PATTERN = re.compile(r"\{\{\s*config\(\s*materialized\s*=\s*'([^']+)'\s*\)\s*\}\}")
REF_PATTERN = re.compile(r"\{\{\s*ref\('([^']+)'\)\s*\}\}")

class DFGEngine:
    def __init__(self, project_dir: str):
        self.project_dir = project_dir
        self.config = self._load_config()
        self.models_dir = os.path.join(self.project_dir, "models")
        self.models_registry = {}
        self.dependencies_map = {}
        self.state_manager = StateManager(self.project_dir)

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
            raise ValueError(f"Target '{target_name}' não encontrado no profile '{profile_name}' em profiles.toml")
            
        return config

    def discover_models(self):
        logger.info("Escaneando diretório de modelos (Python e SQL)...")
        
        for filename in os.listdir(self.models_dir):
            if filename == "__init__.py": continue
            filepath = os.path.join(self.models_dir, filename)
            
            # --- MODELOS PYTHON ---
            if filename.endswith(".py"):
                model_name = filename[:-3]
                spec = importlib.util.spec_from_file_location(model_name, filepath)
                module = importlib.util.module_from_spec(spec)
                sys.modules[model_name] = module 
                spec.loader.exec_module(module)
                
                self.models_registry[model_name] = module.model
                self.dependencies_map[model_name] = getattr(module, 'DEPENDENCIES', [])

            # --- MODELOS SQL (Com Materialização) ---
            elif filename.endswith(".sql"):
                model_name = filename[:-4]
                with open(filepath, "r", encoding="utf-8") as f:
                    raw_sql = f.read()
                
                # Captura a configuração de materialização (default: table)
                config_match = CONFIG_PATTERN.search(raw_sql)
                materialization = config_match.group(1) if config_match else "table"
                
                # Remove a tag de config do SQL para não dar erro de sintaxe no banco
                clean_sql = CONFIG_PATTERN.sub("", raw_sql).strip()
                
                # Extrai dependências via ref()
                deps = REF_PATTERN.findall(clean_sql)
                
                self.models_registry[model_name] = {
                    "type": "sql", 
                    "raw": clean_sql,
                    "materialized": materialization 
                }
                self.dependencies_map[model_name] = deps

        logger.success(f"{len(self.models_registry)} modelos carregados no Grafo.")

    def run(self):
        target_name = self.config["project"]["target"]
        target_config = self.config["targets"][target_name]
        
        logger.info(f"Iniciando Pipeline no ambiente: {target_name.upper()}")
        
        adapter = AdapterFactory.get_adapter(target_config["type"])
        adapter.connect(target_config)
        
        self.discover_models()
        resolver = DAGResolver(self.dependencies_map)
        execution_order = resolver.get_execution_order()
        
        results_cache = {}

        for model_name in execution_order:
            if model_name not in self.models_registry: continue
            logger.forge(model_name)
            
            model_content = self.models_registry[model_name]
            
            # LÓGICA SQL
            if isinstance(model_content, dict) and model_content.get("type") == "sql":
                mat_type = model_content["materialized"].upper()
                raw_sql = model_content["raw"]
                compiled_sql = re.sub(REF_PATTERN, r"\1", raw_sql)
                
                # Limpeza preventiva: remove se existir como table ou view
                drop_queries = [
                    f"DROP VIEW IF EXISTS {model_name} CASCADE;",
                    f"DROP TABLE IF EXISTS {model_name} CASCADE;"
                ]
                create_query = f"CREATE {mat_type} {model_name} AS \n{compiled_sql}"
                
                try:
                    for drop in drop_queries: adapter.execute(drop)
                    adapter.execute(create_query)
                    logger.success(f"Modelo SQL {model_name} materializado como {mat_type}.")
                except Exception as e:
                    logger.error(f"Erro ao materializar SQL {model_name}: {e}")
                    raise
                    
            # LÓGICA PYTHON
            else:
                context = {
                    "config": self.config,
                    "ref": lambda name: results_cache.get(name),
                    "state": self.state_manager.get(model_name),
                    "set_state": lambda val, m=model_name: self.state_manager.set(m, val)
                }
                
                data = model_content(context)
                results_cache[model_name] = data
                
                if data:
                    target_schema = target_config.get("schema", "public")
                    adapter.load_data(table_name=model_name, data=data, schema=target_schema)
                    logger.success(f"Modelo Python {model_name} forjado com {len(data)} registros.")
                else:
                    logger.info(f"Modelo Python {model_name} não retornou novos dados.")

        logger.success("--- Forja finalizada com êxito! ---")
    
    def test(self):
        target_name = self.config["project"]["target"]
        target_config = self.config["targets"][target_name]
        
        adapter = AdapterFactory.get_adapter(target_config["type"])
        adapter.connect(target_config)
        
        self.discover_models()
        
        logger.info("\n--- Iniciando Validação de Contratos de Dados ---")
        erros_encontrados = 0
        
        for model_name, model_info in self.models_registry.items():
            # Pegamos o contrato apenas de modelos Python (os SQLs ainda não possuem contrato definido)
            if callable(model_info):
                module = sys.modules.get(model_name)
                contract = getattr(module, 'CONTRACT', None)
            else:
                # Opcional: Futuramente adicionar leitura de contratos para SQL via YAML
                contract = None
            
            if not contract:
                logger.warn(f"Modelo '{model_name}': Nenhum contrato (CONTRACT) definido. Pulando.")
                continue
                
            logger.forge(f"Testando '{model_name}'...")
            
            try:
                count_res = adapter.execute(f"SELECT COUNT(*) FROM {model_name}")
                count_total = count_res[0][0] if count_res else 0
                if count_total == 0:
                    logger.warn(f"  [AVISO] A tabela '{model_name}' está vazia.")
            except Exception:
                logger.error(f"  [ERRO] Tabela '{model_name}' não encontrada.")
                erros_encontrados += 1
                continue

            for coluna, testes in contract.items():
                for teste in testes:
                    if teste == "not_null":
                        query = f"SELECT COUNT(*) FROM {model_name} WHERE {coluna} IS NULL"
                        res = adapter.execute(query)
                        nulos = res[0][0] if res else 0
                        if nulos > 0:
                            logger.error(f"  [FALHA] {model_name}.{coluna}: {nulos} nulos encontrados!")
                            erros_encontrados += 1
                        else:
                            logger.success(f"  [PASSOU] {model_name}.{coluna} sem nulos.")
                            
                    elif teste == "unique":
                        query = f"SELECT COUNT(*) FROM (SELECT {coluna} FROM {model_name} GROUP BY {coluna} HAVING COUNT(*) > 1) AS sub"
                        res = adapter.execute(query)
                        duplicatas = res[0][0] if res else 0
                        if duplicatas > 0:
                            logger.error(f"  [FALHA] {model_name}.{coluna}: duplicatas detectadas!")
                            erros_encontrados += 1
                        else:
                            logger.success(f"  [PASSOU] {model_name}.{coluna} é único.")
                            
        print("-" * 50)
        if erros_encontrados == 0:
            logger.success("Todos os contratos validados!")
        else:
            logger.error(f"Validação encerrada com {erros_encontrados} erro(s).")
            sys.exit(1)
    
    def compile(self):
        """
        Realiza o Dry Run: resolve os templates e salva os SQLs puros na pasta target/
        sem executar nada no banco de dados.
        """
        import os
        from dfg.logger import logger
        
        logger.info("Iniciando compilação dos modelos (Dry Run)...")
        self.discover_models()
        
        # Cria a pasta de artefatos (como o dbt faz)
        compiled_dir = os.path.join(self.project_dir, "target", "compiled")
        os.makedirs(compiled_dir, exist_ok=True)
        
        sql_models_count = 0
        
        for model_name, model_content in self.models_registry.items():
            # A compilação faz sentido apenas para os modelos SQL
            if isinstance(model_content, dict) and model_content.get("type") == "sql":
                raw_sql = model_content["raw"]
                mat_type = model_content.get("materialized", "table").upper()
                
                # Resolve a mágica do DFG (as funções ref)
                compiled_sql = re.sub(REF_PATTERN, r"\1", raw_sql)
                
                # Prepara o arquivo final
                output_path = os.path.join(compiled_dir, f"{model_name}.sql")
                
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(f"-- [Data Forge] Artefato Compilado\n")
                    f.write(f"-- Materialização configurada: {mat_type}\n\n")
                    f.write(compiled_sql)
                    
                logger.success(f"Compilado: {model_name}.sql -> {output_path}")
                sql_models_count += 1
                
        if sql_models_count == 0:
            logger.warn("Nenhum modelo SQL encontrado para compilar.")
        else:
            logger.info(f"Total de {sql_models_count} modelo(s) compilado(s) em 'target/compiled/'.")