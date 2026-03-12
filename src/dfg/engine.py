# src/dfg/engine.py
import tomllib
import importlib.util
import os
import sys
import re
from dfg.dag import DAGResolver
from dfg.adapters.factory import AdapterFactory
from dfg.logger import logger
from dfg.state import StateManager # Importação do novo módulo

REF_PATTERN = re.compile(r"\{\{\s*ref\('([^']+)'\)\s*\}\}")

class DFGEngine:
    def __init__(self, project_dir: str):
        self.project_dir = project_dir
        self.config = self._load_config()
        self.models_dir = os.path.join(self.project_dir, "models")
        self.models_registry = {}
        self.dependencies_map = {}
        # Inicializa o gerenciador de estado na pasta do projeto
        self.state_manager = StateManager(self.project_dir)

    def _load_config(self) -> dict:
        import tomllib
        
        project_toml_path = os.path.join(self.project_dir, "dfg_project.toml")
        profiles_toml_path = os.path.join(self.project_dir, "profiles.toml")
        
        if not os.path.exists(project_toml_path):
            raise FileNotFoundError("Arquivo dfg_project.toml não encontrado. Rode 'dfg init' primeiro.")
            
        if not os.path.exists(profiles_toml_path):
            raise FileNotFoundError("Arquivo profiles.toml não encontrado na raiz do projeto.")

        # Carrega ambos os arquivos
        with open(project_toml_path, "rb") as f:
            config = tomllib.load(f)
            
        with open(profiles_toml_path, "rb") as f:
            profiles = tomllib.load(f)
            
        profile_name = config["project"]["profile"]
        target_name = config["project"]["target"]
        
        # Injeta as credenciais do profile para dentro da configuração que o DFG vai usar
        try:
            credentials = profiles[profile_name]["outputs"][target_name]
            config["targets"] = {target_name: credentials}
        except KeyError:
            raise ValueError(f"Target '{target_name}' não encontrado no profile '{profile_name}' dentro de profiles.toml")
            
        return config

    def discover_models(self):
        import sys
        from dfg.logger import logger
        logger.info("Escaneando diretório de modelos (Python e SQL)...")
        
        for filename in os.listdir(self.models_dir):
            if filename == "__init__.py": continue
            
            filepath = os.path.join(self.models_dir, filename)
            
            # --- PARSER PARA MODELOS PYTHON (Ingestão/EL) ---
            if filename.endswith(".py"):
                model_name = filename[:-3]
                spec = importlib.util.spec_from_file_location(model_name, filepath)
                module = importlib.util.module_from_spec(spec)
                sys.modules[model_name] = module 
                spec.loader.exec_module(module)
                
                # Registra como função Python
                self.models_registry[model_name] = module.model
                self.dependencies_map[model_name] = getattr(module, 'DEPENDENCIES', [])

            # --- PARSER PARA MODELOS SQL (Transformação/T) ---
            elif filename.endswith(".sql"):
                model_name = filename[:-4]
                with open(filepath, "r", encoding="utf-8") as f:
                    raw_sql = f.read()
                
                # Extrai dependências lendo os refs via Regex
                deps = REF_PATTERN.findall(raw_sql)
                
                # Registra como um dicionário contendo o SQL
                self.models_registry[model_name] = {"type": "sql", "raw": raw_sql}
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
            
            # SE FOR MODELO SQL (A Mágica da Transformação)
            if isinstance(model_content, dict) and model_content.get("type") == "sql":
                raw_sql = model_content["raw"]
                
                # Compila o SQL substituindo {{ ref('tabela') }} pelo nome real da tabela
                compiled_sql = re.sub(REF_PATTERN, r"\1", raw_sql)
                
                # Materialização Padrão (Idempotente): Deleta se existir e cria de novo
                drop_query = f"DROP TABLE IF EXISTS {model_name};"
                create_query = f"CREATE TABLE {model_name} AS \n{compiled_sql}"
                
                try:
                    adapter.execute(drop_query)
                    adapter.execute(create_query)
                    logger.success(f"Modelo SQL {model_name} materializado com sucesso.")
                except Exception as e:
                    logger.error(f"Erro ao executar SQL no modelo {model_name}: {e}")
                    raise
                    
            # SE FOR MODELO PYTHON (A Mágica da Extração)
            else:
                context = {
                    "config": self.config,
                    "ref": lambda name: results_cache.get(name),
                    "state": self.state_manager.get(model_name),
                    "set_state": lambda val, m=model_name: self.state_manager.set(m, val)
                }
                
                data = model_content(context) # Chama a função model()
                results_cache[model_name] = data
                
                if data:
                    target_schema = target_config.get("schema", "public")
                    adapter.load_data(table_name=model_name, data=data, schema=target_schema)
                    logger.success(f"Modelo Python {model_name} forjado com {len(data)} registros.")
                else:
                    logger.info(f"Modelo Python {model_name} não retornou novos dados.")

            logger.success("--- Forja finalizada com êxito! ---")
    
    def test(self):
        import sys
        from dfg.logger import logger
        from dfg.adapters.factory import AdapterFactory # Garanta que está importado
        
        # 1. Carregar configuração do target atual
        target_name = self.config["project"]["target"]
        target_config = self.config["targets"][target_name]
        
        # 2. Inicializar e conectar o adapter (AQUI estava o erro!)
        adapter = AdapterFactory.get_adapter(target_config["type"])
        adapter.connect(target_config)
        
        # 3. Registrar e carregar os modelos na memória
        self.discover_models()
        
        logger.info("\n--- Iniciando Validação de Contratos de Dados ---")
        erros_encontrados = 0
        
        for model_name, func in self.models_registry.items():
            # Busca o módulo no cache global do Python
            module = sys.modules.get(model_name)
            contract = getattr(module, 'CONTRACT', None)
            
            if not contract:
                logger.warn(f"Modelo '{model_name}': Nenhum contrato (CONTRACT) definido. Pulando.")
                continue
                
            logger.forge(f"Testando '{model_name}'...")
            
            # Sanity Check: A tabela existe e tem dados?
            try:
                count_res = adapter.execute(f"SELECT COUNT(*) FROM {model_name}")
                count_total = count_res[0][0] if count_res else 0
                if count_total == 0:
                    logger.warn(f"  [AVISO] A tabela '{model_name}' está vazia.")
            except Exception:
                logger.error(f"  [ERRO] Tabela '{model_name}' não encontrada no banco.")
                erros_encontrados += 1
                continue

            # Validação das regras do contrato
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
                        # Query universal para duplicatas
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
            logger.success("Todos os contratos validados com êxito!")
        else:
            logger.error(f"Validação encerrada com {erros_encontrados} erro(s).")
            sys.exit(1)