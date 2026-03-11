# src/dfg/engine.py
import tomllib
import importlib.util
import os
from dfg.dag import DAGResolver
from dfg.adapters.factory import AdapterFactory
from dfg.logger import logger
from dfg.state import StateManager # Importação do novo módulo

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
        config_path = os.path.join(self.project_dir, "dfg_project.toml")
        with open(config_path, "rb") as f:
            return tomllib.load(f)

    def discover_models(self):
        logger.info("Escaneando diretório de modelos...")
        for filename in os.listdir(self.models_dir):
            if filename.endswith(".py") and filename != "__init__.py":
                model_name = filename[:-3]
                filepath = os.path.join(self.models_dir, filename)
                spec = importlib.util.spec_from_file_location(model_name, filepath)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                
                self.models_registry[model_name] = module.model
                self.dependencies_map[model_name] = getattr(module, 'DEPENDENCIES', [])
        logger.success(f"{len(self.models_registry)} modelos carregados.")

    def run(self):
        try:
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
                
                # O Contexto agora é rico: tem config, ref e STATE
                context = {
                    "config": self.config,
                    "ref": lambda name: results_cache.get(name),
                    "state": self.state_manager.get(model_name),
                    "set_state": lambda val, m=model_name: self.state_manager.set(m, val)
                }
                
                data = self.models_registry[model_name](context)
                results_cache[model_name] = data
                
                # Usa o schema definido no TOML ou padrão 'public'
                target_schema = target_config.get("schema", "public")
                adapter.load_data(table_name=model_name, data=data, schema=target_schema)
                
                logger.success(f"Modelo {model_name} forjado com sucesso.")

            logger.success("--- Forja finalizada com êxito! ---")

        except Exception as e:
            logger.error(f"Erro durante a execução: {e}")
            raise