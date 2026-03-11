# src/dfg/adapters/factory.py
import importlib
from dfg.adapters.generic import GenericDBAPIAdapter
from dfg.logger import logger

class AdapterFactory:
    # Mapeamento de 'tipo no TOML' -> 'nome do módulo python'
    DRIVER_MAP = {
        "postgres": "psycopg2",
        "mysql": "mysql.connector",
        "sqlite": "sqlite3",
        "duckdb": "duckdb" # DuckDB também segue a PEP 249 em grande parte
    }

    @classmethod
    def get_adapter(cls, adapter_type: str):
        driver_name = cls.DRIVER_MAP.get(adapter_type.lower())
        
        if not driver_name:
            raise ValueError(f"Driver para '{adapter_type}' não configurado no DFG.")

        try:
            # Importa o driver dinamicamente apenas quando necessário
            driver_module = importlib.import_module(driver_name)
            logger.info(f"Driver '{driver_name}' carregado com sucesso.")
            return GenericDBAPIAdapter(driver_module)
        except ImportError:
            logger.error(f"Driver '{driver_name}' não encontrado. Instale-o com: pip install {driver_name}")
            raise