# src/dfg/adapters/factory.py
"""
Factory de adaptadores de banco de dados do DataForge.

Responsável por mapear os tipos declarados nos arquivos de perfil
para os drivers Python correspondentes e instanciar o adaptador correto.
"""
import importlib

from dfg.adapters.generic import GenericDBAPIAdapter
from dfg.logging import logger


class AdapterFactory:
    """
    Factory estática para criação de adaptadores DB-API 2.0.

    Mapeamento: tipo no profiles.toml → nome do módulo Python do driver.
    """

    DRIVER_MAP: dict[str, str] = {
        "postgres": "psycopg2",
        "postgresql": "psycopg2",
        "mysql": "mysql.connector",
        "sqlite": "sqlite3",
        "duckdb": "duckdb",
    }

    @classmethod
    def get_adapter(cls, adapter_type: str) -> GenericDBAPIAdapter:
        """
        Instancia e retorna o adaptador para o tipo de banco especificado.

        Parâmetros
        ----------
        adapter_type : str
            Tipo de banco conforme declarado no profiles.toml
            (ex: 'duckdb', 'postgres', 'sqlite').

        Levanta
        -------
        ValueError
            Se o tipo de banco não estiver mapeado.
        ImportError
            Se o driver Python correspondente não estiver instalado.
        """
        normalized = adapter_type.lower().strip()
        driver_name = cls.DRIVER_MAP.get(normalized)

        if not driver_name:
            supported = ", ".join(sorted(cls.DRIVER_MAP.keys()))
            raise ValueError(
                f"Driver para o tipo '{adapter_type}' não está configurado no DataForge. "
                f"Tipos suportados: {supported}."
            )

        try:
            driver_module = importlib.import_module(driver_name)
            return GenericDBAPIAdapter(driver_module)
        except ImportError:
            logger.error(
                f"Driver '{driver_name}' não encontrado. "
                f"Instale-o com: pip install {driver_name.split('.')[0]}"
            )
            raise
