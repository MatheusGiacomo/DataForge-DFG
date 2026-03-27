# src/dfg/compile.py
import jinja2
from dfg.logging import logger

class ModelContext:
    """
    Representa o contexto de execução de um modelo SQL.
    Injetamos métodos aqui que estarão disponíveis dentro do {{ ... }} do Jinja.
    """
    def __init__(self, model_name, target_schema):
        self.model_name = model_name
        self.target_schema = target_schema
        self.model_config = {} 
        self.dependencies = [] 

    def ref(self, referenced_model):
        """Implementação da função {{ ref('nome_da_tabela') }}."""
        self.dependencies.append(referenced_model)
        # Retorna apenas o nome por enquanto; 
        # Futuramente pode retornar schema.tabela conforme o banco.
        return referenced_model

    def config(self, **kwargs):
        """Implementação da função {{ config(materialized='table') }}."""
        self.model_config.update(kwargs)
        # Retorna string vazia para o Jinja não renderizar 'None' no SQL final
        return ""

class SQLCompiler:
    def __init__(self, target_schema):
        self.target_schema = target_schema
        # Configuração do ambiente Jinja
        self.env = jinja2.Environment(
            loader=jinja2.BaseLoader(),
            trim_blocks=True,
            lstrip_blocks=True,
            undefined=jinja2.StrictUndefined # SÊNIOR: Erra feio se usar variável não definida
        )

    def compile(self, sql_raw: str, model_name: str):
        """
        Transforma SQL com Jinja em SQL puro e extrai metadados.
        """
        context = ModelContext(model_name, self.target_schema)

        try:
            template = self.env.from_string(sql_raw)
            # Renderiza passando as funções 'ref' e 'config' como globais do template
            sql_compiled = template.render(
                ref=context.ref,
                config=context.config,
                target_schema=self.target_schema
            )

            return {
                "sql": sql_compiled.strip(),
                "depends_on": list(set(context.dependencies)), # Remove duplicatas de refs
                "config": context.model_config
            }
        except jinja2.exceptions.TemplateSyntaxError as e:
            logger.error(f"Erro de sintaxe no modelo '{model_name}' (Linha {e.lineno}): {e.message}")
            raise
        except Exception as e:
            logger.error(f"Erro inesperado ao compilar '{model_name}': {e}")
            raise