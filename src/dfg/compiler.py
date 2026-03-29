# src/dfg/compile.py
import jinja2
import re
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
    def parse_snapshot(self, raw_sql: str) -> dict:
        """
        Analisa um arquivo SQL em busca de blocos de snapshot e extrai suas configurações e query base.
        """
        # 1. Encontrar o bloco principal: {% snapshot nome_do_snapshot %} ... {% endsnapshot %}
        # re.DOTALL permite que o '.' capture quebras de linha
        snapshot_pattern = r"{%\s*snapshot\s+(\w+)\s*%}(.*?){%\s*endsnapshot\s*%}"
        match = re.search(snapshot_pattern, raw_sql, re.DOTALL)
        
        if not match:
            return None # Não é um arquivo de snapshot válido
            
        snapshot_name = match.group(1)
        inner_content = match.group(2)
        
        # 2. Encontrar o bloco de configuração: {{ config(...) }}
        config_pattern = r"{{\s*config\((.*?)\)\s*}}"
        config_match = re.search(config_pattern, inner_content, re.DOTALL)
        
        config_dict = {}
        if config_match:
            config_str = config_match.group(1)
            # Extrai pares chave-valor simples, ex: unique_key='user_id'
            kwargs_pattern = r"(\w+)\s*=\s*['\"]([^'\"]+)['\"]"
            for key, value in re.findall(kwargs_pattern, config_str):
                config_dict[key] = value
                
            # Remove o bloco de config do SQL para sobrar apenas o SELECT
            source_sql = re.sub(config_pattern, '', inner_content, flags=re.DOTALL).strip()
        else:
            source_sql = inner_content.strip()
            
        # 3. Compila o SQL restante (resolve as tags Jinja padrão, como o {{ ref('stg_users') }})
        # Aqui assumimos que o método `compile` ou `render` já existe na sua classe SQLCompiler
        compiled_sql = self.compile_query(source_sql) # Ajuste para o nome do método que renderiza jinja
        
        return {
            "snapshot_name": snapshot_name,
            "config": config_dict,
            "compiled_sql": compiled_sql
        }

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