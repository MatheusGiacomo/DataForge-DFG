# src/dfg/compiler.py
"""
Compilador Jinja2 para modelos SQL e snapshots do DataForge.

Responsabilidades:
- Renderizar templates Jinja2 em SQL puro
- Extrair metadados de configuração ({{ config(...) }})
- Rastrear dependências ({{ ref('model') }})
- Fazer parsing de blocos {% snapshot %} ... {% endsnapshot %}
"""
import re

import jinja2

from dfg.logging import logger


class ModelContext:
    """
    Contexto de execução injetado nos templates Jinja2.

    Expõe as funções `ref()` e `config()` que o autor do modelo
    pode usar diretamente no SQL, sem importações extras.
    """

    def __init__(self, model_name: str, target_schema: str):
        self.model_name = model_name
        self.target_schema = target_schema
        self.model_config: dict = {}
        self.dependencies: list[str] = []

    def ref(self, referenced_model: str) -> str:
        """
        Macro {{ ref('nome_da_tabela') }}.

        Registra a dependência e retorna o nome da tabela para ser
        interpolado no SQL final.
        """
        self.dependencies.append(referenced_model)
        return referenced_model

    def config(self, **kwargs) -> str:
        """
        Macro {{ config(materialized='table') }}.

        Armazena as configurações do modelo. Retorna string vazia
        para que o Jinja não insira 'None' no SQL renderizado.
        """
        self.model_config.update(kwargs)
        return ""


class SQLCompiler:
    """
    Motor de compilação Jinja2 para o DataForge.

    Parâmetros
    ----------
    target_schema : str
        Schema de destino que será disponibilizado como variável
        nos templates ({{ target_schema }}).
    """

    def __init__(self, target_schema: str = "public"):
        self.target_schema = target_schema
        self.env = jinja2.Environment(
            loader=jinja2.BaseLoader(),
            trim_blocks=True,
            lstrip_blocks=True,
            # StrictUndefined: falha imediatamente em variáveis não definidas,
            # evitando bugs silenciosos por typos nos templates.
            undefined=jinja2.StrictUndefined,
        )

    # ------------------------------------------------------------------
    # API Pública
    # ------------------------------------------------------------------

    def compile(self, sql_raw: str, model_name: str) -> dict:
        """
        Compila um arquivo SQL com Jinja2 e extrai seus metadados.

        Retorna
        -------
        dict com chaves:
            - ``sql``: SQL puro, pronto para execução
            - ``depends_on``: lista de modelos referenciados via ref()
            - ``config``: dicionário de configuração (materialized, unique_key, …)
        """
        context = ModelContext(model_name, self.target_schema)
        try:
            template = self.env.from_string(sql_raw)
            sql_compiled = template.render(
                ref=context.ref,
                config=context.config,
                target_schema=self.target_schema,
            )
            return {
                "sql": sql_compiled.strip(),
                "depends_on": list(set(context.dependencies)),
                "config": context.model_config,
            }
        except jinja2.exceptions.TemplateSyntaxError as e:
            logger.error(
                f"Erro de sintaxe Jinja no modelo '{model_name}' "
                f"(linha {e.lineno}): {e.message}"
            )
            raise
        except Exception as e:
            logger.error(f"Erro inesperado ao compilar '{model_name}': {e}")
            raise

    def render(self, sql_raw: str) -> str:
        """
        Renderização simples de Jinja2 sem contexto de modelo.

        Usado internamente pelo parse_snapshot para limpar as tags
        do SQL interno do bloco snapshot antes de executá-lo.
        """
        try:
            template = self.env.from_string(sql_raw)
            return template.render(target_schema=self.target_schema).strip()
        except jinja2.exceptions.TemplateSyntaxError as e:
            logger.error(f"Erro de sintaxe Jinja ao renderizar SQL (linha {e.lineno}): {e.message}")
            raise
        except Exception as e:
            logger.error(f"Erro inesperado ao renderizar SQL: {e}")
            raise

    def parse_snapshot(self, raw_sql: str) -> dict | None:
        """
        Analisa um arquivo de snapshot no formato:

        .. code-block:: sql

            {% snapshot nome_do_snapshot %}
            {{ config(unique_key='id', strategy='timestamp', updated_at='updated_at') }}
            SELECT * FROM {{ ref('stg_users') }}
            {% endsnapshot %}

        Retorna
        -------
        dict com chaves:
            - ``snapshot_name``: nome do snapshot
            - ``config``: dicionário de configuração extraído de {{ config(...) }}
            - ``compiled_sql``: SQL da query SELECT compilado (Jinja resolvido)

        Retorna ``None`` se o arquivo não contiver um bloco snapshot válido.
        """
        # Extrai o bloco snapshot completo
        snapshot_pattern = r"\{%\s*snapshot\s+(\w+)\s*%\}(.*?)\{%\s*endsnapshot\s*%\}"
        match = re.search(snapshot_pattern, raw_sql, re.DOTALL)

        if not match:
            return None

        snapshot_name = match.group(1)
        inner_content = match.group(2)

        # Extrai o bloco {{ config(...) }} e seus pares chave=valor
        config_pattern = r"\{\{\s*config\((.*?)\)\s*\}\}"
        config_match = re.search(config_pattern, inner_content, re.DOTALL)

        config_dict: dict = {}
        if config_match:
            config_str = config_match.group(1)
            # Captura pares: chave='valor' ou chave="valor"
            for key, value in re.findall(r"(\w+)\s*=\s*['\"]([^'\"]+)['\"]", config_str):
                config_dict[key] = value

            # Remove o bloco config do conteúdo para isolar a query SELECT
            source_sql = re.sub(config_pattern, "", inner_content, flags=re.DOTALL).strip()
        else:
            source_sql = inner_content.strip()

        # Usa o método render() para resolver referências Jinja dentro da query
        # (ex: {{ ref('stg_model') }}) antes de entregar ao SnapshotRunner.
        # Nota: usamos um ModelContext temporário para registrar dependências
        # sem precisar de um nome de modelo real.
        context = ModelContext(snapshot_name, self.target_schema)
        try:
            template = self.env.from_string(source_sql)
            compiled_sql = template.render(
                ref=context.ref,
                config=context.config,
                target_schema=self.target_schema,
            ).strip()
        except Exception as e:
            logger.error(f"Erro ao compilar SQL do snapshot '{snapshot_name}': {e}")
            raise

        return {
            "snapshot_name": snapshot_name,
            "config": config_dict,
            "compiled_sql": compiled_sql,
        }