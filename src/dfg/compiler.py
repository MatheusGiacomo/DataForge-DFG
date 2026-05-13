# src/dfg/compiler.py
"""
Compilador Jinja2 para modelos SQL e snapshots do DataForge.

Responsabilidades:
- Renderizar templates Jinja2 em SQL puro
- Extrair metadados de configuração ({{ config(...) }})
- Rastrear dependências ({{ ref('model') }})
- Fazer parsing de blocos {% snapshot %} ... {% endsnapshot %}
- Expor variáveis de runtime via {{ var('nome', default) }} (v0.3.0)
- Injetar macros globais carregadas de macros/*.sql (v0.4.0)
- Compilar arquivos de análise de analysis/*.sql (v0.4.0)
"""
import re

import jinja2

from dfg.logging import logger


class ModelContext:
    """
    Contexto de execução injetado nos templates Jinja2.

    Expõe ref(), config() e var() que o autor do modelo pode usar
    diretamente no SQL, sem importações extras.
    """

    def __init__(self, model_name: str, target_schema: str, runtime_vars: dict | None = None):
        self.model_name = model_name
        self.target_schema = target_schema
        self.model_config: dict = {}
        self.dependencies: list[str] = []
        self._vars: dict = runtime_vars or {}

    def ref(self, referenced_model: str) -> str:
        """Macro {{ ref('nome') }}: registra dependência e retorna o nome da tabela."""
        self.dependencies.append(referenced_model)
        return referenced_model

    def config(self, **kwargs) -> str:
        """Macro {{ config(...) }}: armazena configuração e retorna string vazia."""
        self.model_config.update(kwargs)
        return ""

    def var(self, key: str, default=None):
        """
        Macro {{ var('nome', default) }}: acessa variáveis de runtime do --var.

        Uso no modelo SQL:
            WHERE data >= '{{ var("data_inicio", "2024-01-01") }}'
        """
        return self._vars.get(key, default)


class SQLCompiler:
    """
    Motor de compilação Jinja2 para o DataForge.

    Parâmetros
    ----------
    target_schema : str
        Schema de destino disponibilizado como variável nos templates.
    runtime_vars : dict | None
        Variáveis passadas via --var no CLI. Acessíveis via {{ var('nome') }}.
    macro_globals : dict | None
        Macros carregadas pelo MacroLoader. Injetadas como globals no env.
    """

    def __init__(
        self,
        target_schema: str = "public",
        runtime_vars: dict | None = None,
        macro_globals: dict | None = None,
    ):
        self.target_schema = target_schema
        self.runtime_vars = runtime_vars or {}

        self.env = jinja2.Environment(
            loader=jinja2.BaseLoader(),
            trim_blocks=True,
            lstrip_blocks=True,
            # StrictUndefined: falha em variáveis não definidas para evitar
            # bugs silenciosos por typos nos templates.
            undefined=jinja2.StrictUndefined,
        )

        # Injeta macros como globals do ambiente Jinja2.
        # Isso as torna disponíveis em TODOS os templates sem import explícito.
        if macro_globals:
            self.env.globals.update(macro_globals)

    def _make_context(self, model_name: str) -> ModelContext:
        return ModelContext(
            model_name=model_name,
            target_schema=self.target_schema,
            runtime_vars=self.runtime_vars,
        )

    def _render(self, template_str: str, context: ModelContext) -> str:
        """Renderiza um template com o contexto fornecido."""
        template = self.env.from_string(template_str)
        return template.render(
            ref=context.ref,
            config=context.config,
            var=context.var,
            target_schema=self.target_schema,
        )

    # ------------------------------------------------------------------
    # API Pública — Modelos SQL
    # ------------------------------------------------------------------

    def compile(self, sql_raw: str, model_name: str) -> dict:
        """
        Compila um arquivo SQL com Jinja2 e extrai seus metadados.

        Retorna dict com: sql, depends_on, config.
        """
        context = self._make_context(model_name)
        try:
            sql_compiled = self._render(sql_raw, context)
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
        """Renderização simples sem contexto de modelo (usado internamente)."""
        context = self._make_context("__render__")
        try:
            return self._render(sql_raw, context).strip()
        except jinja2.exceptions.TemplateSyntaxError as e:
            logger.error(f"Erro de sintaxe Jinja ao renderizar SQL (linha {e.lineno}): {e.message}")
            raise
        except Exception as e:
            logger.error(f"Erro inesperado ao renderizar SQL: {e}")
            raise

    # ------------------------------------------------------------------
    # API Pública — Arquivos de Análise (v0.4.0)
    # ------------------------------------------------------------------

    def compile_analysis(self, sql_raw: str, analysis_name: str) -> str:
        """
        Compila um arquivo de análise ad-hoc (analysis/*.sql).

        Análises têm acesso a ref(), var() e macros — mas não a config().
        O resultado é apenas SQL compilado (sem materialização no banco).

        Parâmetros
        ----------
        sql_raw : str
            Conteúdo bruto do arquivo .sql.
        analysis_name : str
            Nome do arquivo de análise (para mensagens de erro).

        Retorna
        -------
        str
            SQL puro, pronto para execução manual ou visualização.
        """
        context = self._make_context(analysis_name)
        try:
            return self._render(sql_raw, context).strip()
        except jinja2.exceptions.TemplateSyntaxError as e:
            logger.error(
                f"Erro de sintaxe Jinja na análise '{analysis_name}' "
                f"(linha {e.lineno}): {e.message}"
            )
            raise
        except Exception as e:
            logger.error(f"Erro inesperado ao compilar análise '{analysis_name}': {e}")
            raise

    # ------------------------------------------------------------------
    # API Pública — Snapshots
    # ------------------------------------------------------------------

    def parse_snapshot(self, raw_sql: str) -> dict | None:
        """
        Analisa um arquivo de snapshot:

            {% snapshot nome %}
            {{ config(unique_key='id', updated_at='updated_at') }}
            SELECT * FROM {{ ref('stg_model') }}
            {% endsnapshot %}

        Retorna None se não encontrar um bloco snapshot válido.
        """
        snapshot_pattern = re.compile(
            r"\{%\s*snapshot\s+(\w+)\s*%\}(.*?)\{%\s*endsnapshot\s*%\}",
            re.DOTALL,
        )
        match = snapshot_pattern.search(raw_sql)
        if not match:
            return None

        snapshot_name = match.group(1)
        inner_content = match.group(2)

        config_pattern = re.compile(r"\{\{\s*config\((.*?)\)\s*\}\}", re.DOTALL)
        config_match = config_pattern.search(inner_content)

        config_dict: dict = {}
        if config_match:
            config_str = config_match.group(1)
            for key, value in re.findall(r"(\w+)\s*=\s*['\"]([^'\"]+)['\"]", config_str):
                config_dict[key] = value
            source_sql = config_pattern.sub("", inner_content).strip()
        else:
            source_sql = inner_content.strip()

        context = self._make_context(snapshot_name)
        try:
            compiled_sql = self._render(source_sql, context).strip()
        except Exception as e:
            logger.error(f"Erro ao compilar SQL do snapshot '{snapshot_name}': {e}")
            raise

        return {
            "snapshot_name": snapshot_name,
            "config": config_dict,
            "compiled_sql": compiled_sql,
        }