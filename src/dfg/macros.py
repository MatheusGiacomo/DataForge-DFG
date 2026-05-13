# src/dfg/macros.py
"""
Sistema de Macros Reutilizáveis do DataForge (v0.4.0).

Macros são blocos Jinja2 reutilizáveis definidos em arquivos `.sql`
dentro da pasta `macros/`. Uma vez carregadas, ficam disponíveis em
todos os modelos SQL e arquivos de análise sem necessidade de import.

Formato dos arquivos de macro:

    -- macros/utils.sql
    {% macro current_timestamp() %}
        CAST(NOW() AS TIMESTAMP)
    {% endmacro %}

    {% macro cents_to_dollars(column) %}
        ROUND({{ column }} / 100.0, 2)
    {% endmacro %}

Uso em qualquer modelo SQL:

    -- models/fct_orders.sql
    {{ config(materialized='table') }}

    SELECT
        order_id,
        {{ cents_to_dollars('amount_cents') }} AS amount_usd,
        {{ current_timestamp() }}              AS processed_at
    FROM {{ ref('stg_orders') }}

Implementação:

O Jinja2 representa cada bloco {% macro %} como um objeto ``Callable``
que pode ser extraído de um módulo de template via ``template.make_module()``.
O MacroLoader carrega todos os arquivos de macro, extrai os callables e os
disponibiliza como globais no ambiente Jinja do compilador.

Isso elimina a necessidade de ``{% from 'arquivo.sql' import macro %}``,
tornando as macros verdadeiramente "globais" no escopo do projeto.
"""
from __future__ import annotations

import os

import jinja2

from dfg.logging import logger


class MacroLoader:
    """
    Carrega e compila macros Jinja2 a partir de arquivos `.sql` em `macros/`.

    Parâmetros
    ----------
    macros_dir : str
        Caminho para a pasta `macros/` do projeto.

    Atributos Públicos
    ------------------
    globals : dict[str, Callable]
        Dicionário {nome_da_macro: callable} pronto para ser injetado
        como globals no ``jinja2.Environment``.
    """

    def __init__(self, macros_dir: str):
        self.macros_dir = macros_dir
        self.globals: dict = {}
        self._loaded_files: list[str] = []
        self._errors: list[str] = []

    # ------------------------------------------------------------------
    # Carregamento
    # ------------------------------------------------------------------

    def load(self, env: jinja2.Environment) -> "MacroLoader":
        """
        Escaneia `macros_dir`, carrega todos os arquivos `.sql` e extrai
        os callables de macro usando ``jinja2.Template.make_module()``.

        Parâmetros
        ----------
        env : jinja2.Environment
            O mesmo ambiente Jinja2 usado pelo SQLCompiler. As macros são
            compiladas dentro deste ambiente para garantir consistência de
            configuração (trim_blocks, lstrip_blocks, etc.).

        Retorna
        -------
        self : MacroLoader
            Suporte a encadeamento: ``MacroLoader(dir).load(env)``.
        """
        if not os.path.isdir(self.macros_dir):
            return self  # Pasta inexistente = sem macros, não é erro

        sql_files = sorted(
            f for f in os.listdir(self.macros_dir) if f.endswith(".sql")
        )

        for filename in sql_files:
            filepath = os.path.join(self.macros_dir, filename)
            self._load_file(env, filename, filepath)

        if self._loaded_files:
            logger.debug(
                f"Macros carregadas: {len(self.globals)} macro(s) "
                f"de {len(self._loaded_files)} arquivo(s)."
            )

        return self

    def _load_file(self, env: jinja2.Environment, filename: str, filepath: str) -> None:
        """
        Carrega um único arquivo de macro e extrai seus callables.

        ``template.make_module()`` executa o template e retorna um
        ``TemplateModule``, que expõe os macros como atributos Python
        chamáveis. Esse é o mecanismo oficial do Jinja2 para compartilhar
        macros entre templates sem importação explícita.
        """
        try:
            with open(filepath, encoding="utf-8") as f:
                content = f.read()

            if not content.strip():
                return

            template = env.from_string(content)
            module = template.make_module()

            extracted: list[str] = []
            for attr_name in dir(module):
                if attr_name.startswith("_"):
                    continue

                attr = getattr(module, attr_name)

                # Apenas macros Jinja2 são expostas (type: jinja2.runtime.Macro)
                if isinstance(attr, jinja2.runtime.Macro):
                    if attr_name in self.globals:
                        logger.warn(
                            f"Macro '{attr_name}' definida em '{filename}' "
                            f"sobrescreve uma definição anterior. "
                            f"Verifique se há duplicatas nos seus arquivos de macro."
                        )
                    self.globals[attr_name] = attr
                    extracted.append(attr_name)

            if extracted:
                self._loaded_files.append(filename)
                logger.debug(f"  {filename}: {extracted}")

        except jinja2.exceptions.TemplateSyntaxError as e:
            msg = f"Erro de sintaxe em macro '{filename}' (linha {e.lineno}): {e.message}"
            logger.error(msg)
            self._errors.append(msg)
        except Exception as e:
            msg = f"Falha ao carregar macro '{filename}': {e}"
            logger.error(msg)
            self._errors.append(msg)

    # ------------------------------------------------------------------
    # Diagnóstico
    # ------------------------------------------------------------------

    @property
    def macro_names(self) -> list[str]:
        """Lista os nomes de todas as macros carregadas, em ordem alfabética."""
        return sorted(self.globals.keys())

    @property
    def has_errors(self) -> bool:
        return bool(self._errors)

    def __repr__(self) -> str:
        return (
            f"MacroLoader("
            f"macros={self.macro_names}, "
            f"files={self._loaded_files})"
        )