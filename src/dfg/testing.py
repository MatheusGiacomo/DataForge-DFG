# src/dfg/testing.py
"""
Motor de Testes de Qualidade de Dados do DataForge (v0.3.0).

Implementa o padrão Strategy: cada tipo de teste é uma classe que sabe
construir seu próprio SQL de verificação. O TestRunner orquestra a
execução e produz um relatório estruturado.

Testes por Coluna:
    not_null         — sem valores NULL
    unique           — sem valores duplicados
    accepted_values  — apenas valores de uma lista permitida
    relationships    — integridade referencial entre tabelas
    not_negative     — valores numéricos >= 0
    between          — valores dentro de um intervalo [min, max]
    custom_sql       — SQL arbitrário fornecido pelo usuário

Testes por Modelo:
    row_count_between — total de linhas dentro de um intervalo
    freshness         — dado não é mais antigo do que X horas
    no_duplicate_rows — sem linhas completamente duplicadas

Formato no schema.yml:
    models:
      - name: stg_pedidos
        tests:                          # testes no nível do modelo
          - row_count_between:
              min: 1000
              max: 5000000
          - freshness:
              field: criado_em
              max_age_hours: 25
          - no_duplicate_rows:
              columns: [pedido_id, criado_em]
        columns:
          - name: status
            tests:
              - not_null
              - unique
              - accepted_values:
                  values: [pending, complete, cancelled]
          - name: valor_total
            tests:
              - not_null
              - not_negative
              - between:
                  min_value: 0.01
                  max_value: 999999.99
          - name: cliente_id
            tests:
              - relationships:
                  to: stg_clientes
                  field: id
          - name: descricao
            tests:
              - custom_sql:
                  sql: >
                    SELECT COUNT(*) FROM {model}
                    WHERE {column} IS NOT NULL
                    AND LENGTH({column}) < 3
"""
from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone


# =============================================================================
# Resultado de um único teste
# =============================================================================


@dataclass
class TestResult:
    """Resultado de execução de um único teste."""

    test_name: str
    model: str
    column: str | None       # None para testes de nível de modelo
    status: str              # "pass" | "fail" | "error"
    message: str = ""
    rows_affected: int = 0   # linhas que violam o teste (para falhas)


@dataclass
class TestReport:
    """Relatório completo de uma execução de `dfg test`."""

    results: list[TestResult] = field(default_factory=list)

    @property
    def passed(self) -> int:
        return sum(1 for r in self.results if r.status == "pass")

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if r.status == "fail")

    @property
    def errors(self) -> int:
        return sum(1 for r in self.results if r.status == "error")

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def has_failures(self) -> bool:
        return self.failed > 0 or self.errors > 0


# =============================================================================
# Interface abstrata para testes por coluna
# =============================================================================


class ColumnTest(ABC):
    """
    Interface base para testes que operam em uma coluna específica.

    O método `build_sql` retorna uma query SQL que deve retornar
    o número de linhas que VIOLAM o teste. Zero = passou.
    """

    @abstractmethod
    def build_sql(self, model: str, column: str) -> str:
        """Retorna SQL que conta as violações do teste. Zero significa passou."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Nome do teste para exibição no relatório."""


# =============================================================================
# Interface abstrata para testes por modelo
# =============================================================================


class ModelTest(ABC):
    """
    Interface base para testes que operam no modelo inteiro (sem coluna).

    O método `build_sql` retorna uma query que retorna exatamente uma linha
    com uma coluna: o valor que será avaliado pelo `check()`.
    """

    @abstractmethod
    def build_sql(self, model: str) -> str:
        """Retorna SQL que produz o valor a ser avaliado."""

    @abstractmethod
    def check(self, value) -> tuple[bool, str]:
        """
        Avalia o valor retornado pelo SQL.

        Retorna
        -------
        tuple[bool, str]
            (passou, mensagem_de_falha)
        """

    @property
    @abstractmethod
    def name(self) -> str:
        """Nome do teste para exibição no relatório."""


# =============================================================================
# Testes por Coluna
# =============================================================================


class NotNullTest(ColumnTest):
    name = "not_null"

    def build_sql(self, model: str, column: str) -> str:
        return f"SELECT COUNT(*) FROM {model} WHERE {column} IS NULL"


class UniqueTest(ColumnTest):
    name = "unique"

    def build_sql(self, model: str, column: str) -> str:
        return (
            f"SELECT COUNT(*) FROM ("
            f"  SELECT {column} FROM {model}"
            f"  GROUP BY {column} HAVING COUNT(*) > 1"
            f") AS __dfg_unique_violations"
        )


class AcceptedValuesTest(ColumnTest):
    """
    Verifica se todos os valores de uma coluna pertencem a uma lista predefinida.

    Configuração no schema.yml:
        - accepted_values:
            values: [pending, complete, cancelled]
            quote_values: true   # padrão: true (trata como strings)
    """

    def __init__(self, values: list, quote_values: bool = True):
        if not values:
            raise ValueError("AcceptedValuesTest requer ao menos um valor em 'values'.")
        self._values = values
        self._quote = quote_values

    @property
    def name(self) -> str:
        return "accepted_values"

    def build_sql(self, model: str, column: str) -> str:
        if self._quote:
            escaped = [str(v).replace("'", "''") for v in self._values]
            in_list = ", ".join(f"'{v}'" for v in escaped)
        else:
            in_list = ", ".join(str(v) for v in self._values)

        return (
            f"SELECT COUNT(*) FROM {model}"
            f" WHERE {column} IS NOT NULL"
            f" AND {column} NOT IN ({in_list})"
        )


class RelationshipsTest(ColumnTest):
    """
    Verifica integridade referencial: todos os valores da coluna existem
    como chave na tabela de referência.

    Configuração no schema.yml:
        - relationships:
            to: stg_clientes    # tabela de referência
            field: id           # coluna na tabela de referência
    """

    def __init__(self, to: str, field: str):
        self._to = to
        self._field = field

    @property
    def name(self) -> str:
        return f"relationships(→ {self._to}.{self._field})"

    def build_sql(self, model: str, column: str) -> str:
        return (
            f"SELECT COUNT(*) FROM {model} AS __src"
            f" WHERE __src.{column} IS NOT NULL"
            f" AND __src.{column} NOT IN ("
            f"   SELECT {self._field} FROM {self._to}"
            f" )"
        )


class NotNegativeTest(ColumnTest):
    """
    Verifica se todos os valores numéricos são >= 0.

    Configuração no schema.yml:
        - not_negative
    """

    name = "not_negative"

    def build_sql(self, model: str, column: str) -> str:
        return (
            f"SELECT COUNT(*) FROM {model}"
            f" WHERE {column} IS NOT NULL AND {column} < 0"
        )


class BetweenTest(ColumnTest):
    """
    Verifica se todos os valores estão dentro do intervalo [min_value, max_value].

    Configuração no schema.yml:
        - between:
            min_value: 0.01
            max_value: 999999.99
    """

    def __init__(self, min_value, max_value):
        self._min = min_value
        self._max = max_value

    @property
    def name(self) -> str:
        return f"between({self._min}, {self._max})"

    def build_sql(self, model: str, column: str) -> str:
        return (
            f"SELECT COUNT(*) FROM {model}"
            f" WHERE {column} IS NOT NULL"
            f" AND ({column} < {self._min} OR {column} > {self._max})"
        )


class CustomSQLTest(ColumnTest):
    """
    Executa um SQL arbitrário fornecido pelo usuário.

    O SQL deve retornar uma única linha com uma única coluna numérica.
    Zero = passou. Qualquer valor > 0 = falhou.

    Use os placeholders {model} e {column} no SQL:

    Configuração no schema.yml:
        - custom_sql:
            sql: >
              SELECT COUNT(*) FROM {model}
              WHERE {column} IS NOT NULL
              AND LENGTH({column}) < 3
    """

    def __init__(self, sql: str):
        if not sql or not sql.strip():
            raise ValueError("CustomSQLTest requer uma query SQL não vazia em 'sql'.")
        self._sql_template = sql.strip()

    @property
    def name(self) -> str:
        # Extrai as primeiras palavras para identificação no log
        preview = " ".join(self._sql_template.split()[:6])
        return f"custom_sql({preview}...)"

    def build_sql(self, model: str, column: str) -> str:
        return self._sql_template.format(model=model, column=column)


# =============================================================================
# Testes por Modelo
# =============================================================================


class RowCountBetweenTest(ModelTest):
    """
    Verifica se o número total de linhas está dentro de um intervalo.

    Configuração no schema.yml:
        - row_count_between:
            min: 1000
            max: 5000000
    """

    def __init__(self, min: int = 0, max: int | None = None):  # noqa: A002
        self._min = min
        self._max = max

    @property
    def name(self) -> str:
        return f"row_count_between({self._min}, {self._max or '∞'})"

    def build_sql(self, model: str) -> str:
        return f"SELECT COUNT(*) FROM {model}"

    def check(self, value) -> tuple[bool, str]:
        count = int(value)
        if count < self._min:
            return False, f"A tabela tem {count} linha(s), mas o mínimo esperado é {self._min}."
        if self._max is not None and count > self._max:
            return False, f"A tabela tem {count} linha(s), mas o máximo esperado é {self._max}."
        return True, ""


class FreshnessTest(ModelTest):
    """
    Verifica se a tabela foi atualizada recentemente.

    Falha se o registro mais recente for mais antigo do que `max_age_hours` horas.

    Configuração no schema.yml:
        - freshness:
            field: criado_em        # coluna de timestamp
            max_age_hours: 25       # tolerância máxima
    """

    def __init__(self, field: str, max_age_hours: float):
        self._field = field
        self._max_age_hours = max_age_hours

    @property
    def name(self) -> str:
        return f"freshness({self._field} ≤ {self._max_age_hours}h)"

    def build_sql(self, model: str) -> str:
        return f"SELECT MAX({self._field}) FROM {model}"

    def check(self, value) -> tuple[bool, str]:
        if value is None:
            return False, f"A tabela está vazia — sem registros em '{self._field}'."

        # Converte para datetime UTC para comparação
        if isinstance(value, str):
            # Tenta parsear strings de timestamp comuns
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
                try:
                    value = datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
                    break
                except ValueError:
                    continue
            else:
                return False, f"Não foi possível parsear o valor de timestamp: '{value}'."

        if isinstance(value, datetime) and value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)

        now = datetime.now(tz=timezone.utc)
        age_hours = (now - value).total_seconds() / 3600

        if age_hours > self._max_age_hours:
            return (
                False,
                f"O dado mais recente tem {age_hours:.1f}h de idade "
                f"(máximo permitido: {self._max_age_hours}h). "
                f"Último registro em: {value.strftime('%Y-%m-%d %H:%M:%S UTC')}.",
            )
        return True, ""


class NoDuplicateRowsTest(ModelTest):
    """
    Verifica se não há linhas completamente duplicadas nas colunas especificadas.

    Configuração no schema.yml:
        - no_duplicate_rows:
            columns: [pedido_id, criado_em]   # colunas que definem unicidade
    """

    def __init__(self, columns: list[str]):
        if not columns:
            raise ValueError("NoDuplicateRowsTest requer ao menos uma coluna em 'columns'.")
        self._columns = columns

    @property
    def name(self) -> str:
        return f"no_duplicate_rows({', '.join(self._columns)})"

    def build_sql(self, model: str) -> str:
        col_list = ", ".join(self._columns)
        return (
            f"SELECT COUNT(*) FROM ("
            f"  SELECT {col_list} FROM {model}"
            f"  GROUP BY {col_list} HAVING COUNT(*) > 1"
            f") AS __dfg_dup_rows"
        )

    def check(self, value) -> tuple[bool, str]:
        count = int(value)
        if count > 0:
            col_str = ", ".join(self._columns)
            return (
                False,
                f"Encontrado(s) {count} grupo(s) de linhas duplicadas "
                f"nas colunas ({col_str}).",
            )
        return True, ""


# =============================================================================
# Factories: dict → instância de teste
# =============================================================================


def _build_column_test(test_config: str | dict) -> ColumnTest:
    """
    Constrói uma instância de ColumnTest a partir de configuração YAML.

    Aceita tanto string simples ("not_null") quanto dicionário
    ({"accepted_values": {"values": [...]}}).
    """
    if isinstance(test_config, str):
        name = test_config
        params: dict = {}
    elif isinstance(test_config, dict):
        if len(test_config) != 1:
            raise ValueError(
                f"Configuração de teste inválida: {test_config}. "
                f"Cada teste deve ter exatamente uma chave."
            )
        name, params = next(iter(test_config.items()))
        params = params or {}
    else:
        raise TypeError(f"Configuração de teste inválida: {type(test_config)}")

    builders: dict[str, type] = {
        "not_null": lambda p: NotNullTest(),
        "unique": lambda p: UniqueTest(),
        "accepted_values": lambda p: AcceptedValuesTest(
            values=p["values"],
            quote_values=p.get("quote_values", True),
        ),
        "relationships": lambda p: RelationshipsTest(to=p["to"], field=p["field"]),
        "not_negative": lambda p: NotNegativeTest(),
        "between": lambda p: BetweenTest(min_value=p["min_value"], max_value=p["max_value"]),
        "custom_sql": lambda p: CustomSQLTest(sql=p["sql"]),
    }

    builder = builders.get(name)
    if not builder:
        supported = ", ".join(sorted(builders.keys()))
        raise ValueError(
            f"Teste de coluna '{name}' não reconhecido. "
            f"Testes disponíveis: {supported}."
        )

    return builder(params)


def _build_model_test(test_config: dict) -> ModelTest:
    """Constrói uma instância de ModelTest a partir de configuração YAML."""
    if not isinstance(test_config, dict) or len(test_config) != 1:
        raise ValueError(
            f"Testes de modelo devem ser dicionários com uma chave: {test_config}"
        )

    name, params = next(iter(test_config.items()))
    params = params or {}

    builders: dict[str, type] = {
        "row_count_between": lambda p: RowCountBetweenTest(
            min=p.get("min", 0),
            max=p.get("max"),
        ),
        "freshness": lambda p: FreshnessTest(
            field=p["field"],
            max_age_hours=float(p["max_age_hours"]),
        ),
        "no_duplicate_rows": lambda p: NoDuplicateRowsTest(
            columns=p.get("columns", []),
        ),
    }

    builder = builders.get(name)
    if not builder:
        supported = ", ".join(sorted(builders.keys()))
        raise ValueError(
            f"Teste de modelo '{name}' não reconhecido. "
            f"Testes disponíveis: {supported}."
        )

    return builder(params)


# =============================================================================
# TestRunner — orquestrador principal
# =============================================================================


class TestRunner:
    """
    Executa todos os contratos de dados de um projeto DataForge.

    Parâmetros
    ----------
    engine : DFGEngine
        Instância do motor principal, usada para obter o adapter e
        o registry de modelos.
    select : set[str] | None
        Subconjunto de modelos a testar. None = todos os modelos.
    """

    def __init__(self, engine, select: set[str] | None = None):
        self.engine = engine
        self.select = select

    def run(self) -> TestReport:
        """
        Executa todos os testes configurados no schema.yml e retorna
        um `TestReport` com os resultados detalhados.
        """
        from dfg.logging import logger

        self.engine.discover_models()
        adapter = self.engine._get_thread_safe_adapter()
        report = TestReport()

        logger.info("--- Iniciando Validação de Contratos ---")

        try:
            for model_name, model_info in self.engine.models_registry.items():
                if self.select and model_name not in self.select:
                    continue

                contract = model_info.get("config", {}).get("contract", {})
                model_tests_cfg = model_info.get("config", {}).get("model_tests", [])

                has_contract = contract or model_tests_cfg
                if not has_contract:
                    logger.warn(f"Modelo '{model_name}': sem contrato definido. Pulando.")
                    continue

                logger.forge(f"Testando '{model_name}'...")

                # Verifica se a tabela existe e tem dados
                try:
                    row_result = adapter.execute(f"SELECT COUNT(*) FROM {model_name}")
                    row_count = row_result[0][0] if row_result else 0
                    if row_count == 0:
                        logger.warn(f"  [AVISO] Tabela '{model_name}' está vazia.")
                except Exception as e:
                    logger.error(f"  Erro ao acessar '{model_name}': {e}")
                    report.results.append(TestResult(
                        test_name="table_access",
                        model=model_name,
                        column=None,
                        status="error",
                        message=str(e),
                    ))
                    continue

                # --- Testes por coluna ---
                for column, col_tests in contract.items():
                    for test_cfg in col_tests:
                        result = self._run_column_test(
                            adapter, model_name, column, test_cfg
                        )
                        report.results.append(result)
                        self._log_result(result)

                # --- Testes por modelo ---
                for model_test_cfg in model_tests_cfg:
                    result = self._run_model_test(adapter, model_name, model_test_cfg)
                    report.results.append(result)
                    self._log_result(result)

        finally:
            adapter.close()

        self._log_summary(report)
        return report

    # ------------------------------------------------------------------
    # Execução individual
    # ------------------------------------------------------------------

    def _run_column_test(
        self, adapter, model: str, column: str, test_cfg
    ) -> TestResult:
        """Executa um teste de coluna e retorna o resultado."""
        try:
            test = _build_column_test(test_cfg)
        except (ValueError, TypeError) as e:
            return TestResult(
                test_name=str(test_cfg),
                model=model,
                column=column,
                status="error",
                message=f"Configuração inválida: {e}",
            )

        try:
            sql = test.build_sql(model, column)
            result = adapter.execute(sql)
            violations = result[0][0] if result else 0

            if violations > 0:
                return TestResult(
                    test_name=test.name,
                    model=model,
                    column=column,
                    status="fail",
                    rows_affected=int(violations),
                    message=(
                        f"'{model}.{column}' violou o teste '{test.name}': "
                        f"{violations} linha(s) inválida(s)."
                    ),
                )

            return TestResult(
                test_name=test.name, model=model, column=column, status="pass"
            )

        except Exception as e:
            return TestResult(
                test_name=test.name,
                model=model,
                column=column,
                status="error",
                message=f"Erro ao executar SQL do teste: {e}",
            )

    def _run_model_test(self, adapter, model: str, test_cfg) -> TestResult:
        """Executa um teste de modelo e retorna o resultado."""
        try:
            test = _build_model_test(test_cfg)
        except (ValueError, TypeError) as e:
            test_name = list(test_cfg.keys())[0] if isinstance(test_cfg, dict) else str(test_cfg)
            return TestResult(
                test_name=test_name,
                model=model,
                column=None,
                status="error",
                message=f"Configuração inválida: {e}",
            )

        try:
            sql = test.build_sql(model)
            result = adapter.execute(sql)
            value = result[0][0] if result else None

            passed, message = test.check(value)

            if not passed:
                return TestResult(
                    test_name=test.name,
                    model=model,
                    column=None,
                    status="fail",
                    message=f"'{model}' violou o teste '{test.name}': {message}",
                )

            return TestResult(test_name=test.name, model=model, column=None, status="pass")

        except Exception as e:
            return TestResult(
                test_name=test.name,
                model=model,
                column=None,
                status="error",
                message=f"Erro ao executar SQL do teste: {e}",
            )

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

    @staticmethod
    def _log_result(result: TestResult) -> None:
        from dfg.logging import logger

        scope = f"{result.model}.{result.column}" if result.column else result.model

        if result.status == "pass":
            logger.success(f"    ✓ [{result.test_name}] {scope}")
        elif result.status == "fail":
            rows_info = f" ({result.rows_affected} linha(s))" if result.rows_affected else ""
            logger.error(f"    ✗ [FALHA] [{result.test_name}] {scope}{rows_info}: {result.message}")
        else:
            logger.error(f"    ✗ [ERRO] [{result.test_name}] {scope}: {result.message}")

    @staticmethod
    def _log_summary(report: TestReport) -> None:
        from dfg.logging import logger

        logger.info("─" * 60)
        if report.has_failures:
            logger.error(
                f"Resultado: {report.passed} passou | "
                f"{report.failed} falhou | "
                f"{report.errors} erro(s) | "
                f"{report.total} total"
            )
        else:
            logger.success(
                f"Todos os {report.total} testes passaram com sucesso!"
            )


# =============================================================================
# Utilitário: parser do schema.yml para extrair model_tests
# =============================================================================


def extract_model_tests_from_yaml(model_meta: dict) -> list:
    """
    Extrai a lista de testes no nível de modelo do bloco YAML de metadados.

    Separa os testes de modelo dos testes de coluna para que ambos
    possam ser armazenados no config do modelo de forma independente.

    Parâmetros
    ----------
    model_meta : dict
        Bloco de metadados de um modelo do schema.yml.

    Retorna
    -------
    list
        Lista de dicionários de configuração de testes de modelo.
    """
    return model_meta.get("tests", [])