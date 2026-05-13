# src/dfg/engine.py
"""
Motor principal do DataForge (DFGEngine).

Responsável por:
- Carregar e validar as configurações do projeto (dfg_project.toml + profiles.toml)
- Descobrir e compilar modelos SQL e Python (discover_models)
- Executar o DAG com paralelismo real via ThreadPoolExecutor
- Orquestrar testes, compilação, snapshots e seeds
- Carregar macros de macros/*.sql (v0.4.0)
- Compilar análises de analysis/*.sql (v0.4.0)
- Executar hooks pre_hook/post_hook por modelo (v0.4.0)
- Suporte a --dry-run (v0.4.0)
"""
import contextlib
import graphlib
import importlib.util
import os
import shutil
import sys
import threading
import time
import tomllib
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

import yaml

from dfg.adapters.factory import AdapterFactory
from dfg.artifacts import ArtifactManager
from dfg.compiler import SQLCompiler
from dfg.logging import logger
from dfg.macros import MacroLoader
from dfg.selector import resolve_selection
from dfg.snapshot import SnapshotRunner
from dfg.sources._env import resolve as _resolve_env
from dfg.state import StateManager
from dfg.testing import TestRunner, extract_model_tests_from_yaml


class DFGEngine:
    """
    Coração do DataForge.

    Instancie passando o diretório raiz do projeto. O motor lê as
    configurações, inicializa os serviços internos e fica pronto para
    executar qualquer comando (run, test, compile, snapshot, …).
    """

    def __init__(
        self,
        project_dir: str,
        override_target: str | None = None,
        runtime_vars: dict | None = None,
    ):
        self.project_dir = os.path.abspath(project_dir)
        self.models_dir    = os.path.join(self.project_dir, "models")
        self.snapshots_dir = os.path.join(self.project_dir, "snapshots")
        self.seeds_dir     = os.path.join(self.project_dir, "seeds")
        self.macros_dir    = os.path.join(self.project_dir, "macros")
        self.analysis_dir  = os.path.join(self.project_dir, "analysis")

        # Logger antes de qualquer coisa
        logger.setup(self.project_dir)

        # Overrides de runtime (--target, --var)
        self._override_target = override_target
        self.runtime_vars: dict = runtime_vars or {}

        # Configurações do projeto
        self.config = self._load_config()
        self.artifact_manager = ArtifactManager(self.project_dir)
        self.state_manager    = StateManager(self.project_dir)

        # v0.4.0: carrega macros antes de criar o compilador para que
        # os globals Jinja2 estejam disponíveis desde a primeira compilação.
        target_name   = self.config["project"]["target"]
        target_schema = self.config["targets"][target_name].get("schema", "public")

        self._macro_loader = MacroLoader(self.macros_dir).load(
            # Cria um env temporário apenas para extração de macros.
            # O compilador criará seu próprio env depois com os globals injetados.
            __import__("jinja2").Environment(
                loader=__import__("jinja2").BaseLoader(),
                trim_blocks=True,
                lstrip_blocks=True,
            )
        )

        self.compiler = SQLCompiler(
            target_schema=target_schema,
            runtime_vars=self.runtime_vars,
            macro_globals=self._macro_loader.globals,
        )

        self.snapshot_runner = SnapshotRunner(self)

        # Registros de estado do DAG (preenchidos por discover_models)
        self.models_registry: dict = {}
        self.dependencies_map: dict = {}

        # Locks para thread-safety
        self.print_lock = threading.Lock()
        self.cache_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Configuração
    # ------------------------------------------------------------------

    def _load_config(self) -> dict:
        """
        Lê dfg_project.toml e profiles.toml e monta o dicionário de config.
        Levanta FileNotFoundError ou ValueError em caso de configuração inválida.
        """
        project_toml  = os.path.join(self.project_dir, "dfg_project.toml")
        profiles_toml = os.path.join(self.project_dir, "profiles.toml")

        if not os.path.exists(project_toml):
            raise FileNotFoundError(
                f"Arquivo 'dfg_project.toml' não encontrado em '{self.project_dir}'. "
                f"Execute 'dfg init' para criar a estrutura do projeto."
            )
        if not os.path.exists(profiles_toml):
            raise FileNotFoundError(
                f"Arquivo 'profiles.toml' não encontrado em '{self.project_dir}'."
            )

        with open(project_toml, "rb") as f:
            config = tomllib.load(f)
        with open(profiles_toml, "rb") as f:
            profiles = tomllib.load(f)

        profile_name = config["project"].get("profile")
        target_name  = self._override_target or config["project"].get("target", "dev")

        if not profile_name:
            raise ValueError("Campo 'profile' ausente em [project] no dfg_project.toml.")

        try:
            credentials = profiles[profile_name]["outputs"][target_name]
        except KeyError as err:
            raise ValueError(
                f"Target '{target_name}' não encontrado no profile '{profile_name}' "
                f"do profiles.toml."
            ) from err

        credentials = _resolve_env(credentials)
        config["targets"] = {target_name: credentials}
        return config

    # ------------------------------------------------------------------
    # Adaptador Thread-Safe
    # ------------------------------------------------------------------

    def _get_thread_safe_adapter(self):
        """
        Cria e conecta um adaptador de banco independente para a thread atual.
        Cada worker thread precisa de sua própria conexão.
        """
        target_name   = self.config["project"]["target"]
        target_config = self.config["targets"][target_name]
        adapter = AdapterFactory.get_adapter(target_config["type"])
        adapter.connect(target_config)
        return adapter

    # ------------------------------------------------------------------
    # Descoberta de Modelos (Fase 1 + Fase 2)
    # ------------------------------------------------------------------

    def discover_models(self) -> None:
        """
        Popula models_registry e dependencies_map.

        Fase 1: mapeia .sql e .py, compilando templates Jinja.
        Fase 2: aplica metadados e contratos dos arquivos .yml.
        Idempotente: chamadas subsequentes são ignoradas.
        """
        if self.models_registry:
            return

        if not os.path.exists(self.models_dir):
            with self.print_lock:
                logger.warn(f"Diretório de modelos não encontrado: '{self.models_dir}'.")
            return

        with self.print_lock:
            if self._macro_loader.macro_names:
                logger.forge(
                    f"Macros disponíveis: {', '.join(self._macro_loader.macro_names)}"
                )
            logger.forge("Escaneando modelos e metadados...")

        self._discover_executables()
        self._enrich_with_yaml()

        with self.print_lock:
            logger.info(f"DAG carregado: {len(self.models_registry)} modelos identificados.")

    def _discover_executables(self) -> None:
        """Fase 1: identifica e compila arquivos .py e .sql."""
        for filename in sorted(os.listdir(self.models_dir)):
            if filename.startswith("_") or filename.startswith("."):
                continue
            filepath = os.path.join(self.models_dir, filename)
            if filename.endswith(".py"):
                self._register_python_model(filename, filepath)
            elif filename.endswith(".sql"):
                self._register_sql_model(filename, filepath)

    def _register_python_model(self, filename: str, filepath: str) -> None:
        """Importa dinamicamente um modelo Python e registra no DAG."""
        model_name = filename[:-3]
        try:
            spec   = importlib.util.spec_from_file_location(model_name, filepath)
            module = importlib.util.module_from_spec(spec)
            sys.modules[model_name] = module
            spec.loader.exec_module(module)

            if not hasattr(module, "model") or not callable(module.model):
                raise AttributeError(
                    f"O arquivo '{filename}' deve exportar uma função chamada 'model(context)'."
                )

            self.models_registry[model_name] = {
                "type":   "python",
                "func":   module.model,
                "config": {"contract": getattr(module, "CONTRACT", {})},
            }
            self.dependencies_map[model_name] = getattr(module, "DEPENDENCIES", [])

        except Exception as e:
            with self.print_lock:
                logger.error(f"Falha ao carregar modelo Python '{filename}': {e}")

    def _register_sql_model(self, filename: str, filepath: str) -> None:
        """Lê, compila e registra um modelo SQL no DAG."""
        model_name = filename[:-4]
        try:
            with open(filepath, encoding="utf-8") as f:
                raw_sql = f.read()

            compilation = self.compiler.compile(raw_sql, model_name)

            model_config = compilation["config"]
            model_config.setdefault("materialized", "table")
            model_config.setdefault("contract", {})

            self.models_registry[model_name] = {
                "type":     "sql",
                "raw":      raw_sql,
                "compiled": compilation["sql"],
                "config":   model_config,
            }
            self.dependencies_map[model_name] = compilation["depends_on"]

        except Exception as e:
            with self.print_lock:
                logger.error(f"Falha na compilação do modelo SQL '{filename}': {e}")

    def _enrich_with_yaml(self) -> None:
        """Fase 2: aplica metadados e contratos dos arquivos .yml/.yaml."""
        for filename in sorted(os.listdir(self.models_dir)):
            if not filename.endswith((".yml", ".yaml")):
                continue

            yaml_path = os.path.join(self.models_dir, filename)
            try:
                with open(yaml_path, encoding="utf-8") as f:
                    metadata = yaml.safe_load(f)

                if not metadata or "models" not in metadata:
                    continue

                for m_meta in metadata["models"]:
                    name = m_meta.get("name")
                    if not name or name not in self.models_registry:
                        continue

                    model_cfg = self.models_registry[name]["config"]

                    if "description" in m_meta:
                        model_cfg["description"] = m_meta["description"]

                    # v0.4.0: tags para seleção via tag:nome
                    if "tags" in m_meta:
                        model_cfg["tags"] = m_meta["tags"]

                    # Testes por coluna
                    if "columns" in m_meta:
                        contract = {}
                        for col in m_meta["columns"]:
                            col_name = col.get("name")
                            if col_name and "tests" in col:
                                contract[col_name] = col["tests"]
                        model_cfg["contract"] = contract

                    # Testes de nível de modelo (v0.3.0)
                    model_tests = extract_model_tests_from_yaml(m_meta)
                    if model_tests:
                        model_cfg["model_tests"] = model_tests

                with self.print_lock:
                    logger.success(f"Metadados carregados de: '{filename}'.")

            except Exception as e:
                with self.print_lock:
                    logger.error(f"Erro ao processar YAML '{filename}': {e}")

    # ------------------------------------------------------------------
    # Hooks (v0.4.0)
    # ------------------------------------------------------------------

    def _run_hooks(self, adapter, model_name: str, hook_key: str) -> None:
        """
        Executa hooks SQL declarados em {{ config(pre_hook=..., post_hook=...) }}.

        Aceita string única ou lista de strings. Cada hook é compilado
        pelo Jinja2 antes de ser executado, permitindo o uso de var() e macros.

        Parâmetros
        ----------
        adapter : BaseAdapter
            Conexão de banco da thread atual.
        model_name : str
            Nome do modelo (para mensagens de log).
        hook_key : str
            "pre_hook" ou "post_hook".
        """
        model_info = self.models_registry.get(model_name, {})
        hooks_raw  = model_info.get("config", {}).get(hook_key)

        if not hooks_raw:
            return

        # Normaliza: string → lista
        if isinstance(hooks_raw, str):
            hooks_raw = [hooks_raw]

        for i, hook_sql in enumerate(hooks_raw, start=1):
            if not hook_sql or not hook_sql.strip():
                continue
            try:
                # Compila o hook através do Jinja2 (suporta var() e macros)
                compiled_hook = self.compiler.render(hook_sql)
                with self.print_lock:
                    logger.debug(f"  {hook_key}[{i}] '{model_name}': {compiled_hook[:60]}...")
                adapter.execute(compiled_hook)
            except Exception as e:
                raise RuntimeError(
                    f"Erro no {hook_key}[{i}] de '{model_name}': {e}"
                ) from e

    # ------------------------------------------------------------------
    # Worker de Execução (chamado pela thread pool)
    # ------------------------------------------------------------------

    def _execute_node(
        self,
        model_name: str,
        filter_type: str | None,
        context_cache: dict,
        selected_nodes: set[str] | None = None,
        dry_run: bool = False,
    ) -> dict:
        """
        Executa um modelo de forma isolada em sua própria thread.
        Retorna dict com: model, status, execution_time, [rows, error].
        """
        model_info = self.models_registry[model_name]

        # Filtra por tipo (ingest vs transform)
        if filter_type and model_info["type"] != filter_type:
            return {"model": model_name, "status": "skipped", "execution_time": 0}

        # Filtra por seleção (--select)
        if selected_nodes is not None and model_name not in selected_nodes:
            return {"model": model_name, "status": "skipped", "execution_time": 0}

        # --dry-run: exibe o que seria executado sem tocar no banco
        if dry_run:
            return self._dry_run_node(model_name, model_info)

        start   = time.time()
        adapter = self._get_thread_safe_adapter()

        try:
            # pre_hook antes da materialização
            self._run_hooks(adapter, model_name, "pre_hook")

            if model_info["type"] == "sql":
                rows = self._execute_sql_model(model_name, model_info, adapter)
            else:
                rows = self._execute_python_model(model_name, model_info, adapter, context_cache)

            # post_hook após a materialização
            self._run_hooks(adapter, model_name, "post_hook")

            execution_time = round(time.time() - start, 3)
            with self.print_lock:
                logger.success(f"✓ '{model_name}' concluído em {execution_time}s.")

            return {
                "model":          model_name,
                "status":         "success",
                "execution_time": execution_time,
                "rows":           rows,
            }

        except Exception as e:
            with self.print_lock:
                logger.error(f"✗ Erro crítico em '{model_name}': {e}")
            return {
                "model":          model_name,
                "status":         "error",
                "execution_time": round(time.time() - start, 3),
                "error":          str(e),
            }
        finally:
            adapter.close()

    def _dry_run_node(self, model_name: str, model_info: dict) -> dict:
        """
        Simula a execução de um nó no modo --dry-run.

        Exibe o SQL compilado (para modelos SQL) ou a assinatura da função
        (para modelos Python) sem executar nada no banco.
        """
        model_type = model_info["type"]
        config     = model_info.get("config", {})
        mat        = config.get("materialized", "table").upper()
        pre_hook   = config.get("pre_hook")
        post_hook  = config.get("post_hook")

        with self.print_lock:
            if model_type == "sql":
                compiled = model_info.get("compiled", "")
                logger.forge(f"[DRY-RUN] [{mat}] '{model_name}'")
                logger.info(f"  SQL compilado ({len(compiled)} chars):")
                # Exibe as primeiras 3 linhas do SQL compilado
                preview_lines = compiled.strip().splitlines()[:3]
                for line in preview_lines:
                    logger.debug(f"    {line}")
                if len(compiled.splitlines()) > 3:
                    logger.debug(f"    ... ({len(compiled.splitlines()) - 3} linhas omitidas)")
            else:
                logger.forge(f"[DRY-RUN] [PYTHON] '{model_name}'")
                deps = self.dependencies_map.get(model_name, [])
                logger.info(f"  Dependências: {deps or 'nenhuma'}")

            if pre_hook:
                hooks = [pre_hook] if isinstance(pre_hook, str) else pre_hook
                logger.debug(f"  pre_hook: {len(hooks)} hook(s)")
            if post_hook:
                hooks = [post_hook] if isinstance(post_hook, str) else post_hook
                logger.debug(f"  post_hook: {len(hooks)} hook(s)")

        return {"model": model_name, "status": "dry_run", "execution_time": 0}

    def _execute_sql_model(self, model_name: str, model_info: dict, adapter) -> int:
        """Materializa um modelo SQL no banco de dados."""
        mat_type     = model_info["config"].get("materialized", "table").upper()
        unique_key   = model_info["config"].get("unique_key")
        compiled_sql = model_info["compiled"]

        if mat_type == "INCREMENTAL":
            with self.print_lock:
                logger.forge(f"Processando [INCREMENTAL] '{model_name}'...")

            tmp_table = f"{model_name}__dfg_tmp"
            adapter.execute(f"DROP TABLE IF EXISTS {tmp_table} CASCADE")
            adapter.execute(f"CREATE TABLE {tmp_table} AS\n{compiled_sql}")

            if not adapter.check_table_exists(model_name):
                adapter.execute(f"CREATE TABLE {model_name} AS SELECT * FROM {tmp_table}")
            else:
                if unique_key:
                    adapter.execute(
                        f"DELETE FROM {model_name} "
                        f"WHERE {unique_key} IN (SELECT {unique_key} FROM {tmp_table})"
                    )
                adapter.execute(f"INSERT INTO {model_name} SELECT * FROM {tmp_table}")

            adapter.execute(f"DROP TABLE IF EXISTS {tmp_table} CASCADE")

        else:
            with self.print_lock:
                logger.forge(f"Materializando [{mat_type}] '{model_name}'...")

            # DuckDB raise se DROP VIEW for chamado em TABLE ou vice-versa.
            # suppress garante que o tipo correto seja removido.
            with contextlib.suppress(Exception):
                adapter.execute(f"DROP VIEW IF EXISTS {model_name} CASCADE")
            with contextlib.suppress(Exception):
                adapter.execute(f"DROP TABLE IF EXISTS {model_name} CASCADE")

            adapter.execute(f"CREATE {mat_type} {model_name} AS\n{compiled_sql}")

        return 0

    def _execute_python_model(
        self,
        model_name: str,
        model_info: dict,
        adapter,
        context_cache: dict,
    ) -> int:
        """Executa um modelo Python de ingestão e carrega os dados no banco."""
        with self.print_lock:
            logger.forge(f"Extraindo/Ingerindo [PYTHON] '{model_name}'...")

        target_name   = self.config["project"]["target"]
        target_schema = self.config["targets"][target_name].get("schema", "public")

        context = {
            "config":    self.config,
            "ref":       lambda name: context_cache.get(name),
            "state":     self.state_manager.get(model_name),
            "set_state": lambda val, m=model_name: self.state_manager.set(m, val),
            "var":       lambda key, default=None: self.runtime_vars.get(key, default),
        }

        data = model_info["func"](context)

        if data:
            with self.cache_lock:
                context_cache[model_name] = data
            adapter.load_data(table_name=model_name, data=data, schema=target_schema)
            return len(data)

        return 0

    # ------------------------------------------------------------------
    # Orquestrador do DAG
    # ------------------------------------------------------------------

    def _execute_dag(
        self,
        filter_type: str | None = None,
        command_name: str = "run",
        selected_nodes: set[str] | None = None,
        dry_run: bool = False,
    ) -> bool | str:
        """
        Executa todos os modelos respeitando a ordem topológica do DAG,
        com paralelismo real via ThreadPoolExecutor.

        Retorna
        -------
        True       — todos os modelos executados com sucesso
        False      — houve pelo menos um erro
        "no_work"  — nenhum modelo efetivamente executado (todos skipped)
        "dry_run"  — modo dry_run concluído
        """
        self.discover_models()
        self.artifact_manager.save_manifest(self.models_registry, self.dependencies_map)

        max_workers = self.config.get("project", {}).get("threads", 4)
        ts          = graphlib.TopologicalSorter(self.dependencies_map)
        ts.prepare()

        run_results:   list = []
        context_cache: dict = {}
        has_errors        = False
        success_count     = 0

        with self.print_lock:
            if dry_run:
                logger.info("Modo DRY-RUN: nenhuma alteração será feita no banco.")
            logger.info(f"Iniciando pool de execução ({max_workers} thread(s) alocada(s)).")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures_map: dict = {}

            while ts.is_active():
                for node in ts.get_ready():
                    fut = executor.submit(
                        self._execute_node,
                        node,
                        filter_type,
                        context_cache,
                        selected_nodes,
                        dry_run,
                    )
                    futures_map[fut] = node

                if not futures_map:
                    break

                done, _ = wait(futures_map.keys(), return_when=FIRST_COMPLETED)

                for fut in done:
                    node = futures_map.pop(fut)
                    try:
                        result = fut.result()
                        run_results.append(result)

                        status = result.get("status")
                        if status in ("success", "dry_run"):
                            success_count += 1

                        # CRÍTICO: sempre marcar o nó como concluído no DAG.
                        ts.done(node)

                        if status == "error":
                            has_errors = True

                    except Exception as e:
                        has_errors = True
                        ts.done(node)
                        with self.print_lock:
                            logger.error(f"Exceção na thread do modelo '{node}': {e}")

        if not dry_run:
            # v0.4.0: preserva o resultado anterior antes de sobrescrever
            self._rotate_run_results()
            self.artifact_manager.save_run_results(command_name, run_results)

        if dry_run:
            return "dry_run"
        if has_errors:
            return False
        return True if success_count > 0 else "no_work"

    def _rotate_run_results(self) -> None:
        """
        Copia run_results.json → run_results.prev.json antes de sobrescrever.

        Isso permite que `dfg diff` compare a última com a penúltima execução.
        """
        target_dir    = os.path.join(self.project_dir, "target")
        current_path  = os.path.join(target_dir, "run_results.json")
        prev_path     = os.path.join(target_dir, "run_results.prev.json")

        if os.path.exists(current_path):
            with contextlib.suppress(OSError):
                shutil.copy2(current_path, prev_path)

    # ------------------------------------------------------------------
    # Comandos Públicos
    # ------------------------------------------------------------------

    def ingest(self, select: list[str] | None = None, dry_run: bool = False) -> bool | str:
        """Executa apenas os modelos Python (Extract & Load)."""
        self.discover_models()
        selected = resolve_selection(select, self.dependencies_map, self.models_registry)
        return self._execute_dag(
            filter_type="python", command_name="ingest",
            selected_nodes=selected, dry_run=dry_run,
        )

    def transform(self, select: list[str] | None = None, dry_run: bool = False) -> bool | str:
        """Executa apenas os modelos SQL (Transform)."""
        self.discover_models()
        selected = resolve_selection(select, self.dependencies_map, self.models_registry)
        return self._execute_dag(
            filter_type="sql", command_name="transform",
            selected_nodes=selected, dry_run=dry_run,
        )

    def run(self, select: list[str] | None = None, dry_run: bool = False) -> bool | str:
        """Executa o pipeline completo (ingest + transform)."""
        start  = time.time()
        target = (self._override_target or self.config["project"]["target"]).upper()
        with self.print_lock:
            logger.info(f"Iniciando pipeline no ambiente: {target}")

        self.discover_models()
        selected = resolve_selection(select, self.dependencies_map, self.models_registry)
        result   = self._execute_dag(
            filter_type=None, command_name="run",
            selected_nodes=selected, dry_run=dry_run,
        )

        if result is True:
            elapsed = time.time() - start
            with self.print_lock:
                logger.success(f"--- Pipeline finalizado com sucesso em {elapsed:.3f}s! ---")

        return result

    def test(self, select: list[str] | None = None) -> None:
        """
        Executa os contratos de dados definidos nos modelos.
        Encerra com código 1 em caso de falha (CI/CD).
        """
        self.discover_models()
        selected = resolve_selection(select, self.dependencies_map, self.models_registry)
        runner   = TestRunner(engine=self, select=selected)
        report   = runner.run()

        if report.has_failures:
            sys.exit(1)

    def compile(self) -> None:
        """
        Compila modelos e análises sem executar no banco (Dry Run).
        Gera target/compiled/models/*.sql e target/compiled/analysis/*.sql.
        """
        logger.info("Compilando modelos e gerando manifest.json...")
        self.discover_models()
        self.artifact_manager.save_manifest(self.models_registry, self.dependencies_map)

        # Modelos SQL
        compiled_models_dir = os.path.join(self.project_dir, "target", "compiled", "models")
        os.makedirs(compiled_models_dir, exist_ok=True)

        for name, info in self.models_registry.items():
            if info["type"] != "sql":
                continue
            mat_type     = info["config"].get("materialized", "table").upper()
            compiled_sql = info["compiled"]
            out_path     = os.path.join(compiled_models_dir, f"{name}.sql")
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(f"-- Materialização: {mat_type}\n{compiled_sql}\n")
            logger.success(f"Compilado: models/{name}.sql")

        # Análises ad-hoc (v0.4.0)
        self._compile_analysis()

        logger.success("Compilação concluída.")

    def _compile_analysis(self) -> None:
        """
        Compila todos os arquivos .sql de analysis/ para target/compiled/analysis/.

        Análises têm acesso a ref(), var() e macros, mas não são materializadas
        no banco. São úteis para queries exploratórias que se beneficiam da
        compilação Jinja2 (ex: uso de macros, var(), referência a modelos).
        """
        if not os.path.isdir(self.analysis_dir):
            return

        sql_files = [f for f in os.listdir(self.analysis_dir) if f.endswith(".sql")]
        if not sql_files:
            return

        compiled_analysis_dir = os.path.join(self.project_dir, "target", "compiled", "analysis")
        os.makedirs(compiled_analysis_dir, exist_ok=True)

        compiled_count = 0
        for filename in sorted(sql_files):
            analysis_name = filename[:-4]
            filepath      = os.path.join(self.analysis_dir, filename)

            try:
                with open(filepath, encoding="utf-8") as f:
                    raw_sql = f.read()

                compiled_sql = self.compiler.compile_analysis(raw_sql, analysis_name)

                out_path = os.path.join(compiled_analysis_dir, filename)
                with open(out_path, "w", encoding="utf-8") as f:
                    f.write(f"-- Análise: {analysis_name}\n{compiled_sql}\n")

                logger.success(f"Compilado: analysis/{filename}")
                compiled_count += 1

            except Exception as e:
                logger.error(f"Erro ao compilar análise '{filename}': {e}")

        if compiled_count:
            logger.info(
                f"{compiled_count} análise(s) compilada(s) em "
                f"target/compiled/analysis/."
            )

    def snapshots(self) -> None:
        """Processa todos os arquivos de snapshot na pasta snapshots/."""
        logger.info("Iniciando processamento de Snapshots...")

        if not os.path.exists(self.snapshots_dir):
            logger.warn(f"Diretório de snapshots não encontrado: '{self.snapshots_dir}'.")
            return

        snapshot_files = [f for f in os.listdir(self.snapshots_dir) if f.endswith(".sql")]

        if not snapshot_files:
            logger.info("Nenhum arquivo de snapshot encontrado.")
            return

        success_count = 0
        for file_name in snapshot_files:
            file_path = os.path.join(self.snapshots_dir, file_name)
            try:
                with open(file_path, encoding="utf-8") as f:
                    raw_sql = f.read()

                snapshot_data = self.compiler.parse_snapshot(raw_sql)

                if not snapshot_data:
                    logger.error(
                        f"'{file_name}' não possui um bloco "
                        f"{{% snapshot %}} ... {{% endsnapshot %}} válido."
                    )
                    continue

                success = self.snapshot_runner.run_snapshot(
                    snapshot_name=snapshot_data["snapshot_name"],
                    parsed_config=snapshot_data["config"],
                    compiled_source_sql=snapshot_data["compiled_sql"],
                )
                if success:
                    success_count += 1

            except Exception as e:
                logger.error(f"Falha crítica ao processar snapshot '{file_name}': {e}")

        logger.info(
            f"Snapshots: {success_count}/{len(snapshot_files)} executados com sucesso."
        )