# src/dfg/engine.py
"""
Motor principal do DataForge (DFGEngine).

Responsável por:
- Carregar e validar as configurações do projeto (dfg_project.toml + profiles.toml)
- Descobrir e compilar modelos SQL e Python (discover_models)
- Executar o DAG com paralelismo real via ThreadPoolExecutor
- Orquestrar testes, compilação, snapshots e seeds
"""
import graphlib
import importlib.util
import os
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
from dfg.snapshot import SnapshotRunner
from dfg.state import StateManager


class DFGEngine:
    """
    Coração do DataForge.

    Instancie passando o diretório raiz do projeto. O motor lê as
    configurações, inicializa os serviços internos e fica pronto para
    executar qualquer comando (run, test, compile, snapshot, …).
    """

    def __init__(self, project_dir: str):
        self.project_dir = os.path.abspath(project_dir)
        self.models_dir = os.path.join(self.project_dir, "models")
        self.snapshots_dir = os.path.join(self.project_dir, "snapshots")
        self.seeds_dir = os.path.join(self.project_dir, "seeds")

        # Inicialização do logger antes de qualquer outra coisa
        logger.setup(self.project_dir)

        # Carrega configurações e inicializa serviços de suporte
        self.config = self._load_config()
        self.artifact_manager = ArtifactManager(self.project_dir)
        self.state_manager = StateManager(self.project_dir)

        # O compilador é criado aqui com o schema alvo correto
        target_name = self.config["project"]["target"]
        target_schema = self.config["targets"][target_name].get("schema", "public")
        self.compiler = SQLCompiler(target_schema=target_schema)

        self.snapshot_runner = SnapshotRunner(self)

        # Registros de estado do DAG (preenchidos por discover_models)
        self.models_registry: dict = {}
        self.dependencies_map: dict = {}

        # Locks para thread-safety nas operações de log e cache
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
        project_toml = os.path.join(self.project_dir, "dfg_project.toml")
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
        target_name = config["project"].get("target", "dev")

        if not profile_name:
            raise ValueError("Campo 'profile' ausente em [project] no dfg_project.toml.")

        try:
            credentials = profiles[profile_name]["outputs"][target_name]
        except KeyError as err:
            raise ValueError(
                f"Target '{target_name}' não encontrado no profile '{profile_name}' "
                f"do profiles.toml."
            ) from err

        config["targets"] = {target_name: credentials}
        return config

    # ------------------------------------------------------------------
    # Adaptador Thread-Safe
    # ------------------------------------------------------------------

    def _get_thread_safe_adapter(self):
        """
        Cria e conecta um adaptador de banco independente para a thread atual.

        Cada worker thread precisa de sua própria conexão para evitar
        colisão de cursores durante execução paralela.
        """
        target_name = self.config["project"]["target"]
        target_config = self.config["targets"][target_name]

        adapter = AdapterFactory.get_adapter(target_config["type"])
        adapter.connect(target_config)
        return adapter

    # ------------------------------------------------------------------
    # Descoberta de Modelos (Fase 1 + Fase 2)
    # ------------------------------------------------------------------

    def discover_models(self) -> None:
        """
        Popula ``models_registry`` e ``dependencies_map``.

        Fase 1 — Executáveis: Mapeia .sql e .py, compilando templates Jinja.
        Fase 2 — Enriquecimento: Aplica metadados e contratos dos arquivos .yml.

        Idempotente: chamadas subsequentes são ignoradas se o registry
        já estiver preenchido.
        """
        if self.models_registry:
            return

        if not os.path.exists(self.models_dir):
            with self.print_lock:
                logger.warn(f"Diretório de modelos não encontrado: '{self.models_dir}'.")
            return

        with self.print_lock:
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
            spec = importlib.util.spec_from_file_location(model_name, filepath)
            module = importlib.util.module_from_spec(spec)
            sys.modules[model_name] = module
            spec.loader.exec_module(module)

            if not hasattr(module, "model") or not callable(module.model):
                raise AttributeError(
                    f"O arquivo '{filename}' deve exportar uma função chamada 'model(context)'."
                )

            self.models_registry[model_name] = {
                "type": "python",
                "func": module.model,
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
                "type": "sql",
                "raw": raw_sql,
                "compiled": compilation["sql"],
                "config": model_config,
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

                    if "columns" in m_meta:
                        contract = {}
                        for col in m_meta["columns"]:
                            col_name = col.get("name")
                            if col_name and "tests" in col:
                                contract[col_name] = col["tests"]
                        model_cfg["contract"] = contract

                with self.print_lock:
                    logger.success(f"Metadados carregados de: '{filename}'.")

            except Exception as e:
                with self.print_lock:
                    logger.error(f"Erro ao processar YAML '{filename}': {e}")

    # ------------------------------------------------------------------
    # Worker de Execução (chamado pela thread pool)
    # ------------------------------------------------------------------

    def _execute_node(
        self,
        model_name: str,
        filter_type: str | None,
        context_cache: dict,
    ) -> dict:
        """
        Executa um modelo de forma isolada em sua própria thread.

        Retorna um dicionário com: model, status, execution_time, [rows, error].
        """
        model_info = self.models_registry[model_name]

        # Filtra modelos fora do tipo solicitado (ingest vs transform)
        if filter_type and model_info["type"] != filter_type:
            return {"model": model_name, "status": "skipped", "execution_time": 0}

        start = time.time()
        adapter = self._get_thread_safe_adapter()
        rows_affected = 0

        try:
            if model_info["type"] == "sql":
                rows_affected = self._execute_sql_model(model_name, model_info, adapter)
            else:
                rows_affected = self._execute_python_model(model_name, model_info, adapter, context_cache)

            execution_time = round(time.time() - start, 3)
            with self.print_lock:
                logger.success(f"✓ '{model_name}' concluído em {execution_time}s.")

            return {
                "model": model_name,
                "status": "success",
                "execution_time": execution_time,
                "rows": rows_affected,
            }

        except Exception as e:
            with self.print_lock:
                logger.error(f"✗ Erro crítico em '{model_name}': {e}")
            return {
                "model": model_name,
                "status": "error",
                "execution_time": round(time.time() - start, 3),
                "error": str(e),
            }
        finally:
            adapter.close()

    def _execute_sql_model(self, model_name: str, model_info: dict, adapter) -> int:
        """Materializa um modelo SQL no banco de dados."""
        mat_type = model_info["config"].get("materialized", "table").upper()
        unique_key = model_info["config"].get("unique_key")
        compiled_sql = model_info["compiled"]

        if mat_type == "INCREMENTAL":
            with self.print_lock:
                logger.forge(f"Processando [INCREMENTAL] '{model_name}'...")

            tmp_table = f"{model_name}__dfg_tmp"
            adapter.execute(f"DROP TABLE IF EXISTS {tmp_table} CASCADE")
            adapter.execute(f"CREATE TABLE {tmp_table} AS\n{compiled_sql}")

            table_exists = adapter.check_table_exists(model_name)

            if not table_exists:
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

            adapter.execute(f"DROP VIEW IF EXISTS {model_name} CASCADE")
            adapter.execute(f"DROP TABLE IF EXISTS {model_name} CASCADE")
            adapter.execute(f"CREATE {mat_type} {model_name} AS\n{compiled_sql}")

        return 0  # DDL não retorna contagem de linhas

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

        target_name = self.config["project"]["target"]
        target_schema = self.config["targets"][target_name].get("schema", "public")

        context = {
            "config": self.config,
            "ref": lambda name: context_cache.get(name),
            "state": self.state_manager.get(model_name),
            "set_state": lambda val, m=model_name: self.state_manager.set(m, val),
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

    def _execute_dag(self, filter_type: str | None = None, command_name: str = "run") -> bool | str:
        """
        Executa todos os modelos respeitando a ordem topológica do DAG,
        com paralelismo real via ThreadPoolExecutor.

        Retorna
        -------
        True       — todos os modelos foram executados com sucesso
        False      — houve pelo menos um erro
        "no_work"  — nenhum modelo foi efetivamente executado (todos skipped)
        """
        self.discover_models()
        self.artifact_manager.save_manifest(self.models_registry, self.dependencies_map)

        max_workers = self.config.get("project", {}).get("threads", 4)
        ts = graphlib.TopologicalSorter(self.dependencies_map)
        ts.prepare()

        run_results: list = []
        context_cache: dict = {}
        has_errors = False
        success_count = 0

        with self.print_lock:
            logger.info(f"Iniciando pool de execução ({max_workers} thread(s) alocada(s)).")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures_map: dict = {}

            while ts.is_active():
                # Despacha todos os nós prontos (sem dependências pendentes)
                for node in ts.get_ready():
                    fut = executor.submit(self._execute_node, node, filter_type, context_cache)
                    futures_map[fut] = node

                if not futures_map:
                    # Nenhum nó em voo e nenhum pronto → encerrado
                    break

                # Aguarda o primeiro futuro concluir
                done, _ = wait(futures_map.keys(), return_when=FIRST_COMPLETED)

                for fut in done:
                    node = futures_map.pop(fut)
                    try:
                        result = fut.result()
                        run_results.append(result)

                        status = result.get("status")
                        if status == "success":
                            success_count += 1

                        # CRÍTICO: sempre marcar o nó como concluído no DAG,
                        # mesmo em caso de erro. Do contrário, nós dependentes
                        # nunca ficam prontos e o loop trava.
                        ts.done(node)

                        if status == "error":
                            has_errors = True

                    except Exception as e:
                        has_errors = True
                        ts.done(node)
                        with self.print_lock:
                            logger.error(f"Exceção na thread do modelo '{node}': {e}")

        self.artifact_manager.save_run_results(command_name, run_results)

        if has_errors:
            return False
        return True if success_count > 0 else "no_work"

    # ------------------------------------------------------------------
    # Comandos Públicos
    # ------------------------------------------------------------------

    def ingest(self) -> bool | str:
        """Executa apenas os modelos Python (Extract & Load)."""
        return self._execute_dag(filter_type="python", command_name="ingest")

    def transform(self) -> bool | str:
        """Executa apenas os modelos SQL (Transform)."""
        return self._execute_dag(filter_type="sql", command_name="transform")

    def run(self) -> bool | str:
        """Executa o pipeline completo (ingest + transform)."""
        start = time.time()
        target = self.config["project"]["target"].upper()
        with self.print_lock:
            logger.info(f"Iniciando pipeline no ambiente: {target}")

        result = self._execute_dag(filter_type=None, command_name="run")

        if result is True:
            elapsed = time.time() - start
            with self.print_lock:
                logger.success(f"--- Pipeline finalizado com sucesso em {elapsed:.3f}s! ---")

        return result

    def test(self) -> None:
        """
        Executa os contratos de dados definidos nos modelos.

        Se algum teste falhar, encerra o processo com código 1
        para sinalizar falha ao ambiente de CI/CD.
        """
        self.discover_models()
        adapter = self._get_thread_safe_adapter()
        logger.info("--- Iniciando Validação de Contratos ---")
        erros = 0

        try:
            for model_name, model_info in self.models_registry.items():
                contract = model_info.get("config", {}).get("contract")

                if not contract:
                    logger.warn(f"Modelo '{model_name}': sem contrato definido. Pulando.")
                    continue

                logger.forge(f"Testando '{model_name}'...")

                try:
                    row_count_result = adapter.execute(f"SELECT COUNT(*) FROM {model_name}")
                    if not row_count_result or row_count_result[0][0] == 0:
                        logger.warn(f"  [AVISO] Tabela '{model_name}' está vazia.")

                    for coluna, testes in contract.items():
                        for teste in testes:
                            erros += self._run_single_test(adapter, model_name, coluna, teste)

                except Exception as e:
                    logger.error(f"  Erro ao acessar '{model_name}' no banco: {e}")
                    erros += 1

        finally:
            adapter.close()

        if erros > 0:
            logger.error(f"Validação concluída com {erros} falha(s).")
            sys.exit(1)

        logger.success("Todos os contratos validados com sucesso!")

    def _run_single_test(
        self, adapter, model_name: str, column: str, test_name: str
    ) -> int:
        """Executa um único teste e retorna 0 (passou) ou 1 (falhou)."""
        if test_name == "not_null":
            result = adapter.execute(
                f"SELECT COUNT(*) FROM {model_name} WHERE {column} IS NULL"
            )
            nulos = result[0][0] if result else 0
            if nulos > 0:
                logger.error(f"  [FALHA] not_null: '{model_name}.{column}' tem {nulos} nulos.")
                return 1

        elif test_name == "unique":
            result = adapter.execute(
                f"SELECT COUNT(*) FROM ("
                f"  SELECT {column} FROM {model_name} "
                f"  GROUP BY {column} HAVING COUNT(*) > 1"
                f") AS __dfg_dups"
            )
            dups = result[0][0] if result else 0
            if dups > 0:
                logger.error(f"  [FALHA] unique: '{model_name}.{column}' tem {dups} chave(s) duplicada(s).")
                return 1

        else:
            logger.warn(f"  [AVISO] Teste '{test_name}' não reconhecido. Pulando.")

        return 0

    def compile(self) -> None:
        """
        Gera os arquivos SQL compilados e o manifest.json sem executar no banco (Dry Run).
        """
        logger.info("Compilando modelos e gerando manifest.json...")
        self.discover_models()
        self.artifact_manager.save_manifest(self.models_registry, self.dependencies_map)

        compiled_dir = os.path.join(self.project_dir, "target", "compiled")
        os.makedirs(compiled_dir, exist_ok=True)

        for name, info in self.models_registry.items():
            if info["type"] != "sql":
                continue

            mat_type = info["config"].get("materialized", "table").upper()
            compiled_sql = info["compiled"]

            out_path = os.path.join(compiled_dir, f"{name}.sql")
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(f"-- Materialização: {mat_type}\n{compiled_sql}\n")

            logger.success(f"Compilado: {name}.sql")

        logger.success("Compilação concluída.")

    def snapshots(self) -> None:
        """Processa todos os arquivos de snapshot na pasta snapshots/."""
        logger.info("Iniciando processamento de Snapshots...")

        if not os.path.exists(self.snapshots_dir):
            logger.warn(f"Diretório de snapshots não encontrado: '{self.snapshots_dir}'.")
            return

        snapshot_files = [
            f for f in os.listdir(self.snapshots_dir) if f.endswith(".sql")
        ]

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
