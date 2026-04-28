# src/dfg/initialization.py
"""
Comando 'dfg init' do DataForge.

Inicializa a estrutura de um novo projeto com:
- dfg_project.toml (configuração do projeto)
- profiles.toml (credenciais de conexão por ambiente)
- Estrutura de pastas padrão (models/, seeds/, snapshots/, …)
- Arquivos de boilerplate para começar rapidamente
"""
import importlib.util
import os

from dfg.logging import logger

# ------------------------------------------------------------------
# Catálogo de bancos suportados
# ------------------------------------------------------------------

DB_CATALOG = [
    {
        "name": "DuckDB",
        "lib": "duckdb",
        "type": "duckdb",
        "fields": 'database = "{name}_dev.db"',
    },
    {
        "name": "PostgreSQL (psycopg2)",
        "lib": "psycopg2",
        "type": "postgres",
        "fields": (
            'host = "localhost"\n'
            "port = 5432\n"
            'user = "admin"\n'
            'password = "password"\n'
            'database = "{name}_db"\n'
            'schema = "public"'
        ),
    },
    {
        "name": "MySQL",
        "lib": "mysql.connector",
        "type": "mysql",
        "fields": (
            'host = "localhost"\n'
            "port = 3306\n"
            'user = "root"\n'
            'password = "password"\n'
            'database = "{name}_db"'
        ),
    },
    {
        "name": "SQL Server (pyodbc)",
        "lib": "pyodbc",
        "type": "sqlserver",
        "fields": (
            'driver = "ODBC Driver 17 for SQL Server"\n'
            'server = "localhost"\n'
            'user = "sa"\n'
            'password = "password"\n'
            'database = "{name}_db"'
        ),
    },
    {
        "name": "BigQuery",
        "lib": "google.cloud.bigquery",
        "type": "bigquery",
        "fields": (
            'method = "service-account"\n'
            'project = "seu-projeto-gcp"\n'
            'dataset = "{name}_dataset"\n'
            'keyfile = "caminho/para/keyfile.json"'
        ),
    },
    {
        "name": "Snowflake",
        "lib": "snowflake.connector",
        "type": "snowflake",
        "fields": (
            'account = "xyz123.region"\n'
            'user = "admin"\n'
            'password = "password"\n'
            'warehouse = "compute_wh"\n'
            'database = "{name}_db"\n'
            'schema = "public"'
        ),
    },
    {
        "name": "SQLite (Nativo)",
        "lib": "sqlite3",
        "type": "sqlite",
        "fields": 'database = "{name}_sqlite.db"',
    },
]

ANVIL_ASCII = r"""
                       ████████████████████████████████
   ███████████████████▓▓████████████████████████████▒
    ▒█████████████████▓████████████████████████▓
      ▒███████████▒█████████████████████████▒
         ▓████████░███████████████████████
                   █████████████████████░
                        ███████████████
                         █████████████
                         █████████████
                        ░████   ░█████
                       ▓██▓▒█████▓▓████
                    ▓████████████████████░
                 ░█████ ██████████████▓█████
                 ░███████           ░███████
"""

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def is_lib_installed(lib_name: str) -> bool:
    """
    Verifica se uma biblioteca Python está instalada sem importá-la.

    Usa importlib.util.find_spec() no módulo raiz para evitar erros
    com submódulos (ex: 'google.cloud.bigquery' → verifica 'google').
    """
    try:
        base_module = lib_name.split(".")[0]
        spec = importlib.util.find_spec(base_module)
        return spec is not None
    except (ModuleNotFoundError, ValueError):
        return False


def discover_installed_drivers() -> dict:
    """
    Retorna um dicionário indexado (1, 2, …) dos bancos disponíveis
    no ambiente atual do usuário.
    """
    available = {}
    index = 1
    for db in DB_CATALOG:
        if is_lib_installed(db["lib"]):
            available[str(index)] = db
            index += 1
    return available


# ------------------------------------------------------------------
# Conteúdo dos arquivos boilerplate
# ------------------------------------------------------------------

_EXAMPLE_SQL = "{{ config(materialized='table') }}\n\nSELECT 1 AS id, 'Data Forge' AS tool\n"

_EXAMPLE_SNAPSHOT = """\
{% snapshot my_first_snapshot %}

{{
    config(
        unique_key='id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

SELECT * FROM {{ ref('my_first_model') }}

{% endsnapshot %}
"""

_EXAMPLE_SCHEMA_YAML = """\
version: 1

models:
  - name: my_first_model
    description: "Modelo de exemplo gerado pelo dfg init."
    columns:
      - name: id
        tests:
          - not_null
          - unique
"""

# ------------------------------------------------------------------
# Comando principal
# ------------------------------------------------------------------


def init_command(args) -> None:
    """
    Inicializa um novo projeto DataForge no diretório atual.

    Fluxo:
    1. Solicita o nome do projeto
    2. Detecta os drivers de banco instalados no ambiente
    3. Permite o usuário escolher o banco preferido
    4. Gera dfg_project.toml, profiles.toml e estrutura de pastas
    5. Cria arquivos de boilerplate para acelerar o início
    """
    # Inicializa o logger para o diretório atual (sem projeto ainda)
    logger.setup(os.getcwd())

    print(ANVIL_ASCII)
    logger.success("Bem-vindo ao Data Forge!")
    print("-" * 50)

    project_name = input("Nome do projeto (ex: meu_projeto): ").strip() or "dfg_project"

    # Validação simples do nome do projeto
    if not project_name.replace("_", "").replace("-", "").isalnum():
        logger.error("Nome do projeto deve conter apenas letras, números, _ e -.")
        return

    installed_dbs = discover_installed_drivers()

    if not installed_dbs:
        logger.error(
            "Nenhum driver de banco de dados detectado no ambiente. "
            "Instale pelo menos um: pip install duckdb  (recomendado para iniciar)"
        )
        return

    print("\nBancos de dados detectados no seu ambiente:")
    for idx, db in installed_dbs.items():
        print(f"  [{idx}] {db['name']}")

    while True:
        choice = input("\nEscolha o número do banco preferido: ").strip()
        if choice in installed_dbs:
            selected_db = installed_dbs[choice]
            break
        logger.error(f"Opção inválida. Escolha um número entre 1 e {len(installed_dbs)}.")

    logger.success(f"Configurando projeto '{project_name}' com: {selected_db['name']}")

    # --- Geração do dfg_project.toml ---
    project_toml = (
        f"[project]\n"
        f'name = "{project_name}"\n'
        f'profile = "{project_name}"\n'
        f'target = "dev"\n'
        f"threads = 4\n"
    )
    _write_file("dfg_project.toml", project_toml)

    # --- Geração do profiles.toml ---
    conn_fields = selected_db["fields"].format(name=project_name)
    prod_fields = conn_fields.replace("_dev", "_prod").replace("localhost", "10.0.0.1")

    profiles_toml = (
        f"[{project_name}]\n"
        f'target = "dev"\n'
        f"\n"
        f"[{project_name}.outputs.dev]\n"
        f'type = "{selected_db["type"]}"\n'
        f"{conn_fields}\n"
        f"\n"
        f"[{project_name}.outputs.prod]\n"
        f'type = "{selected_db["type"]}"\n'
        f"# Substitua pelas credenciais de produção:\n"
        f"{prod_fields}\n"
    )
    _write_file("profiles.toml", profiles_toml)

    # --- Estrutura de pastas ---
    folders = [
        "models",
        "snapshots",
        "seeds",
        "analysis",
        "tests",
        "logs",
        os.path.join("target", "compiled"),
    ]
    for folder in folders:
        os.makedirs(folder, exist_ok=True)

    # --- Arquivos de boilerplate (não sobrescreve se já existem) ---
    _write_file_if_absent(os.path.join("models", "my_first_model.sql"), _EXAMPLE_SQL)
    _write_file_if_absent(os.path.join("snapshots", "my_first_snapshot.sql"), _EXAMPLE_SNAPSHOT)
    _write_file_if_absent(os.path.join("models", "schema.yml"), _EXAMPLE_SCHEMA_YAML)

    print("-" * 50)
    logger.success(f"Projeto '{project_name}' forjado com sucesso! 🔥")
    print(
        "\nPróximos passos:\n"
        "  1. Revise o profiles.toml com suas credenciais de banco\n"
        "  2. Crie seus modelos na pasta models/\n"
        "  3. Execute: dfg run\n"
    )


# ------------------------------------------------------------------
# Utilitários internos
# ------------------------------------------------------------------


def _write_file(path: str, content: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def _write_file_if_absent(path: str, content: str) -> None:
    if not os.path.exists(path):
        _write_file(path, content)
