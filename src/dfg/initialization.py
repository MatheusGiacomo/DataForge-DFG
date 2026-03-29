# src/dfg/initialization.py
import os
import importlib.util
from dfg.logging import logger

# Catálogo mestre com os bancos suportados.
# Apenas os que estiverem instalados na máquina irão aparecer para o usuário.
DB_CATALOG = [
    {"name": "DuckDB", "lib": "duckdb", "type": "duckdb", "fields": 'database = "{name}_dev.db"'},
    {"name": "PostgreSQL (psycopg2)", "lib": "psycopg2", "type": "postgres", "fields": 'host = "localhost"\nport = 5432\nuser = "admin"\npassword = "password"\ndatabase = "{name}_db"\nschema = "public"'},
    {"name": "MySQL", "lib": "mysql.connector", "type": "mysql", "fields": 'host = "localhost"\nport = 3306\nuser = "root"\npassword = "password"\ndatabase = "{name}_db"'},
    {"name": "SQL Server (pyodbc)", "lib": "pyodbc", "type": "sqlserver", "fields": 'driver = "ODBC Driver 17 for SQL Server"\nserver = "localhost"\nuser = "sa"\npassword = "password"\ndatabase = "{name}_db"'},
    {"name": "BigQuery", "lib": "google.cloud.bigquery", "type": "bigquery", "fields": 'method = "service-account"\nproject = "seu-projeto-gcp"\ndataset = "{name}_dataset"\nkeyfile = "caminho/para/keyfile.json"'},
    {"name": "Snowflake", "lib": "snowflake.connector", "type": "snowflake", "fields": 'account = "xyz123.region"\nuser = "admin"\npassword = "password"\nwarehouse = "compute_wh"\ndatabase = "{name}_db"\nschema = "public"'},
    {"name": "SQLite (Nativo)", "lib": "sqlite3", "type": "sqlite", "fields": 'database = "{name}_sqlite.db"'}
]

ANVIL_ASCII = """
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

def is_lib_installed(lib_name):
    """Verifica de forma segura se a biblioteca está instalada no venv."""
    try:
        base_module = lib_name.split('.')[0] 
        spec = importlib.util.find_spec(base_module)
        return spec is not None
    except Exception:
        return False

def discover_installed_drivers():
    """Retorna drivers instalados na máquina."""
    available = {}
    index = 1
    for db in DB_CATALOG:
        if is_lib_installed(db["lib"]):
            available[str(index)] = db
            index += 1
    return available

def init_command(args):
    print(ANVIL_ASCII)
    logger.success("Bem-vindo à Forja Data Forge (DFG)!")
    print("-" * 45)
    
    project_name = input("Nome do seu projeto (ex: dfg_project): ") or "dfg_project"
    
    installed_dbs = discover_installed_drivers()
    
    print("\nBancos de dados identificados no seu ambiente:")
    for idx, db in installed_dbs.items():
        print(f"[{idx}] {db['name']}")
    
    while True:
        choice = input("\nDigite o número para o banco de preferência: ")
        if choice in installed_dbs:
            selected_db = installed_dbs[choice]
            break
        else:
            logger.error("Opção inválida. Digite um dos números da lista acima.")

    logger.success(f"Configurando forja para: {selected_db['name']}")

    # 1. Cria o dfg_project.toml
    project_toml = f"""[project]
name = "{project_name}"
profile = "{project_name}"
target = "dev"
threads = 4
"""
    with open("dfg_project.toml", "w", encoding="utf-8") as f:
        f.write(project_toml)
        
    # 2. Cria o profiles.toml
    conn_fields = selected_db["fields"].format(name=project_name)
    
    profiles_toml = f"""[{project_name}]
target = "dev"

[{project_name}.outputs.dev]
type = "{selected_db['type']}"
{conn_fields}

[{project_name}.outputs.prod]
type = "{selected_db['type']}"
# Substitua pelas Credenciais de Produção:
{conn_fields.replace('dev', 'prod').replace('localhost', '10.0.0.1')}
"""
    with open("profiles.toml", "w", encoding="utf-8") as f:
        f.write(profiles_toml)
        
    # 3. Estrutura de Pastas Profissional (Snapshots inclusa)
    folders = [
        "models", 
        "snapshots",  # Nova pasta para SCD Tipo 2
        "seeds", 
        "analysis", 
        "tests",
        "target/compiled"
    ]
    for folder in folders:
        os.makedirs(folder, exist_ok=True)
    
    # 4. Boilerplates (Arquivos iniciais para ajudar o usuário)
    
    # Exemplo de Modelo
    example_sql = os.path.join("models", "my_first_model.sql")
    if not os.path.exists(example_sql):
        with open(example_sql, "w", encoding="utf-8") as f:
            f.write("{{ config(materialized='table') }}\n\nSELECT 1 as id, 'Data Forge' as tool")

    # Exemplo de Snapshot (SCD2)
    example_snapshot = os.path.join("snapshots", "my_first_snapshot.sql")
    if not os.path.exists(example_snapshot):
        with open(example_snapshot, "w", encoding="utf-8") as f:
            f.write("""{% snapshot my_first_snapshot %}

{{
    config(
        unique_key='id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

SELECT * FROM {{ ref('my_first_model') }}

{% endsnapshot %}""")

    print("-" * 45)
    logger.success(f"Projeto '{project_name}' forjado com sucesso!")