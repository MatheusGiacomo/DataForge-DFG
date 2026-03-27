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
    """
    Verifica de forma segura se a biblioteca está instalada no venv.
    Para evitar o erro crítico do Python ao buscar submódulos (com ponto),
    verificamos estritamente o módulo raiz.
    """
    try:
        # Pulo do gato: "mysql.connector" vira apenas "mysql".
        # "google.cloud.bigquery" vira apenas "google".
        base_module = lib_name.split('.')[0] 
        
        # Ao buscar nomes sem ponto, o find_spec nunca dá erro, apenas retorna None
        spec = importlib.util.find_spec(base_module)
        return spec is not None
    except Exception:
        # Fallback de segurança máxima
        return False

def discover_installed_drivers():
    """
    Varre o catálogo e retorna um dicionário numerado dinamicamente
    apenas com os drivers que realmente existem na máquina do usuário.
    """
    available = {}
    index = 1
    for db in DB_CATALOG:
        if is_lib_installed(db["lib"]):
            available[str(index)] = db
            index += 1
    return available

def init_command(args):
    print(ANVIL_ASCII)
    logger.success("Bem-vindo à Forja NexusData!")
    print("-" * 45)
    
    project_name = input("Nome do seu projeto (ex: nexus_project): ") or "nexus_project"
    
    # --- Lógica Dinâmica de Auto-Discovery ---
    installed_dbs = discover_installed_drivers()
    
    print("\nBancos de dados identificados no seu ambiente:")
    for idx, db in installed_dbs.items():
        print(f"[{idx}] {db['name']}")
    
    # Loop de segurança: obriga o usuário a digitar uma opção válida
    while True:
        choice = input("\nDigite o número para o banco de preferência: ")
        if choice in installed_dbs:
            selected_db = installed_dbs[choice]
            break
        else:
            logger.error("Opção inválida. Digite um dos números da lista acima.")

    logger.success(f"Configurando forja para: {selected_db['name']}")

    # 1. Cria o dfg_project.toml (Configuração de Infra)
    project_toml = f"""[project]
name = "{project_name}"
profile = "{project_name}"
target = "dev"
threads = 4
"""
    with open("dfg_project.toml", "w", encoding="utf-8") as f:
        f.write(project_toml)
        
    # 2. Cria o profiles.toml (Configuração de Credenciais)
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
        
    # 3. Estrutura de Pastas Profissional
    folders = ["models", "seeds", "analysis", "target/compiled"]
    for folder in folders:
        os.makedirs(folder, exist_ok=True)
    
    # Cria um modelo de exemplo
    example_sql = os.path.join("models", "my_first_model.sql")
    if not os.path.exists(example_sql):
        with open(example_sql, "w", encoding="utf-8") as f:
            f.write("{{ config(materialized='table') }}\n\nSELECT 1 as id, 'Nexus' as platform")

    print("-" * 45)
    logger.success(f"Projeto '{project_name}' forjado com sucesso!")
    logger.info(f"O arquivo 'profiles.toml' foi customizado com os campos exigidos pelo {selected_db['name']}.")