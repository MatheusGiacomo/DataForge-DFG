# src/dfg/initialization.py
import os
from dfg.logger import logger

# A Bigorna do Data Forge em ASCII Art (usando a cor azul do seu logger)
ANVIL_ASCII = f"""{logger.BLUE}
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
{logger.RESET}"""

def init_command(args):
    print(ANVIL_ASCII)
    logger.success("Bem vindo ao Data Forge!")
    print("-" * 45)
    
    project_name = input("Nome do seu projeto (ex: meu_projeto): ") or "meu_projeto"
    target_db = input("Banco de dados padrão (duckdb/postgres): ").lower() or "duckdb"
    
    # 1. Cria o dfg_project.toml
    project_toml = f"""[project]
name = "{project_name}"
profile = "{project_name}"
target = "dev"
"""
    with open("dfg_project.toml", "w") as f:
        f.write(project_toml)
        
    # 2. Cria o profiles.toml na RAIZ do projeto
    profiles_toml = f"""[{project_name}]
target = "dev"

[{project_name}.outputs.dev]
type = "{target_db}"
database = "{project_name}_dev.db"

[{project_name}.outputs.prod]
type = "postgres"
host = "localhost"
port = 5432
user = "admin"
password = "senha_segura"
database = "prod_db"
schema = "public"
"""
    with open("profiles.toml", "w") as f:
        f.write(profiles_toml)
        
    # 3. Cria a pasta de modelos
    os.makedirs("models", exist_ok=True)
    
    print("-" * 45)
    logger.success(f"Projeto '{project_name}' forjado com sucesso na pasta atual!")
    logger.info("Abra o arquivo 'profiles.toml' para configurar suas conexões antes de rodar os modelos.")