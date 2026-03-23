# src/dfg/docs.py
import sys
import os
from datetime import datetime
from dfg.logging import logger
from dfg.engine import DFGEngine

def docs_command(args):
    logger.info("Gerando documentação e linhagem visual...")
    current_dir = os.getcwd()
    
    try:
        engine = DFGEngine(project_dir=current_dir)
        engine.discover_models()
    except Exception as e:
        logger.error(f"Falha ao carregar o projeto: {e}")
        sys.exit(1)

    catalog_path = os.path.join(current_dir, "catalog.md")
    
    with open(catalog_path, "w", encoding="utf-8") as f:
        # Cabeçalho
        f.write(f"# 🛠️ Catálogo de Dados: {engine.config['project']['name'].upper()}\n\n")
        f.write(f"> Gerado automaticamente pelo **Data Forge (DFG)** em {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # --- SEÇÃO 1: LINHAGEM VISUAL (MERMAID) ---
        f.write("## 📊 Linhagem Visual (DAG)\n\n")
        f.write("```mermaid\n")
        f.write("graph LR\n")
        f.write("    %% Estilização\n")
        f.write("    classDef python fill:#3776ab,color:#fff,stroke:#333,stroke-width:2px;\n")
        f.write("    classDef sql fill:#f29111,color:#fff,stroke:#333,stroke-width:2px;\n")
        
        # Desenha as conexões
        for model_name, deps in engine.dependencies_map.items():
            model_info = engine.models_registry[model_name]
            is_sql = isinstance(model_info, dict) and model_info.get("type") == "sql"
            
            # Aplica classes de estilo para diferenciar Python de SQL
            style_class = "sql" if is_sql else "python"
            f.write(f"    {model_name}(({model_name}))::: {style_class}\n")
            
            for dep in deps:
                f.write(f"    {dep} --> {model_name}\n")
        
        f.write("```\n\n")
        f.write("> 💡 **Dica:** Azul = Ingestão Python | Laranja = Transformação SQL\n\n")

        # --- SEÇÃO 2: DETALHES DOS MODELOS ---
        f.write("## 🗄️ Dicionário de Modelos\n\n")
        
        for model_name, deps in engine.dependencies_map.items():
            model_info = engine.models_registry[model_name]
            is_sql = isinstance(model_info, dict) and model_info.get("type") == "sql"
            tipo = "SQL (Transformação)" if is_sql else "Python (Extração/API)"
            
            f.write(f"### 🔹 `{model_name}`\n")
            f.write(f"- **Tipo:** {tipo}\n")
            
            if deps:
                f.write(f"- **Dependências:** {', '.join([f'`{d}`' for d in deps])}\n")
            
            # Adiciona os Data Contracts se for Python
            if not is_sql:
                module = sys.modules.get(model_name)
                contract = getattr(module, 'CONTRACT', None)
                if contract:
                    f.write("\n**Contratos de Validação:**\n")
                    f.write("| Coluna | Testes Aplicados |\n")
                    f.write("| :--- | :--- |\n")
                    for col, tests in contract.items():
                        f.write(f"| `{col}` | {', '.join(tests)} |\n")
            
            f.write("\n---\n\n")
            
    logger.success(f"Catálogo forjado com sucesso: {catalog_path}")