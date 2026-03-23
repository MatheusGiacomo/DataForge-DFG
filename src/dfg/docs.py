# src/dfg/docs.py
import os
import json
import http.server
import socketserver
import webbrowser
from dfg.logging import logger

def docs_command(args):
    project_dir = os.getcwd()
    target_dir = os.path.join(project_dir, "target")
    manifest_path = os.path.join(target_dir, "manifest.json")

    if not os.path.exists(manifest_path):
        logger.error("Arquivo manifest.json não encontrado. Rode 'dfg compile' ou 'dfg run' primeiro.")
        return

    # Lê a topologia do projeto
    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    # Prepara os dados para o Vis.js
    nodes = []
    edges = []
    
    for model_name, info in manifest["nodes"].items():
        # Cores diferentes: Python (Ingestão) = Azul, SQL (Transformação) = Verde
        color = "#3b82f6" if info["type"] == "python" else "#10b981"
        shape = "hexagon" if info["type"] == "python" else "database"
        
        nodes.append({
            "id": model_name,
            "label": model_name,
            "color": color,
            "shape": shape,
            "title": f"Tipo: {info['type'].upper()}<br>Materialização: {info['materialized']}"
        })

        for dep in info["depends_on"]:
            edges.append({"from": dep, "to": model_name, "arrows": "to"})

    # Template HTML embutido (Dark Mode elegante)
    html_content = f"""
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="UTF-8">
        <title>Data Forge - Linhagem de Dados</title>
        <script type="text/javascript" src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
        <style>
            body {{ background-color: #0f172a; color: #f8fafc; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 0; }}
            #header {{ padding: 20px; background-color: #1e293b; border-bottom: 1px solid #334155; }}
            #mynetwork {{ width: 100vw; height: calc(100vh - 80px); }}
        </style>
    </head>
    <body>
        <div id="header">
            <h2>🔥 Data Forge - Grafo de Linhagem (DAG)</h2>
        </div>
        <div id="mynetwork"></div>
        <script type="text/javascript">
            var nodes = new vis.DataSet({json.dumps(nodes)});
            var edges = new vis.DataSet({json.dumps(edges)});
            var container = document.getElementById('mynetwork');
            var data = {{ nodes: nodes, edges: edges }};
            var options = {{
                layout: {{ hierarchical: {{ direction: "LR", sortMethod: "directed" }} }},
                physics: {{ enabled: false }},
                nodes: {{ font: {{ color: '#ffffff' }} }}
            }};
            var network = new vis.Network(container, data, options);
        </script>
    </body>
    </html>
    """

    # Salva o index.html na pasta target
    html_path = os.path.join(target_dir, "index.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    logger.success("Grafo gerado com sucesso!")

    # Se a pessoa passou o comando `serve`, subimos o servidor
    if getattr(args, 'serve', False):
        PORT = 8080
        os.chdir(target_dir) # Muda o diretório para servir a pasta correta
        Handler = http.server.SimpleHTTPRequestHandler
        
        logger.info(f"Subindo servidor em http://localhost:{PORT}")
        webbrowser.open(f"http://localhost:{PORT}")
        
        try:
            with socketserver.TCPServer(("", PORT), Handler) as httpd:
                httpd.serve_forever()
        except KeyboardInterrupt:
            logger.info("\nServidor encerrado.")