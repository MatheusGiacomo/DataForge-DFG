# src/dfg/docs.py
import os
import json
import http.server
import socketserver
import webbrowser
from dfg.logging import logger

def docs_command(args):
    """
    Gera a documentação técnica e o grafo de linhagem (DAG) interativo.
    Se a flag --serve for passada, inicia um servidor web local.
    """
    # Registro para auditoria nos logs do sistema
    logger.info("Iniciando comando 'dfg docs'...")

    project_dir = os.getcwd()
    target_dir = os.path.join(project_dir, "target")
    manifest_path = os.path.join(target_dir, "manifest.json")

    # Verificação de segurança: O manifest é a base de tudo
    if not os.path.exists(manifest_path):
        logger.error("Arquivo manifest.json não encontrado. Rode 'dfg compile' ou 'dfg run' primeiro.")
        return

    # Lê a topologia do projeto gerada pelo motor
    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    # Prepara os dados para a biblioteca Vis.js (Frontend)
    nodes = []
    edges = []
    
    for model_name, info in manifest["nodes"].items():
        # Lógica visual: Diferenciamos a origem dos dados pelo formato e cor
        # Python (Ingestão/API) = Hexágono Azul | SQL (Transformação) = Database Verde
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

    # Template HTML (Design Moderno Dark Mode)
    html_content = f"""
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="UTF-8">
        <title>Data Forge - Linhagem de Dados</title>
        <script type="text/javascript" src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
        <style>
            body {{ background-color: #0f172a; color: #f8fafc; font-family: 'Segoe UI', sans-serif; margin: 0; padding: 0; overflow: hidden; }}
            #header {{ padding: 15px 25px; background-color: #1e293b; border-bottom: 1px solid #334155; display: flex; align-items: center; justify-content: space-between; }}
            #mynetwork {{ width: 100vw; height: calc(100vh - 80px); background-color: #0f172a; }}
            .legend {{ font-size: 12px; display: flex; gap: 20px; }}
            .dot {{ height: 10px; width: 10px; border-radius: 50%; display: inline-block; margin-right: 5px; }}
        </style>
    </head>
    <body>
        <div id="header">
            <h2 style="margin:0;">🔥 Data Forge <span style="font-weight: 300; font-size: 18px;">| Linhagem (DAG)</span></h2>
            <div class="legend">
                <div><span class="dot" style="background-color: #3b82f6;"></span> Ingestão (Python)</div>
                <div><span class="dot" style="background-color: #10b981;"></span> Modelo (SQL)</div>
            </div>
        </div>
        <div id="mynetwork"></div>
        <script type="text/javascript">
            var nodes = new vis.DataSet({json.dumps(nodes)});
            var edges = new vis.DataSet({json.dumps(edges)});
            var container = document.getElementById('mynetwork');
            var data = {{ nodes: nodes, edges: edges }};
            var options = {{
                layout: {{ hierarchical: {{ direction: "LR", sortMethod: "directed", nodeSpacing: 150 }} }},
                physics: {{ enabled: false }},
                nodes: {{ 
                    font: {{ color: '#ffffff', size: 14 }},
                    borderWidth: 2,
                    shadow: true 
                }},
                edges: {{ 
                    color: {{ color: '#64748b', highlight: '#3b82f6' }},
                    width: 2,
                    smooth: {{ type: 'cubicBezier' }}
                }}
            }};
            var network = new vis.Network(container, data, options);
        </script>
    </body>
    </html>
    """

    # Garante que a pasta target existe e salva o index.html
    os.makedirs(target_dir, exist_ok=True)
    html_path = os.path.join(target_dir, "index.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    logger.success("Documentação estática (index.html) gerada com sucesso.")

    # Inicia o servidor se solicitado
    if getattr(args, 'serve', False):
        PORT = 8080
        logger.info(f"Modo servidor ativado. Porta alvo: {PORT}")
        
        # Guardamos o diretório atual para voltar depois, se necessário
        original_dir = os.getcwd()
        os.chdir(target_dir) 
        
        Handler = http.server.SimpleHTTPRequestHandler
        
        try:
            with socketserver.TCPServer(("", PORT), Handler) as httpd:
                logger.info(f"Servidor disponível em http://localhost:{PORT}")
                webbrowser.open(f"http://localhost:{PORT}")
                httpd.serve_forever()
        except KeyboardInterrupt:
            logger.info("Servidor encerrado pelo usuário.")
        except Exception as e:
            logger.error(f"Erro ao iniciar servidor de documentação: {e}")
        finally:
            os.chdir(original_dir)