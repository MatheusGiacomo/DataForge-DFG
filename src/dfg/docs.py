# src/dfg/docs.py
"""
Comando 'dfg docs' do DataForge.

Gera a documentação técnica e o grafo de linhagem (DAG) interativo
em formato HTML usando a biblioteca Vis.js.

Uso:
    dfg docs           → gera target/index.html
    dfg docs --serve   → gera e serve em http://localhost:8080
"""
import http.server
import json
import os
import socketserver
import webbrowser

from dfg.logging import logger

_DEFAULT_PORT = 8080

# ------------------------------------------------------------------
# Template HTML (Dark Mode + Vis.js)
# ------------------------------------------------------------------

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DataForge — Linhagem de Dados</title>
    <script type="text/javascript"
        src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js">
    </script>
    <style>
        *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            background-color: #0f172a;
            color: #f8fafc;
            font-family: 'Segoe UI', system-ui, sans-serif;
            overflow: hidden;
        }}
        #header {{
            padding: 14px 24px;
            background-color: #1e293b;
            border-bottom: 1px solid #334155;
            display: flex;
            align-items: center;
            justify-content: space-between;
            height: 60px;
        }}
        #header h2 {{ font-size: 18px; font-weight: 600; }}
        #header h2 span {{ font-weight: 300; color: #94a3b8; }}
        #mynetwork {{
            width: 100vw;
            height: calc(100vh - 60px);
            background-color: #0f172a;
        }}
        .legend {{
            display: flex;
            gap: 20px;
            font-size: 12px;
            color: #cbd5e1;
        }}
        .legend-item {{
            display: flex;
            align-items: center;
            gap: 6px;
        }}
        .dot {{
            width: 10px;
            height: 10px;
            border-radius: 50%;
            flex-shrink: 0;
        }}
    </style>
</head>
<body>
    <div id="header">
        <h2>🔥 DataForge <span>| Linhagem de Dados (DAG)</span></h2>
        <div class="legend">
            <div class="legend-item">
                <span class="dot" style="background:#3b82f6;"></span>
                Ingestão (Python)
            </div>
            <div class="legend-item">
                <span class="dot" style="background:#10b981;"></span>
                Transformação (SQL)
            </div>
        </div>
    </div>
    <div id="mynetwork"></div>
    <script type="text/javascript">
        var nodes = new vis.DataSet({nodes_json});
        var edges = new vis.DataSet({edges_json});
        var container = document.getElementById('mynetwork');
        var data = {{ nodes: nodes, edges: edges }};
        var options = {{
            layout: {{
                hierarchical: {{
                    direction: "LR",
                    sortMethod: "directed",
                    nodeSpacing: 160,
                    levelSeparation: 200
                }}
            }},
            physics: {{ enabled: false }},
            nodes: {{
                font: {{ color: '#f1f5f9', size: 13 }},
                borderWidth: 2,
                shadow: true
            }},
            edges: {{
                color: {{ color: '#475569', highlight: '#3b82f6' }},
                width: 2,
                smooth: {{ type: 'cubicBezier', forceDirection: 'horizontal' }},
                arrows: {{ to: {{ enabled: true, scaleFactor: 0.7 }} }}
            }},
            interaction: {{
                hover: true,
                tooltipDelay: 100
            }}
        }};
        new vis.Network(container, data, options);
    </script>
</body>
</html>
"""


# ------------------------------------------------------------------
# Comando principal
# ------------------------------------------------------------------


def docs_command(args) -> None:
    """
    Gera a documentação HTML com o grafo de linhagem do projeto.

    Requer que o manifest.json tenha sido gerado previamente
    por 'dfg compile' ou 'dfg run'.
    """
    logger.info("Iniciando geração de documentação...")

    project_dir = os.getcwd()
    target_dir = os.path.join(project_dir, "target")
    manifest_path = os.path.join(target_dir, "manifest.json")

    if not os.path.exists(manifest_path):
        logger.error(
            "manifest.json não encontrado em 'target/'. "
            "Execute 'dfg compile' ou 'dfg run' primeiro."
        )
        return

    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    nodes, edges = _build_vis_data(manifest)

    os.makedirs(target_dir, exist_ok=True)
    html_path = os.path.join(target_dir, "index.html")
    html_content = _HTML_TEMPLATE.format(
        nodes_json=json.dumps(nodes, ensure_ascii=False),
        edges_json=json.dumps(edges, ensure_ascii=False),
    )

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    logger.success(f"Documentação gerada: {html_path}")

    if getattr(args, "serve", False):
        _serve(target_dir)


def _build_vis_data(manifest: dict) -> tuple[list, list]:
    """Converte o manifest em listas de nós e arestas para o Vis.js."""
    nodes = []
    edges = []

    for model_name, info in manifest.get("nodes", {}).items():
        model_type = info.get("type", "unknown")
        materialized = info.get("materialized", "")
        description = info.get("description", "")

        color = "#3b82f6" if model_type == "python" else "#10b981"
        shape = "hexagon" if model_type == "python" else "database"

        tooltip_parts = [
            f"<b>{model_name}</b>",
            f"Tipo: {model_type.upper()}",
            f"Materialização: {materialized}",
        ]
        if description:
            tooltip_parts.append(f"Descrição: {description}")

        nodes.append({
            "id": model_name,
            "label": model_name,
            "color": {"background": color, "border": color},
            "shape": shape,
            "title": "<br>".join(tooltip_parts),
        })

        for dep in info.get("depends_on", []):
            edges.append({"from": dep, "to": model_name})

    return nodes, edges


def _serve(target_dir: str) -> None:
    """Inicia um servidor HTTP estático na pasta target/."""
    original_dir = os.getcwd()
    os.chdir(target_dir)

    handler = http.server.SimpleHTTPRequestHandler

    # Silencia o log padrão do SimpleHTTPRequestHandler
    class _QuietHandler(handler):
        def log_message(self, format, *args):  # noqa: A002
            pass

    try:
        with socketserver.TCPServer(("", _DEFAULT_PORT), _QuietHandler) as httpd:
            logger.info(f"Servidor disponível em: http://localhost:{_DEFAULT_PORT}")
            logger.info("Pressione Ctrl+C para encerrar.")
            webbrowser.open(f"http://localhost:{_DEFAULT_PORT}")
            httpd.serve_forever()
    except OSError as e:
        logger.error(
            f"Não foi possível iniciar o servidor na porta {_DEFAULT_PORT}: {e}. "
            f"Verifique se a porta está em uso."
        )
    except KeyboardInterrupt:
        logger.info("Servidor encerrado.")
    finally:
        os.chdir(original_dir)