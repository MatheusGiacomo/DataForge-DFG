# src/dfg/docs.py
"""
Comando 'dfg docs' do DataForge.

Gera a documentação técnica e o grafo de linhagem (DAG) interativo
em formato HTML usando a biblioteca Vis.js.

Uso:
    dfg docs           -> gera target/index.html
    dfg docs --serve   -> gera e serve em http://localhost:8080
"""
import http.server
import json
import os
import socketserver
import webbrowser

from dfg.logging import logger

_DEFAULT_PORT = 8080

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>DataForge — Linhagem de Dados</title>
  <script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
  <style>
    *,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
    :root{
      --bg:#08080e;--surface:#10101a;--surface2:#18182a;--surface3:#20203a;
      --border:#2a2a42;--borderhi:#3d3d60;
      --text:#e8e8ff;--textdim:#7070a0;--textmuted:#454565;
      --orange:#f97316;--orangedim:#7c3000;--orangeglow:rgba(249,115,22,.18);
      --purple:#a855f7;--purpledim:#4a1080;--purpleglow:rgba(168,85,247,.18);
      --hh:58px;--r:10px;
    }
    body{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;overflow:hidden;height:100vh}
    /* Header */
    #hdr{position:fixed;top:0;left:0;right:0;height:var(--hh);background:var(--surface);border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;padding:0 20px;z-index:100;gap:14px}
    #logo{display:flex;align-items:center;gap:9px;white-space:nowrap}
    #logo .icon{width:28px;height:28px;background:linear-gradient(135deg,var(--orange),var(--purple));border-radius:7px;display:grid;place-items:center;font-size:14px}
    #logo .name{font-size:14px;font-weight:700;background:linear-gradient(90deg,var(--orange),var(--purple));-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text}
    #logo .pipe{color:var(--borderhi);font-weight:300;-webkit-text-fill-color:var(--borderhi)}
    #logo .sub{font-size:12px;color:var(--textdim);-webkit-text-fill-color:var(--textdim)}
    /* Search */
    #swrap{flex:1;max-width:300px;position:relative}
    #search{width:100%;background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:7px 12px 7px 32px;color:var(--text);font-size:12px;outline:none;transition:border-color .2s,box-shadow .2s}
    #search::placeholder{color:var(--textmuted)}
    #search:focus{border-color:var(--purple);box-shadow:0 0 0 3px var(--purpleglow)}
    #sico{position:absolute;left:9px;top:50%;transform:translateY(-50%);color:var(--textmuted);font-size:13px;pointer-events:none}
    /* Stats */
    #stats{display:flex;gap:5px;align-items:center}
    .pill{display:flex;align-items:center;gap:4px;padding:3px 10px;border-radius:20px;font-size:11px;font-weight:600;border:1px solid;white-space:nowrap}
    .pill.o{background:var(--orangeglow);border-color:var(--orangedim);color:var(--orange)}
    .pill.p{background:var(--purpleglow);border-color:var(--purpledim);color:var(--purple)}
    .pill .d{width:5px;height:5px;border-radius:50%;background:currentColor}
    /* Controls */
    #ctrls{display:flex;gap:5px}
    .cb{background:var(--surface2);border:1px solid var(--border);border-radius:7px;color:var(--textdim);padding:5px 10px;font-size:12px;cursor:pointer;transition:all .15s;white-space:nowrap}
    .cb:hover{border-color:var(--borderhi);color:var(--text);background:var(--surface3)}
    .cb.on{border-color:var(--purple);color:var(--purple);background:var(--purpleglow)}
    /* Canvas */
    #g{position:fixed;top:var(--hh);left:0;right:0;bottom:0;background:var(--bg);background-image:radial-gradient(circle at 20% 30%,rgba(168,85,247,.04) 0%,transparent 55%),radial-gradient(circle at 80% 70%,rgba(249,115,22,.04) 0%,transparent 55%)}
    /* Tooltip override */
    .vis-tooltip{background:transparent!important;border:none!important;padding:0!important;box-shadow:none!important;pointer-events:none}
    /* Card */
    .fc{background:var(--surface);border:1px solid var(--borderhi);border-radius:var(--r);padding:13px 15px;min-width:210px;max-width:280px;box-shadow:0 4px 6px rgba(0,0,0,.4),0 16px 40px rgba(0,0,0,.65),0 0 0 1px rgba(255,255,255,.04) inset;font-family:'Segoe UI',system-ui,sans-serif;animation:cin .12s ease}
    @keyframes cin{from{opacity:0;transform:translateY(4px) scale(.97)}to{opacity:1;transform:translateY(0) scale(1)}}
    .fc .ch{display:flex;align-items:flex-start;justify-content:space-between;gap:8px;margin-bottom:9px}
    .fc .cn{font-size:13px;font-weight:700;color:var(--text);word-break:break-word;line-height:1.3}
    .fc .bg{display:inline-flex;align-items:center;gap:3px;padding:2px 7px;border-radius:20px;font-size:10px;font-weight:700;letter-spacing:.4px;text-transform:uppercase;white-space:nowrap;flex-shrink:0}
    .fc .bg.py{background:var(--orangeglow);border:1px solid var(--orangedim);color:var(--orange)}
    .fc .bg.sq{background:var(--purpleglow);border:1px solid var(--purpledim);color:var(--purple)}
    .fc .dv{height:1px;background:var(--border);margin:9px 0}
    .fc .cr{display:flex;justify-content:space-between;align-items:center;font-size:11px;margin-bottom:4px;gap:8px}
    .fc .cr:last-child{margin-bottom:0}
    .fc .cl{color:var(--textdim);flex-shrink:0}
    .fc .cv{color:var(--text);font-weight:500;text-align:right}
    .fc .mc{display:inline-block;padding:1px 7px;border-radius:4px;font-size:10px;font-weight:600;letter-spacing:.3px}
    .fc .mt{background:#1e3a2e;color:#4ade80;border:1px solid #166534}
    .fc .mv{background:#1e2a3a;color:#60a5fa;border:1px solid #1e40af}
    .fc .mi{background:#2a1e3a;color:#c084fc;border:1px solid #6b21a8}
    .fc .mm{background:#2a2a1e;color:#facc15;border:1px solid #854d0e}
    .fc .mx{background:#1e1e2a;color:#94a3b8;border:1px solid #334155}
    .fc .dl{display:flex;flex-wrap:wrap;gap:3px;margin-top:2px}
    .fc .dt{padding:1px 6px;background:var(--surface3);border:1px solid var(--border);border-radius:4px;font-size:10px;color:var(--textdim);font-family:'Courier New',monospace}
    .fc .dc{font-size:11px;color:var(--textdim);line-height:1.5;font-style:italic;margin-top:8px;padding-top:8px;border-top:1px solid var(--border)}
    /* Legend */
    #leg{position:fixed;bottom:16px;left:16px;background:var(--surface);border:1px solid var(--border);border-radius:var(--r);padding:9px 13px;display:flex;gap:13px;font-size:11px;color:var(--textdim);z-index:50}
    .li{display:flex;align-items:center;gap:5px}
    .ld{width:9px;height:9px}
    /* Toast */
    #toast{position:fixed;bottom:16px;right:16px;background:var(--surface2);border:1px solid var(--borderhi);border-radius:8px;padding:8px 13px;font-size:12px;color:var(--textdim);z-index:200;opacity:0;transform:translateY(6px);transition:all .2s;pointer-events:none}
    #toast.show{opacity:1;transform:translateY(0)}
  </style>
</head>
<body>
  <div id="hdr">
    <div id="logo">
      <div class="icon">🔥</div>
      <span class="name">DataForge</span>
      <span class="pipe">|</span>
      <span class="sub">Linhagem de Dados</span>
    </div>
    <div id="swrap">
      <span id="sico">🔍</span>
      <input id="search" type="text" placeholder="Filtrar modelos… (Esc para limpar)" autocomplete="off">
    </div>
    <div id="stats">
      <div class="pill o"><span class="d"></span><span id="cp">0</span> Python</div>
      <div class="pill p"><span class="d"></span><span id="cs">0</span> SQL</div>
    </div>
    <div id="ctrls">
      <button class="cb" id="bfit">⊡ Fit</button>
      <button class="cb" id="blay">⇄ Layout</button>
      <button class="cb on" id="bphy">◎ Physics</button>
    </div>
  </div>

  <div id="g"></div>

  <div id="leg">
    <div class="li"><span class="ld" style="background:var(--orange);border-radius:50%"></span>Python (ingestão)</div>
    <div class="li"><span class="ld" style="background:var(--purple)"></span>SQL (transformação)</div>
  </div>
  <div id="toast"></div>

  <script>
  var RN=__DFG_NODES__;
  var RE=__DFG_EDGES__;
  var C={orange:'#f97316',ob:'#c2410c',og:'rgba(249,115,22,.22)',purple:'#a855f7',pb:'#7c3aed',pg:'rgba(168,85,247,.22)',bg:'#08080e',s:'#10101a',border:'#2a2a42',bhi:'#3d3d60',text:'#e8e8ff',edge:'#3d3d60',ehi:'#a855f7'};

  function card(n){
    var py=n.modelType==='python';
    var badge=py?'<span class="bg py">⬡ Python</span>':'<span class="bg sq">⬡ SQL</span>';
    var mc='mx',ml=n.materialized||'—';
    if(ml==='table')mc='mt';else if(ml==='view')mc='mv';else if(ml==='incremental')mc='mi';else if(ml==='memory')mc='mm';
    var deps='';
    if(n.depends_on&&n.depends_on.length){
      var tags=n.depends_on.map(function(d){return'<span class="dt">'+d+'</span>'}).join('');
      deps='<div class="cr" style="align-items:flex-start"><span class="cl">Deps</span><div class="dl">'+tags+'</div></div>';
    }else{
      deps='<div class="cr"><span class="cl">Deps</span><span class="cv" style="color:var(--textmuted)">nenhuma</span></div>';
    }
    var desc=n.description?'<div class="dc">'+n.description+'</div>':'';
    return'<div class="fc"><div class="ch"><div class="cn">'+n.id+'</div>'+badge+'</div><div class="dv"></div><div class="cr"><span class="cl">Materialização</span><span class="mc '+mc+'">'+ml+'</span></div>'+deps+desc+'</div>';
  }

  function mkDS(rn,re){
    var ns=rn.map(function(n){
      var py=n.modelType==='python';
      var col=py?C.orange:C.purple,brd=py?C.ob:C.pb,gl=py?C.og:C.pg;
      return{id:n.id,label:n.id,title:card(n),shape:py?'hexagon':'box',
        color:{background:'#18182a',border:col,highlight:{background:'#20203a',border:col},hover:{background:'#20203a',border:col}},
        font:{color:col,size:13,face:"'Segoe UI',system-ui,sans-serif"},
        borderWidth:2,borderWidthSelected:3,
        shadow:{enabled:true,color:gl,size:12,x:0,y:0},
        widthConstraint:{minimum:100,maximum:190},
        heightConstraint:{minimum:34},
        margin:{top:8,bottom:8,left:12,right:12},
        _r:n};
    });
    var es=re.map(function(e,i){
      return{id:'e'+i,from:e.from,to:e.to,
        arrows:{to:{enabled:true,scaleFactor:.5,type:'arrow'}},
        color:{color:C.edge,highlight:C.ehi,hover:C.ehi,opacity:.8},
        width:1.5,smooth:{type:'cubicBezier',forceDirection:'horizontal',roundness:.4},
        selectionWidth:3,hoverWidth:2.5};
    });
    return{ns:ns,es:es};
  }

  var ds=mkDS(RN,RE);
  var ND=new vis.DataSet(ds.ns);
  var ED=new vis.DataSet(ds.es);
  var net=new vis.Network(document.getElementById('g'),{nodes:ND,edges:ED},{
    layout:{hierarchical:{enabled:true,direction:'LR',sortMethod:'directed',nodeSpacing:120,levelSeparation:210,treeSpacing:160,blockShifting:true,edgeMinimization:true,parentCentralization:true}},
    physics:{enabled:false},
    interaction:{hover:true,tooltipDelay:55,multiselect:true,navigationButtons:false,keyboard:false}
  });

  // stats
  document.getElementById('cp').textContent=RN.filter(function(n){return n.modelType==='python'}).length;
  document.getElementById('cs').textContent=RN.filter(function(n){return n.modelType==='sql'}).length;

  // select highlight
  net.on('selectNode',function(p){
    if(!p.nodes.length)return;
    var sel=p.nodes[0];
    var conn=net.getConnectedNodes(sel);conn.push(sel);
    var ce=net.getConnectedEdges(sel);
    ND.forEach(function(n){var ok=conn.indexOf(n.id)!==-1;var py=n._r.modelType==='python';ND.update({id:n.id,font:{color:ok?(py?C.orange:C.purple):'#2a2a40'},color:{border:ok?(py?C.orange:C.purple):'#181828'}})});
    ED.forEach(function(e){var ok=ce.indexOf(e.id)!==-1;ED.update({id:e.id,color:{color:ok?C.ehi:'#181828',opacity:ok?1:.2},width:ok?2.5:1})});
  });
  net.on('deselectNode',function(){var d=mkDS(RN,RE);ND.update(d.ns);ED.update(d.es)});

  // controls
  document.getElementById('bfit').addEventListener('click',function(){net.fit({animation:{duration:400,easingFunction:'easeInOutCubic'}})});
  var isLR=true;
  document.getElementById('blay').addEventListener('click',function(){isLR=!isLR;net.setOptions({layout:{hierarchical:{direction:isLR?'LR':'UD'}}});toast(isLR?'Layout: Esquerda→Direita':'Layout: Cima→Baixo')});
  var phOn=true;
  document.getElementById('bphy').addEventListener('click',function(){phOn=!phOn;net.setOptions({physics:{enabled:phOn}});this.classList.toggle('on',phOn);toast(phOn?'Física ativada':'Física desativada')});

  // search
  document.getElementById('search').addEventListener('input',function(){
    var q=this.value.toLowerCase().trim();
    if(!q){var d=mkDS(RN,RE);ND.update(d.ns);ED.update(d.es);return}
    var m=RN.filter(function(n){return n.id.toLowerCase().includes(q)}).map(function(n){return n.id});
    ND.forEach(function(n){var ok=m.indexOf(n.id)!==-1;var py=n._r.modelType==='python';ND.update({id:n.id,font:{color:ok?(py?C.orange:C.purple):'#252535'},color:{border:ok?(py?C.orange:C.purple):'#14141e'}})});
  });
  document.getElementById('search').addEventListener('keydown',function(e){if(e.key==='Escape'){this.value='';this.dispatchEvent(new Event('input'))}});

  // toast
  var tt;
  function toast(m){var el=document.getElementById('toast');el.textContent=m;el.classList.add('show');clearTimeout(tt);tt=setTimeout(function(){el.classList.remove('show')},1800)}

  net.once('afterDrawing',function(){net.fit({animation:false})});
  </script>
</body>
</html>
"""


def docs_command(args) -> None:
    """
    Gera a documentação HTML com o grafo de linhagem do projeto.

    Requer que o manifest.json tenha sido gerado previamente
    por 'dfg compile' ou 'dfg run'.
    """
    logger.info("Iniciando geração de documentação...")

    project_dir   = os.getcwd()
    target_dir    = os.path.join(project_dir, "target")
    manifest_path = os.path.join(target_dir, "manifest.json")

    if not os.path.exists(manifest_path):
        logger.error(
            "manifest.json não encontrado em 'target/'. "
            "Execute 'dfg compile' ou 'dfg run' primeiro."
        )
        return

    with open(manifest_path, encoding="utf-8") as f:
        manifest = json.load(f)

    nodes, edges = _build_vis_data(manifest)

    os.makedirs(target_dir, exist_ok=True)
    html_path = os.path.join(target_dir, "index.html")

    html_content = (
        _HTML_TEMPLATE
        .replace("__DFG_NODES__", json.dumps(nodes, ensure_ascii=False))
        .replace("__DFG_EDGES__", json.dumps(edges, ensure_ascii=False))
    )

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    logger.success(f"Documentação gerada: {html_path}")

    if getattr(args, "serve", False):
        _serve(target_dir)


def _build_vis_data(manifest: dict) -> tuple[list, list]:
    """Converte o manifest em listas de nós e arestas para o Vis.js."""
    nodes: list = []
    edges: list = []

    for model_name, info in manifest.get("nodes", {}).items():
        model_type   = info.get("type", "sql")
        materialized = info.get("materialized", "")
        description  = info.get("description", "")
        depends_on   = info.get("depends_on", [])

        nodes.append({
            "id":           model_name,
            "modelType":    model_type,
            "materialized": materialized,
            "description":  description,
            "depends_on":   depends_on,
        })

        for dep in depends_on:
            edges.append({"from": dep, "to": model_name})

    return nodes, edges


def _serve(target_dir: str) -> None:
    """Inicia um servidor HTTP estático na pasta target/."""
    original_dir = os.getcwd()
    os.chdir(target_dir)

    class _QuietHandler(http.server.SimpleHTTPRequestHandler):
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