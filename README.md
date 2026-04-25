# 🔥 DataForge (DFG)

> Motor de ELT em Python puro — dbt meets dlt. Simples, rápido, sem dependências pesadas.

[![PyPI version](https://badge.fury.io/py/dataforge-dfg.svg)](https://badge.fury.io/py/dataforge-dfg)
[![Python](https://img.shields.io/badge/Python-3.11%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Beta-orange)]()

O **DataForge** unifica ingestão e transformação de dados em uma única ferramenta de linha de comando. Escreva **modelos Python** para extrair dados de qualquer fonte e **modelos SQL com Jinja2** para transformá-los — tudo no mesmo pipeline, com DAG automático, testes, snapshots SCD2 e documentação visual incluídos.

```
pip install dataforge-dfg[duckdb]
```

---

## ⚡ Início em 60 segundos

```bash
# Instala com DuckDB
pip install dataforge-dfg[duckdb]

# Cria o projeto
mkdir meu_projeto && cd meu_projeto
dfg init

# Executa o pipeline
dfg run

# Visualiza o grafo de dependências
dfg docs --serve
```

---

## ✨ O que o DataForge faz

| Funcionalidade | Comando |
|---|---|
| Ingestão via Python (APIs, arquivos, DBs) | `dfg ingest` |
| Transformação SQL com Jinja2 + DAG automático | `dfg transform` |
| Pipeline completo (ELT) | `dfg run` |
| Testes de qualidade de dados | `dfg test` |
| Snapshots SCD Tipo 2 | `dfg snapshot` |
| Carga de CSVs estáticos | `dfg seed` |
| Documentação HTML com grafo interativo | `dfg docs` |
| Compilação Jinja2 (dry-run) | `dfg compile` |
| Diagnóstico do ambiente | `dfg debug` |
| Busca nos logs | `dfg log` |

---

## 🗄️ Bancos Suportados

```bash
pip install dataforge-dfg[duckdb]    # DuckDB
pip install dataforge-dfg[postgres]  # PostgreSQL
pip install dataforge-dfg[mysql]     # MySQL
pip install dataforge-dfg[all]       # Todos
# SQLite já vem embutido no Python
```

---

## 📝 Exemplo Rápido

**Modelo Python** — Ingere dados de uma API:
```python
# models/ingest_produtos.py
DEPENDENCIES = []

def model(context):
    import urllib.request, json
    with urllib.request.urlopen("https://api.exemplo.com/produtos") as r:
        return json.loads(r.read())["data"]
```

**Modelo SQL** — Transforma os dados ingeridos:
```sql
-- models/stg_produtos.sql
{{ config(materialized='table') }}

SELECT
    id          AS produto_id,
    UPPER(nome) AS nome,
    preco::FLOAT AS preco
FROM {{ ref('ingest_produtos') }}
WHERE ativo = true
```

**Contrato de dados** — Garante qualidade:
```yaml
# models/schema.yml
version: 1
models:
  - name: stg_produtos
    columns:
      - name: produto_id
        tests: [not_null, unique]
```

```bash
dfg run     # executa tudo
dfg test    # valida os contratos
```

---

## 📚 Documentação Completa

Consulte o arquivo [DOCUMENTACAO.md](DOCUMENTACAO.md) para o guia completo com todos os comandos, casos de uso e exemplos.

---

## 🤝 Contribuindo

Contribuições são bem-vindas! Veja [CONTRIBUTING.md](CONTRIBUTING.md) para as diretrizes.

```bash
git clone https://github.com/seu-usuario/dataforge
cd dataforge
python -m venv .venv && source .venv/bin/activate  # ou .venv\Scripts\Activate.ps1
pip install -e ".[dev]"
pytest
```

---

## 📄 Licença

MIT © Matheus — veja [LICENSE](LICENSE).
