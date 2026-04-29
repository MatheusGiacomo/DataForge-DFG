# DataForge (DFG)
> Motor de ELT em Python puro — Simples, Rápido, Sem Dependências Pesadas

[![Python](https://img.shields.io/badge/Python-3.11%2B-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Beta-orange)](CHANGELOG.md)

---

## Índice

- [O que é o DataForge?](#o-que-é-o-dataforge)
- [Por que usar o DataForge?](#por-que-usar-o-dataforge)
- [Instalação](#instalação)
- [Início Rápido](#início-rápido)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Configuração](#configuração)
  - [dfg_project.toml](#dfg_projecttoml)
  - [profiles.toml](#profilestoml)
  - [Variáveis de Ambiente no profiles.toml](#variáveis-de-ambiente-no-profilestoml)
  - [Bancos Suportados](#bancos-suportados)
- [Modelos SQL](#modelos-sql)
  - [Materializações](#materializações)
  - [Dependências com ref()](#dependências-com-ref)
  - [Configuração com config()](#configuração-com-config)
  - [Modelo Incremental](#modelo-incremental)
- [Modelos Python](#modelos-python)
  - [Estrutura de um Modelo Python](#estrutura-de-um-modelo-python)
  - [Estado Incremental com State](#estado-incremental-com-state)
  - [Dependências entre Modelos Python e SQL](#dependências-entre-modelos-python-e-sql)
- [Sources — Conectores Nativos de Ingestão](#sources--conectores-nativos-de-ingestão)
  - [RestSource](#restsource)
  - [Autenticação HTTP](#autenticação-http)
  - [Paginação Automática](#paginação-automática)
  - [FileSource](#filesource)
  - [DatabaseSource](#databasesource)
  - [S3Source](#s3source)
  - [GCSSource](#gcssource)
  - [AzureBlobSource](#azureblobsource)
  - [fetch_many — Ingestão em Batch](#fetch_many--ingestão-em-batch)
  - [Variáveis de Ambiente nas Sources](#variáveis-de-ambiente-nas-sources)
  - [Retry Automático](#retry-automático)
- [Contratos de Dados (schema.yml)](#contratos-de-dados-schemayml)
- [Seeds — Dados Estáticos](#seeds--dados-estáticos)
- [Snapshots — SCD Tipo 2](#snapshots--scd-tipo-2)
- [Referência de Comandos](#referência-de-comandos)
  - [dfg init](#dfg-init)
  - [dfg run](#dfg-run)
  - [dfg ingest](#dfg-ingest)
  - [dfg transform](#dfg-transform)
  - [dfg test](#dfg-test)
  - [dfg compile](#dfg-compile)
  - [dfg seed](#dfg-seed)
  - [dfg snapshot](#dfg-snapshot)
  - [dfg docs](#dfg-docs)
  - [dfg debug](#dfg-debug)
  - [dfg log](#dfg-log)
- [DAG — Grafo de Dependências](#dag--grafo-de-dependências)
- [Paralelismo](#paralelismo)
- [Observabilidade](#observabilidade)
  - [Logs](#logs)
  - [manifest.json](#manifestjson)
  - [run_results.json](#run_resultsjson)
- [Casos de Uso Completos](#casos-de-uso-completos)
  - [Pipeline de E-commerce](#pipeline-de-e-commerce)
  - [Ingestão de API com Estado](#ingestão-de-api-com-estado)
  - [Ingestão de Arquivos do S3](#ingestão-de-arquivos-do-s3)
  - [Replicação entre Bancos com CDC Incremental](#replicação-entre-bancos-com-cdc-incremental)
  - [Auditoria com Snapshot SCD2](#auditoria-com-snapshot-scd2)
- [Boas Práticas](#boas-práticas)
- [Solução de Problemas](#solução-de-problemas)

---

## O que é o DataForge?

O **DataForge** é um motor de ELT (Extract, Load, Transform) construído inteiramente em Python puro, com o mínimo possível de dependências externas. Ele combina as melhores ideias do **dbt** (transformações SQL declarativas, testes, documentação) com as do **dlt** (ingestão de dados via Python, schema evolution automática, estado incremental).

Com o DataForge você escreve seus pipelines de dados usando **SQL e Python** como cidadãos de primeira classe — sem YAML excessivo, sem configurações complexas, sem aprender um novo framework.

```
Fonte de Dados          DataForge                  Banco de Dados
─────────────           ─────────────              ──────────────
APIs REST       ──►     Sources Nativas  ──►        DuckDB
AWS S3 / GCS    ──►     (RestSource,     ──►        PostgreSQL
Azure Blob      ──►      FileSource,     ──►        MySQL
Bancos legados  ──►      S3Source...)               SQLite
Arquivos CSV    ──►
                        Modelos SQL      ──►
                        (transformação)

                        Testes, Docs,
                        Snapshots, Seeds
```

---

## Por que usar o DataForge?

| Característica | DataForge | dbt | dlt |
|---|---|---|---|
| Transformações SQL | ✅ | ✅ | ❌ |
| Ingestão via Python | ✅ | ❌ | ✅ |
| Sources nativas (REST, S3, GCS...) | ✅ | ❌ | ✅ |
| Schema Evolution | ✅ | ❌ | ✅ |
| Estado Incremental | ✅ | ❌ | ✅ |
| Snapshots SCD2 | ✅ | ✅ | ❌ |
| Testes de dados | ✅ | ✅ | ❌ |
| Documentação HTML | ✅ | ✅ | ❌ |
| Zero dependências pesadas | ✅ | ❌ | ❌ |
| Pipeline único (ELT) | ✅ | ❌ | ❌ |

**Dependências obrigatórias:**
- `jinja2` — renderização dos templates SQL
- `pyyaml` — leitura dos arquivos de contrato

---

## Instalação

**Pré-requisito:** Python 3.11 ou superior.

```bash
# 1. Clone ou baixe o repositório
git clone https://github.com/seu-usuario/dataforge.git
cd dataforge

# 2. Crie e ative um ambiente virtual (recomendado)
python -m venv .venv

# Windows
.venv\Scripts\Activate.ps1

# Linux / macOS
source .venv/bin/activate

# 3. Instale o DataForge
pip install -e .

# 4. Instale o driver do banco de dados desejado
pip install duckdb           # DuckDB (recomendado para começar)
pip install psycopg2-binary  # PostgreSQL
pip install mysql-connector-python # MySQL
# SQLite já vem embutido no Python

# 5. Instale os SDKs de cloud storage (apenas o que você usar)
pip install boto3                    # AWS S3, MinIO, Cloudflare R2
pip install google-cloud-storage     # Google Cloud Storage
pip install azure-storage-blob       # Azure Blob Storage
pip install pyarrow                  # Leitura de arquivos Parquet

# 6. Verifique a instalação
dfg --help
```

---

## Início Rápido

```bash
mkdir meu_pipeline && cd meu_pipeline
dfg init
dfg run
```

O `dfg init` detecta automaticamente os drivers instalados e guia você pelo setup inicial, gerando `dfg_project.toml`, `profiles.toml` e a estrutura de pastas do projeto.

---

## Estrutura do Projeto

```
meu_pipeline/
│
├── dfg_project.toml        # Configuração do projeto (nome, profile, threads)
├── profiles.toml           # Credenciais de conexão por ambiente
├── .dfg_state.json         # Estado incremental (gerado automaticamente)
│
├── models/                 # Modelos de transformação e ingestão
│   ├── schema.yml          # Contratos de dados (testes, descrições)
│   ├── stg_users.sql       # Modelo SQL de staging
│   ├── fct_vendas.sql      # Modelo SQL de transformação
│   └── ingest_api.py       # Modelo Python de ingestão
│
├── snapshots/              # Snapshots SCD Tipo 2
│   └── hist_clientes.sql
│
├── seeds/                  # Dados estáticos (CSV)
│   └── paises.csv
│
├── logs/
│   └── dfg.log             # Log persistente de todas as execuções
│
└── target/                 # Artefatos gerados (não versionar)
    ├── manifest.json
    ├── run_results.json
    ├── index.html
    └── compiled/
```

---

## Configuração

### dfg_project.toml

```toml
[project]
name    = "meu_pipeline"
profile = "meu_pipeline"
target  = "dev"
threads = 4
```

---

### profiles.toml

Define as credenciais de conexão por ambiente. Adicione ao `.gitignore`.

```toml
[meu_pipeline]
target = "dev"

[meu_pipeline.outputs.dev]
type     = "duckdb"
database = "meu_pipeline_dev.db"

[meu_pipeline.outputs.prod]
type     = "postgres"
host     = "db.meuservidor.com"
port     = 5432
user     = "admin"
password = "senha_secreta"
database = "analytics"
```

---

### Variáveis de Ambiente no profiles.toml

A partir da v0.2.0, qualquer valor no `profiles.toml` suporta a sintaxe `{{ env('NOME_DA_VARIAVEL') }}`. O DataForge resolve essas referências automaticamente antes de passar as credenciais para o driver.

```toml
[meu_pipeline.outputs.prod]
type     = "postgres"
host     = "{{ env('DB_HOST') }}"
port     = 5432
user     = "{{ env('DB_USER') }}"
password = "{{ env('DB_PASSWORD') }}"
database = "analytics"
```

Defina as variáveis antes de executar:

```bash
# Linux / macOS
export DB_HOST=db.meuservidor.com
export DB_USER=admin
export DB_PASSWORD=minha_senha_segura

# Windows (PowerShell)
$env:DB_HOST = "db.meuservidor.com"
$env:DB_USER = "admin"
$env:DB_PASSWORD = "minha_senha_segura"
```

Se uma variável referenciada não estiver definida, o DataForge encerra com uma mensagem clara indicando qual está faltando.

---

### Bancos Suportados

| Banco | Tipo no profiles.toml | Driver Python |
|---|---|---|
| DuckDB | `duckdb` | `pip install duckdb` |
| PostgreSQL | `postgres` ou `postgresql` | `pip install psycopg2-binary` |
| MySQL | `mysql` | `pip install mysql-connector-python` |
| SQLite | `sqlite` | Nativo (sem instalação) |

---

## Modelos SQL

Modelos SQL são arquivos `.sql` dentro de `models/`, compilados via Jinja2.

### Materializações

| Tipo | Comportamento | Quando usar |
|---|---|---|
| `table` | DROP + CREATE TABLE AS SELECT | Query pesada, resultado persistido |
| `view` | DROP + CREATE VIEW AS SELECT | Dados acessados raramente |
| `incremental` | Insere apenas linhas novas | Tabelas grandes de eventos/logs |

```sql
-- models/stg_pedidos.sql
{{ config(materialized='table') }}

SELECT
    id             AS pedido_id,
    cliente_id,
    valor_total,
    status,
    criado_em
FROM raw_pedidos
WHERE status != 'cancelado'
```

---

### Dependências com ref()

```sql
-- models/fct_resumo.sql
{{ config(materialized='table') }}

SELECT c.nome, COUNT(p.pedido_id) AS total_pedidos
FROM {{ ref('stg_clientes') }} c
JOIN {{ ref('stg_pedidos') }} p ON c.id = p.cliente_id
GROUP BY c.nome
```

O DataForge garante que `stg_clientes` e `stg_pedidos` sejam executados antes de `fct_resumo`.

---

### Configuração com config()

```sql
{{ config(
    materialized='table',
    description='Fatos de vendas por dia'
) }}

SELECT ...
```

---

### Modelo Incremental

```sql
-- models/eventos_app.sql
{{
    config(
        materialized='incremental',
        unique_key='evento_id'
    )
}}

SELECT evento_id, usuario_id, tipo_evento, ocorreu_em
FROM raw_eventos_app
```

Na primeira execução cria a tabela. Nas seguintes, remove os registros com `unique_key` existente e insere os novos.

---

## Modelos Python

Modelos Python são arquivos `.py` em `models/` para a fase de Extract & Load. A partir da v0.2.0, use o módulo `dfg.sources` para eliminar código repetitivo de conexão e paginação.

### Estrutura de um Modelo Python

```python
# models/ingest_produtos.py
DEPENDENCIES = []

def model(context):
    import urllib.request, json
    with urllib.request.urlopen("https://api.exemplo.com/produtos") as r:
        return json.loads(r.read())["data"]
```

**O dicionário `context`:**

| Chave | Tipo | Descrição |
|---|---|---|
| `context["config"]` | `dict` | Configurações completas do projeto |
| `context["ref"]` | `callable` | Dados de outro modelo já executado |
| `context["state"]` | `any` | Estado persistido da última execução |
| `context["set_state"]` | `callable` | Persiste estado para a próxima execução |

---

### Estado Incremental com State

```python
# models/ingest_pedidos.py
DEPENDENCIES = []

def model(context):
    import urllib.request, json
    since = context["state"] or "2020-01-01T00:00:00Z"
    url = f"https://api.loja.com/pedidos?since={since}"

    with urllib.request.urlopen(url) as r:
        pedidos = json.loads(r.read())["pedidos"]

    if pedidos:
        context["set_state"](max(p["atualizado_em"] for p in pedidos))

    return pedidos
```

---

### Dependências entre Modelos Python e SQL

```python
# models/enriquecer_clientes.py
DEPENDENCIES = ["stg_clientes"]

def model(context):
    base = context["ref"]("stg_clientes")
    return [{**c, "score": calcular_score(c["cpf"])} for c in (base or [])]
```

---

## Sources — Conectores Nativos de Ingestão

O módulo `dfg.sources` (v0.2.0) fornece conectores declarativos e reutilizáveis para as fontes mais comuns. Cada source inclui retry automático com backoff exponencial, suporte a `{{ env('VAR') }}` e integração direta com o mecanismo de estado incremental.

```python
# Antes (v0.1.0) — código repetitivo
def model(context):
    import urllib.request, json
    req = urllib.request.Request(
        "https://api.exemplo.com/data",
        headers={"Authorization": "Bearer TOKEN"}
    )
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())["data"]

# Agora (v0.2.0) — declarativo e reutilizável
from dfg.sources import RestSource

source = RestSource(
    base_url="https://api.exemplo.com",
    auth={"type": "bearer", "token": "{{ env('API_TOKEN') }}"},
)

def model(context):
    return source.get("/data", extract_path="data")
```

---

### RestSource

Conector para APIs REST com suporte a todos os métodos HTTP, autenticação e paginação automática.

```python
from dfg.sources import RestSource

source = RestSource(
    base_url="https://api.exemplo.com",
    auth={"type": "bearer", "token": "{{ env('API_TOKEN') }}"},
    pagination={"type": "offset", "page_size": 200},
    headers={"Accept-Language": "pt-BR"},
    timeout=30,
    rate_limit_rps=5.0,
)

def model(context):
    return source.get("/v1/clientes", extract_path="data.items")
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|---|---|---|
| `base_url` | `str` | URL base da API. Suporta `{{ env() }}` |
| `auth` | `dict` ou `AuthStrategy` | Configuração de autenticação |
| `pagination` | `dict` ou `PaginationStrategy` | Estratégia de paginação |
| `headers` | `dict` | Headers HTTP adicionais fixos |
| `timeout` | `int` | Timeout em segundos (padrão: 30) |
| `rate_limit_rps` | `float` | Máximo de requests por segundo |
| `max_retries` | `int` | Tentativas em caso de falha (padrão: 3) |

**Métodos:**

```python
# GET com parâmetros e extração de path aninhado
source.get("/endpoint", params={"status": "active"}, extract_path="results")

# POST (APIs GraphQL, filtros via body)
source.post("/graphql", body={"query": "{ users { id name } }"}, extract_path="data.users")
```

O `extract_path` usa notação de ponto para navegar em respostas aninhadas:

```python
# Resposta: {"meta": {"total": 500}, "data": {"items": [...]}}
source.get("/produtos", extract_path="data.items")
```

---

### Autenticação HTTP

**Bearer Token:**

```python
auth = {"type": "bearer", "token": "{{ env('API_TOKEN') }}"}
```

**API Key via header ou query parameter:**

```python
# Via header
auth = {"type": "api_key", "header": "X-API-Key", "key": "{{ env('API_KEY') }}"}

# Via query parameter (?api_key=...)
auth = {"type": "api_key", "param": "api_key", "key": "{{ env('API_KEY') }}"}
```

**HTTP Basic Authentication:**

```python
auth = {
    "type": "basic",
    "username": "{{ env('API_USER') }}",
    "password": "{{ env('API_PASS') }}",
}
```

**OAuth2 Client Credentials (renovação automática de token):**

```python
auth = {
    "type": "oauth2",
    "client_id":     "{{ env('CLIENT_ID') }}",
    "client_secret": "{{ env('CLIENT_SECRET') }}",
    "token_url":     "https://auth.exemplo.com/oauth/token",
    "scopes":        ["read:data", "read:users"],
}
```

O `OAuth2Auth` obtém o token automaticamente, usa cache enquanto válido e renova com 30 segundos de antecedência da expiração.

---

### Paginação Automática

O `RestSource` itera por todas as páginas automaticamente até que não haja mais dados.

**Offset** — `?offset=0&limit=100`:

```python
pagination = {"type": "offset", "page_size": 100}

# Com campo total na resposta
pagination = {
    "type": "offset",
    "page_size": 100,
    "limit_param": "limit",
    "offset_param": "offset",
    "total_path": "meta.total",
}
```

**Número de Página** — `?page=1&per_page=100`:

```python
pagination = {"type": "page_number", "page_size": 100}
```

**Cursor** — `?cursor=<token>` (Twitter, Shopify, Stripe):

```python
pagination = {
    "type": "cursor",
    "cursor_param": "after",
    "cursor_path": "meta.next_cursor",
}
```

**Link Header** — RFC 5988 (GitHub, GitLab, Jira):

```python
# API retorna: Link: <https://api.exemplo.com/items?page=2>; rel="next"
pagination = {"type": "link_header"}
```

**Next URL** — próxima URL no corpo da resposta:

```python
# API retorna: {"next": "https://api.exemplo.com/items?cursor=xyz", "data": [...]}
pagination = {"type": "next_url", "next_url_path": "next"}
```

---

### FileSource

Lê arquivos locais ou remotos (HTTP/HTTPS) nos formatos CSV, JSON, JSONL e Parquet.

```python
from dfg.sources import FileSource

# CSV local
source = FileSource(path="/dados/clientes.csv")

# JSON remoto com extração de path aninhado
source = FileSource(
    path="https://api.exemplo.com/export/produtos.json",
    extract_path="data.products",
)

# Parquet local (requer: pip install pyarrow)
source = FileSource(path="/dados/vendas.parquet")

def model(context):
    return source.fetch()
```

**Formatos suportados:**

| Extensão | Formato | Observações |
|---|---|---|
| `.csv`, `.tsv` | CSV | Inferência automática de tipos, suporte a BOM do Excel |
| `.json` | JSON | Lista no topo nível ou aninhada via `extract_path` |
| `.jsonl`, `.ndjson` | JSON Lines | Um objeto por linha, linhas em branco ignoradas |
| `.parquet` | Parquet | Requer `pip install pyarrow` |

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|---|---|---|
| `path` | `str` | Caminho local ou URL remota. Suporta `{{ env() }}` |
| `format` | `str` | Formato explícito quando a extensão não é suficiente |
| `extract_path` | `str` | Notação de ponto para extrair lista de JSON aninhado |
| `encoding` | `str` | Encoding do arquivo (padrão: `utf-8-sig`) |
| `delimiter` | `str` | Delimitador CSV (padrão: `,`) |

---

### DatabaseSource

Copia dados entre bancos de dados, reutilizando os adaptadores já configurados no DataForge.

```python
from dfg.sources import DatabaseSource

source = DatabaseSource(
    connection={
        "type":     "postgres",
        "host":     "{{ env('LEGADO_DB_HOST') }}",
        "database": "erp_producao",
        "user":     "{{ env('LEGADO_DB_USER') }}",
        "password": "{{ env('LEGADO_DB_PASSWORD') }}",
    },
    query="SELECT id, nome, email, criado_em FROM clientes WHERE ativo = TRUE",
)

def model(context):
    return source.fetch()
```

**Extração incremental com parâmetros nomeados:**

```python
source = DatabaseSource(
    connection={...},
    query="SELECT * FROM pedidos WHERE atualizado_em > :since ORDER BY atualizado_em ASC",
)

def model(context):
    since = context["state"] or "2020-01-01"
    records = source.fetch(since=since)

    if records:
        context["set_state"](str(max(r["atualizado_em"] for r in records)))

    return records
```

Os placeholders `:param_name` são substituídos pelos keyword arguments passados ao `fetch()`.

---

### S3Source

Lê objetos do AWS S3 e de qualquer storage S3-compatível (MinIO, Cloudflare R2, DigitalOcean Spaces, Backblaze B2).

**Instalação:** `pip install boto3`

```python
from dfg.sources.cloud import S3Source

# AWS S3
source = S3Source(
    uri="s3://meu-bucket/dados/clientes.parquet",
    region="sa-east-1",
    access_key="{{ env('AWS_ACCESS_KEY_ID') }}",
    secret_key="{{ env('AWS_SECRET_ACCESS_KEY') }}",
)

# MinIO (S3-compatível)
source = S3Source(
    uri="s3://meu-bucket/dados/vendas.csv",
    endpoint_url="http://localhost:9000",
    access_key="{{ env('MINIO_USER') }}",
    secret_key="{{ env('MINIO_PASSWORD') }}",
)

# IAM Role (EC2, Lambda, ECS — sem credenciais explícitas)
source = S3Source(uri="s3://meu-bucket/dados/arquivo.json", region="us-east-1")

def model(context):
    return source.fetch()
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|---|---|---|
| `uri` | `str` | URI do objeto: `s3://bucket/path/arquivo.csv` |
| `region` | `str` | Região AWS (ex: `sa-east-1`) |
| `access_key` | `str` | AWS Access Key ID. Suporta `{{ env() }}` |
| `secret_key` | `str` | AWS Secret Access Key. Suporta `{{ env() }}` |
| `session_token` | `str` | Token STS para credenciais temporárias |
| `endpoint_url` | `str` | Endpoint para storages S3-compatíveis |

---

### GCSSource

Lê objetos do Google Cloud Storage.

**Instalação:** `pip install google-cloud-storage`

```python
from dfg.sources.cloud import GCSSource

# Via service account JSON
source = GCSSource(
    uri="gs://meu-bucket/dados/clientes.csv",
    credentials_path="{{ env('GOOGLE_SA_KEY_PATH') }}",
)

# Via Application Default Credentials (gcloud auth, Workload Identity)
source = GCSSource(uri="gs://meu-bucket/dados/vendas.parquet")

def model(context):
    return source.fetch()
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|---|---|---|
| `uri` | `str` | URI do objeto: `gs://bucket/path/arquivo.json` |
| `credentials_path` | `str` | Caminho para o JSON da service account. Suporta `{{ env() }}` |
| `project` | `str` | ID do projeto GCP (geralmente inferido das credenciais) |

---

### AzureBlobSource

Lê blobs do Azure Blob Storage. No URI `az://`, o host corresponde ao nome do **container**.

**Instalação:** `pip install azure-storage-blob`

```python
from dfg.sources.cloud import AzureBlobSource

# Via connection string
source = AzureBlobSource(
    uri="az://meu-container/dados/clientes.csv",
    connection_string="{{ env('AZURE_STORAGE_CONNECTION_STRING') }}",
)

# Via account name + key
source = AzureBlobSource(
    uri="az://meu-container/dados/vendas.json",
    account_name="{{ env('AZURE_STORAGE_ACCOUNT') }}",
    account_key="{{ env('AZURE_STORAGE_KEY') }}",
)

# Via SAS Token
source = AzureBlobSource(
    uri="az://meu-container/arquivo.parquet",
    account_name="{{ env('AZURE_STORAGE_ACCOUNT') }}",
    sas_token="{{ env('AZURE_SAS_TOKEN') }}",
)

def model(context):
    return source.fetch()
```

Para autenticação via Managed Identity ou Azure CLI, instale também `pip install azure-identity` e use apenas `account_name` sem credenciais.

---

### fetch_many — Ingestão em Batch

Todos os conectores de cloud storage suportam `fetch_many()` para listar e processar múltiplos objetos com filtro glob.

```python
source = S3Source(
    uri="s3://analytics-bucket/",
    region="sa-east-1",
    access_key="{{ env('AWS_ACCESS_KEY_ID') }}",
    secret_key="{{ env('AWS_SECRET_ACCESS_KEY') }}",
)

def model(context):
    # Lê todos os CSVs do prefixo e retorna uma lista única mesclada
    return source.fetch_many(
        prefix="exports/pedidos/2024/",
        pattern="*.csv",
        merge=True,  # padrão
    )
```

Com `merge=False`, retorna um dicionário `{chave: registros}`:

```python
resultado = source.fetch_many(prefix="exports/", pattern="*.json", merge=False)
# {"exports/jan.json": [...], "exports/fev.json": [...]}
```

O `pattern` usa glob: `*.csv` corresponde a qualquer `.csv`, `vendas_2024*.json` filtra por prefixo de nome.

---

### Variáveis de Ambiente nas Sources

Todas as sources suportam `{{ env('VAR') }}` em qualquer parâmetro de string, resolvido no momento da instanciação:

```python
source = RestSource(
    base_url="{{ env('API_BASE_URL') }}",
    auth={
        "type":          "oauth2",
        "client_id":     "{{ env('OAUTH_CLIENT_ID') }}",
        "client_secret": "{{ env('OAUTH_CLIENT_SECRET') }}",
        "token_url":     "{{ env('OAUTH_TOKEN_URL') }}",
    },
)
```

Se uma variável não estiver definida, o DataForge encerra com:

```
error  Variável de ambiente 'OAUTH_CLIENT_SECRET' não está definida.
       Defina-a antes de executar: export OAUTH_CLIENT_SECRET=seu_valor
```

---

### Retry Automático

Todas as sources herdam retry com **backoff exponencial** e **jitter aleatório**:

```
Tentativa 1 → falha → aguarda ~1.0s
Tentativa 2 → falha → aguarda ~2.0s
Tentativa 3 → falha → propaga a exceção
```

O jitter (até 10% aleatório) distribui as tentativas paralelas para evitar sobrecarga simultânea.

Os parâmetros são configuráveis:

```python
source = RestSource(
    base_url="https://api.instavel.com",
    max_retries=5,
    retry_delay=2.0,
    retry_backoff=3.0,
)
```

---

## Contratos de Dados (schema.yml)

```yaml
version: 1

models:
  - name: stg_pedidos
    description: "Pedidos limpos oriundos do sistema transacional."
    columns:
      - name: pedido_id
        tests:
          - not_null
          - unique
      - name: valor_total
        tests:
          - not_null
```

```bash
dfg test
```

Testes disponíveis: `not_null`, `unique`. Encerra com código `1` em caso de falha — integrável com CI/CD.

---

## Seeds — Dados Estáticos

```csv
# seeds/paises.csv
codigo,nome,continente
BR,Brasil,América do Sul
US,Estados Unidos,América do Norte
```

```bash
dfg seed
```

Comportamento: inferência automática de tipos, suporte a BOM do Excel, Drop & Replace idempotente.

---

## Snapshots — SCD Tipo 2

```sql
{% snapshot hist_clientes %}

{{
    config(
        unique_key='cliente_id',
        updated_at='atualizado_em'
    )
}}

SELECT cliente_id, nome, email, plano, atualizado_em
FROM {{ ref('stg_clientes') }}

{% endsnapshot %}
```

```bash
dfg snapshot
```

**Colunas de controle adicionadas automaticamente:**

| Coluna | Tipo | Descrição |
|---|---|---|
| `dfg_valid_from` | TIMESTAMP | Quando esta versão entrou em vigor |
| `dfg_valid_to` | TIMESTAMP | Quando foi substituída (NULL = atual) |
| `dfg_is_active` | BOOLEAN | TRUE para a versão mais recente |
| `dfg_updated_at` | TIMESTAMP | Timestamp da última operação |

**Consultas históricas:**

```sql
-- Estado atual
SELECT * FROM hist_clientes WHERE dfg_is_active = TRUE;

-- Estado em uma data passada
SELECT * FROM hist_clientes
WHERE cliente_id = 1
  AND dfg_valid_from <= '2024-02-01'
  AND (dfg_valid_to > '2024-02-01' OR dfg_valid_to IS NULL);
```

---

## Referência de Comandos

### dfg init
Inicializa um novo projeto no diretório atual. Detecta drivers instalados e gera `dfg_project.toml`, `profiles.toml` e a estrutura de pastas.

```bash
dfg init
```

---

### dfg run
Executa o pipeline completo: ingestão (Python) + transformação (SQL), respeitando o DAG.

```bash
dfg run
```

---

### dfg ingest
Executa apenas os modelos Python (Extract & Load). Ignora modelos SQL.

```bash
dfg ingest
```

---

### dfg transform
Executa apenas os modelos SQL. Ignora modelos Python.

```bash
dfg transform
```

---

### dfg test
Valida os contratos de dados do `schema.yml` contra os dados no banco.

```bash
dfg test
```

---

### dfg compile
Renderiza os templates Jinja2 e salva em `target/compiled/`. Não executa no banco (Dry Run).

```bash
dfg compile
```

---

### dfg seed
Carrega todos os CSVs de `seeds/` no banco (Drop & Replace).

```bash
dfg seed
```

---

### dfg snapshot
Processa todos os snapshots de `snapshots/` com lógica SCD Tipo 2.

```bash
dfg snapshot
```

---

### dfg docs
Gera documentação HTML com grafo de linhagem interativo.

```bash
dfg docs           # gera target/index.html
dfg docs --serve   # gera e serve em http://localhost:8080
```

---

### dfg debug
Diagnóstico do ambiente: configurações, credenciais e conexão com o banco.

```bash
dfg debug
```

---

### dfg log
Busca registros no arquivo de log por ID de sessão (`DDMMAADFG`).

```bash
dfg log 150426DFG
dfg log 150426DFG --run
dfg log 150426DFG --run -d   # exporta para .txt
```

Flags disponíveis: `--run`, `--ingest`, `--transform`, `--test`, `--compile`, `--docs`, `--snapshot`, `--seed`, `-d`/`--dump`.

---

## DAG — Grafo de Dependências

O DataForge constrói um DAG a partir das declarações `{{ ref() }}` (SQL) e `DEPENDENCIES` (Python). A execução segue ordenação topológica automática com detecção de ciclos.

```
ingest_clientes (Python)  ──►  stg_clientes (SQL)  ──►  fct_resumo (SQL)
ingest_pedidos (Python)   ──►  stg_pedidos  (SQL)  ──┘
```

---

## Paralelismo

Modelos independentes no mesmo nível do DAG são executados em paralelo via `ThreadPoolExecutor`. Cada thread tem sua própria conexão com o banco.

```toml
[project]
threads = 4   # padrão
```

---

## Observabilidade

### Logs

Registros em `logs/dfg.log`, organizados por sessão diária com ID `DDMMAADFG`.

**Níveis:** `debug` (cinza), `info` (azul), `invocation` (laranja), `ok` (verde), `warning` (amarelo), `error` (vermelho).

---

### manifest.json

Topologia do projeto gerada em `target/manifest.json` após `dfg compile` ou `dfg run`.

```json
{
    "metadata": {"generated_at": "...", "tool": "DataForge (DFG)"},
    "nodes": {
        "stg_pedidos": {
            "type": "sql", "materialized": "table",
            "description": "...", "depends_on": ["ingest_pedidos"]
        }
    },
    "dependencies": {"stg_pedidos": ["ingest_pedidos"]}
}
```

---

### run_results.json

Estatísticas da execução em `target/run_results.json`.

```json
{
    "metadata": {"generated_at": "...", "command": "dfg run"},
    "summary": {"total": 5, "success": 5, "error": 0, "skipped": 0},
    "results": [
        {"model": "ingest_pedidos", "status": "success", "execution_time": 3.124, "rows": 1502}
    ]
}
```

---

## Casos de Uso Completos

### Pipeline de E-commerce

```python
# models/ingest_clientes.py
from dfg.sources import RestSource

DEPENDENCIES = []

source = RestSource(
    base_url="{{ env('API_BASE_URL') }}",
    auth={"type": "bearer", "token": "{{ env('API_TOKEN') }}"},
    pagination={"type": "offset", "page_size": 200},
)

def model(context):
    return source.get("/v1/clientes", extract_path="data")
```

```python
# models/ingest_pedidos.py
from dfg.sources import RestSource

DEPENDENCIES = []

source = RestSource(
    base_url="{{ env('API_BASE_URL') }}",
    auth={"type": "bearer", "token": "{{ env('API_TOKEN') }}"},
    pagination={"type": "cursor", "cursor_param": "after", "cursor_path": "meta.cursor"},
)

def model(context):
    since = context["state"] or "2020-01-01T00:00:00Z"
    records = source.get("/v1/pedidos", params={"since": since}, extract_path="data")
    if records:
        context["set_state"](max(r["atualizado_em"] for r in records))
    return records
```

```sql
-- models/stg_pedidos.sql
{{ config(materialized='table') }}

SELECT
    id                   AS pedido_id,
    cliente_id,
    CAST(total AS FLOAT) AS valor_total,
    LOWER(status)        AS status,
    criado_em::TIMESTAMP AS criado_em
FROM {{ ref('ingest_pedidos') }}
WHERE id IS NOT NULL
```

```sql
-- models/fct_kpis_diarios.sql
{{ config(materialized='table') }}

SELECT
    DATE(criado_em)            AS data,
    COUNT(*)                   AS total_pedidos,
    COUNT(DISTINCT cliente_id) AS clientes_unicos,
    SUM(valor_total)           AS receita_total,
    AVG(valor_total)           AS ticket_medio
FROM {{ ref('stg_pedidos') }}
WHERE status = 'completo'
GROUP BY DATE(criado_em)
ORDER BY data DESC
```

```bash
dfg seed && dfg run && dfg test && dfg docs --serve
```

---

### Ingestão de API com Estado

API paginada via GitHub Link Header com estado persistido:

```python
# models/ingest_github_repos.py
from dfg.sources import RestSource

DEPENDENCIES = []

source = RestSource(
    base_url="https://api.github.com",
    auth={"type": "bearer", "token": "{{ env('GITHUB_TOKEN') }}"},
    pagination={"type": "link_header"},
    headers={"Accept": "application/vnd.github.v3+json"},
    rate_limit_rps=10.0,
)

def model(context):
    return source.get("/orgs/minha-org/repos", params={"sort": "updated", "per_page": 100})
```

---

### Ingestão de Arquivos do S3

Ingestão em batch de exportações diárias de um bucket S3:

```python
# models/ingest_vendas_s3.py
from dfg.sources.cloud import S3Source

DEPENDENCIES = []

source = S3Source(
    uri="s3://analytics-bucket/",
    region="sa-east-1",
    access_key="{{ env('AWS_ACCESS_KEY_ID') }}",
    secret_key="{{ env('AWS_SECRET_ACCESS_KEY') }}",
)

def model(context):
    from datetime import datetime
    prefix = f"exports/vendas/{datetime.now().strftime('%Y/%m')}/"
    return source.fetch_many(prefix=prefix, pattern="*.parquet")
```

---

### Replicação entre Bancos com CDC Incremental

Copia dados de um banco legado para o data warehouse de forma incremental:

```python
# models/replica_clientes.py
from dfg.sources import DatabaseSource

DEPENDENCIES = []

source = DatabaseSource(
    connection={
        "type":     "mysql",
        "host":     "{{ env('LEGADO_DB_HOST') }}",
        "database": "erp_producao",
        "user":     "{{ env('LEGADO_DB_USER') }}",
        "password": "{{ env('LEGADO_DB_PASSWORD') }}",
    },
    query="""
        SELECT id, nome, email, atualizado_em
        FROM clientes
        WHERE atualizado_em > :since
        ORDER BY atualizado_em ASC
    """,
)

def model(context):
    since = context["state"] or "2020-01-01 00:00:00"
    records = source.fetch(since=since)
    if records:
        context["set_state"](str(max(r["atualizado_em"] for r in records)))
    return records
```

---

### Auditoria com Snapshot SCD2

```sql
{% snapshot hist_planos_clientes %}
{{ config(unique_key='cliente_id', updated_at='atualizado_em') }}
SELECT cliente_id, nome, plano_atual, valor_mensal, atualizado_em
FROM {{ ref('stg_clientes') }}
{% endsnapshot %}
```

```sql
-- Upgrades de plano em Março/2024
SELECT COUNT(DISTINCT cliente_id) AS upgrades
FROM hist_planos_clientes
WHERE dfg_valid_from BETWEEN '2024-03-01' AND '2024-03-31'
  AND plano_atual IN ('Premium', 'Enterprise');
```

---

## Boas Práticas

**Organização de modelos:** prefixe com `ingest_`, `stg_`, `int_`, `fct_`, `dim_`, `vw_`. Instancie sources fora da função `model()` para reutilização.

**Sources:** use `{{ env('VAR') }}` para todas as credenciais. Configure `rate_limit_rps` em APIs com rate limit. Use `fetch_many()` com prefixo específico, não o bucket inteiro.

**Performance:** `materialized='view'` para modelos raramente consultados. `materialized='incremental'` para tabelas grandes de eventos. Aumente `threads` para pipelines com muitos modelos independentes.

**Segurança:** com `{{ env() }}`, o `profiles.toml` pode ser versionado sem expor credenciais. Nunca use credenciais hardcoded nas sources.

**Reprodutibilidade:** versione `models/`, `seeds/`, `snapshots/`, `schema.yml`. Nunca versione `target/`, `.dfg_state.json`, `logs/` e bancos locais.

**.gitignore recomendado:**

```
.dfg_state.json
target/
logs/
*.db
*.duckdb
```

---

## Solução de Problemas

**`Fatal error in launcher: Unable to create process`**
O `dfg.exe` foi instalado fora do virtualenv: `pip uninstall dfg -y && pip install -e .`

**`'dfg_project.toml' não encontrado`**
Execute o comando na raiz do projeto: `cd /caminho/do/projeto`

**`Driver 'psycopg2' não encontrado`**
`pip install psycopg2-binary`

**`S3Source requer 'boto3'`**
`pip install boto3`

**`GCSSource requer 'google-cloud-storage'`**
`pip install google-cloud-storage`

**`AzureBlobSource requer 'azure-storage-blob'`**
`pip install azure-storage-blob`

**`FileSource (Parquet): 'pyarrow' não está instalado`**
`pip install pyarrow`

**`Variável de ambiente 'NOME_VAR' não está definida`**
Defina antes de executar: `export NOME_VAR=valor` (Linux/macOS) ou `$env:NOME_VAR = "valor"` (PowerShell).

**`Dependência circular detectada no DAG`**
`dfg compile` mostra o SQL de cada modelo. `dfg docs` visualiza o grafo.

**Snapshot não detecta mudanças**
Verifique se a coluna `updated_at` está sendo atualizada na fonte.

**Logs duplicados no terminal**
Mantenha uma única instância de `DFGEngine` por processo ao usar como biblioteca.

---