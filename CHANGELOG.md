# Changelog

Todas as mudanças notáveis deste projeto são documentadas aqui.
Formato baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.0.0/).
Versionamento segue [Semantic Versioning](https://semver.org/lang/pt-BR/).

---

## [0.1.0] — 2026-04-24

### Adicionado
- Motor de ELT completo com suporte a modelos SQL (Jinja2) e Python
- Materializações: `table`, `view`, `incremental`
- Sistema de dependências via `{{ ref() }}` com resolução de DAG topológico
- Execução paralela via `ThreadPoolExecutor` com número de threads configurável
- Modelos Python com ingestão de dados, schema evolution automática e estado incremental
- Testes de qualidade de dados: `not_null`, `unique`
- Contratos declarados em `schema.yml`
- Seeds: carga de CSVs com inferência de tipos e Drop & Replace
- Snapshots SCD Tipo 2 com colunas de controle `dfg_valid_from/to/is_active`
- Documentação HTML interativa com grafo de linhagem (Vis.js) via `dfg docs`
- Sistema de log persistente com sessões diárias identificadas por ID `DDMMAADFG`
- Busca e filtragem de logs via `dfg log`
- Diagnóstico do ambiente via `dfg debug`
- Compilação Jinja2 com dry-run via `dfg compile`
- Artefatos de observabilidade: `manifest.json` e `run_results.json`
- Suporte a DuckDB, PostgreSQL, MySQL e SQLite via DB-API 2.0
- Inicialização de projetos via `dfg init` com detecção automática de drivers

---

## [0.2.0] — 2026-04-28

### Adicionado

#### Módulo `dfg.sources` — Conectores Nativos de Ingestão

A maior adição desta versão é o módulo `dfg.sources`, que elimina a necessidade
de escrever código de ingestão do zero em cada modelo Python.

**`RestSource`** — Conector declarativo para APIs REST:
- Suporte completo a todos os métodos HTTP (GET, POST, PUT, PATCH)
- Rate limiting configurável via `rate_limit_rps`
- Header `User-Agent` padrão identificando o DataForge
- Resolução de variáveis de ambiente em qualquer parâmetro de configuração

**`FileSource`** — Ingestão de arquivos locais e remotos:
- Formatos suportados: CSV, JSON, JSONL (JSON Lines / NDJSON), Parquet
- Detecção automática do formato pela extensão do arquivo
- Suporte a BOM (arquivos exportados pelo Excel) via encoding `utf-8-sig`
- Inferência automática de tipos em CSV: string → int → float → None
- Parquet via `pyarrow` com importação lazy (não é dependência obrigatória)
- Download remoto via HTTP/HTTPS com headers customizáveis

**`DatabaseSource`** — Extração de dados entre bancos relacionais:
- Reutiliza a `AdapterFactory` existente (suporta DuckDB, PostgreSQL, MySQL, SQLite)
- Substituição de parâmetros nomeados na query via `:param_name`
- Compatível com o mecanismo de estado incremental do DataForge
- Obtém nomes de colunas via `cursor.description` (padrão DB-API 2.0)

**`S3Source`** — AWS S3 e storages S3-compatíveis:
- Suporte a MinIO, Cloudflare R2, DigitalOcean Spaces, Backblaze B2 e Oracle OCI via `endpoint_url`
- Autenticação por credenciais explícitas, variáveis de ambiente AWS padrão, AWS profiles ou IAM Role
- Paginação automática via `list_objects_v2` para buckets com mais de 1.000 objetos
- Método `fetch_many(prefix, pattern)` para ingestão em batch com filtro glob

**`GCSSource`** — Google Cloud Storage:
- Autenticação via service account JSON, variável `GOOGLE_APPLICATION_CREDENTIALS` ou ADC
- Método `fetch_many(prefix, pattern)` para ingestão em batch
- Listagem de objetos via `list_blobs()` com paginação automática pelo SDK

**`AzureBlobSource`** — Azure Blob Storage:
- Autenticação via connection string, account key, SAS token ou `DefaultAzureCredential`
- Suporte a Managed Identity, Azure CLI e Workload Identity via `DefaultAzureCredential`
- Método `fetch_many(prefix, pattern)` para ingestão em batch

**Estratégias de Autenticação HTTP** (`dfg.sources.auth`):
- `BearerAuth` — token Bearer / JWT via header `Authorization`
- `ApiKeyAuth` — chave de API via header HTTP ou query parameter
- `BasicAuth` — HTTP Basic Authentication com codificação Base64 (RFC 7617)
- `OAuth2Auth` — OAuth2 Client Credentials Flow (RFC 6749 §4.4) com cache de token e renovação automática por expiração

**Estratégias de Paginação HTTP** (`dfg.sources.pagination`):
- `OffsetPagination` — `?offset=0&limit=100`, com suporte a campo `total` na resposta
- `PageNumberPagination` — `?page=1&per_page=100`, com suporte a `total_pages`
- `CursorPagination` — `?cursor=<token>`, com extração do cursor via notação de ponto
- `LinkHeaderPagination` — header `Link: <url>; rel="next"` (RFC 5988), usado por GitHub e GitLab
- `NextUrlPagination` — próxima URL no corpo da resposta JSON, extraída via notação de ponto

**Infraestrutura interna das sources:**
- `_env.py` — resolver de `{{ env('VAR') }}` com suporte recursivo a strings, dicionários e listas
- `_retry.py` — mixin de retry com backoff exponencial e jitter aleatório (anti thundering-herd)
- `BaseSource` — interface abstrata com extração de dados aninhados via notação de ponto
- `BaseCloudSource` — base para conectores cloud com `fetch_many()` e `_InMemoryFileParser`
- `fetch_many()` disponível em todos os conectores cloud: lista objetos, filtra por glob e mescla os registros

#### Variáveis de Ambiente no `profiles.toml`

O `profiles.toml` agora suporta a sintaxe `{{ env('NOME_DA_VARIAVEL') }}` em
qualquer valor de credencial, mantendo senhas fora do código-fonte e do controle
de versão:

```toml
[meu_projeto.outputs.prod]
type     = "postgres"
host     = "{{ env('DB_HOST') }}"
password = "{{ env('DB_PASSWORD') }}"
```

A resolução é aplicada automaticamente pelo `DFGEngine` ao carregar as
configurações, antes de passar as credenciais para o adaptador de banco.

---