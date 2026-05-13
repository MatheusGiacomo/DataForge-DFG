# Changelog

Todas as mudanças notáveis deste projeto são documentadas aqui.
Formato baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.0.0/).
Versionamento segue [Semantic Versioning](https://semver.org/lang/pt-BR/).

---

## [0.3.0] — 2026-05-13

### Adicionado

#### Macros Reutilizáveis (`macros/`)

Blocos Jinja2 reutilizáveis definidos em arquivos `.sql` dentro da pasta
`macros/` são automaticamente carregados e disponibilizados em todos os
modelos, análises e snapshots — sem necessidade de `{% import %}` explícito.

```sql
-- macros/utils.sql
{% macro cents_to_dollars(col) %}
    ROUND({{ col }} / 100.0, 2)
{% endmacro %}

{% macro upper_status(col) %} UPPER({{ col }}) {% endmacro %}
```

```sql
-- models/fct_orders.sql — uso direto, sem import
SELECT {{ cents_to_dollars('amount_cents') }} AS amount_usd
FROM {{ ref('stg_orders') }}
```

Implementação: `MacroLoader` usa `jinja2.Template.make_module()` para extrair
os callables de macro e injetá-los como globals do ambiente Jinja2, tornando-os
visíveis globalmente em todo o projeto. Duplicatas entre arquivos geram aviso
de log. Arquivos com erro de sintaxe são reportados individualmente sem
interromper o carregamento dos demais.

#### Análises Ad-hoc (`analysis/`)

Arquivos `.sql` em `analysis/` são compilados via Jinja2 (`dfg compile`) mas
não são materializados no banco. Têm acesso a `ref()`, `var()` e macros.
Úteis para queries exploratórias, relatórios ad-hoc e investigações de dados.

```sql
-- analysis/revenue_by_channel.sql
SELECT
    {{ upper_status('channel') }}              AS channel,
    {{ cents_to_dollars('SUM(amount_cents)') }} AS revenue
FROM {{ ref('fct_orders') }}
WHERE date >= '{{ var("start_date", "2024-01-01") }}'
GROUP BY channel
```

O SQL compilado é salvo em `target/compiled/analysis/` para revisão,
execução manual ou versionamento.

#### Hooks: `pre_hook` e `post_hook`

Cada modelo SQL pode declarar hooks que são executados antes e após a
materialização, dentro da mesma transação da thread. Declarados via
`{{ config(...) }}`, suportam string única ou lista de strings. Cada
hook é compilado pelo Jinja2 antes da execução (suporta `var()` e macros).

```sql
{{
    config(
        materialized='table',
        pre_hook="DELETE FROM audit_log WHERE model = 'fct_orders'",
        post_hook=[
            "INSERT INTO audit_log VALUES ('fct_orders', NOW())",
            "UPDATE run_meta SET last_run = NOW() WHERE model = 'fct_orders'"
        ]
    )
}}
SELECT ...
```

#### `dfg lineage` — Linhagem no Terminal

Exibe o grafo de dependências diretamente no terminal, sem necessidade de
abrir o browser. Dois modos de exibição:

```bash
# Grafo completo do projeto em camadas topológicas
dfg lineage

# Upstream + downstream de um modelo específico
dfg lineage --model fct_revenue
```

O renderizador usa BFS nos grafos direto e reverso de dependências para
calcular ancestrais e descendentes transitivos. Saída colorida com símbolos
`●` (Python/ingestão) e `○` (SQL/transformação), indicadores de materialização
e marcador `◄ você está aqui` no modelo focal.

#### `dfg diff` — Comparação de Execuções

Compara a última execução com a anterior e exibe uma tabela de diferenças
no terminal, destacando regressões e melhorias de performance.

```bash
dfg run   # primeira execução
dfg run   # segunda execução
dfg diff  # compara as duas
```

Classificações por modelo:

| Classificação | Condição |
|---|---|
| `REGREDIU ⚠`    | estava passando, agora falha |
| `RECUPEROU ✓`   | estava falhando, agora passa |
| `NOVO`          | não existia na execução anterior |
| `REMOVIDO`      | existia antes, não existe mais |
| `MAIS RÁPIDO ↑` | passou nas duas, delta ≤ −15% |
| `MAIS LENTO ↓`  | passou nas duas, delta ≥ +15% |

O `engine.py` preserva automaticamente o resultado anterior em
`target/run_results.prev.json` antes de cada nova execução (rotação de arquivo).

#### `dfg run --dry-run` — Simulação de Execução

Executa o DAG completo sem realizar nenhuma operação no banco de dados.
Para cada modelo, exibe o tipo, materialização, prévia do SQL compilado
(primeiras 3 linhas) e informações sobre hooks configurados.

```bash
dfg run --dry-run
dfg run --select +fct_revenue --dry-run
dfg transform --dry-run
dfg ingest --dry-run
```

Modo `dry_run` também disponível em `ingest` e `transform`.

### Modificado

- **`compiler.py`**: aceita `macro_globals` no construtor (injetados via
  `env.globals.update()`); novo método público `compile_analysis()` para
  análises ad-hoc com acesso completo a macros e `var()`.

- **`engine.py`**: `MacroLoader` é inicializado antes do `SQLCompiler` para
  garantir que macros estejam disponíveis desde a primeira compilação; novo
  método `_run_hooks()` que compila e executa hooks Jinja2; novo método
  `_dry_run_node()` para simulação; método `_rotate_run_results()` preserva
  histórico para `dfg diff`; pasta `analysis/` descoberta e compilada em
  `compile()`; flag `dry_run` propagado por `run()`, `ingest()` e
  `transform()`; pasta `macros_dir` e `analysis_dir` adicionados ao `__init__`.

- **`cli.py`**: comandos `dfg lineage` e `dfg diff` adicionados; flag
  `--dry-run` adicionado aos comandos `run`, `ingest` e `transform`;
  helper `_add_execution_args()` refatorado com parâmetro `include_dry_run`.

---

## [0.3.0] — 2026-04-30

### Adicionado

#### Testes de Qualidade de Dados Avançados (`dfg.testing`)

Motor de testes completamente reescrito usando o padrão Strategy. Cada tipo
de teste é uma classe independente com método `build_sql()`, permitindo
extensão sem modificar o código existente.

**Novos testes por coluna** (declarados em `schema.yml` → `columns`):

| Teste | Verificação |
|---|---|
| `accepted_values` | Todos os valores pertencem à lista permitida |
| `relationships` | Integridade referencial com outra tabela |
| `not_negative` | Valores numéricos >= 0 |
| `between` | Valores dentro do intervalo `[min_value, max_value]` |
| `custom_sql` | SQL arbitrário fornecido pelo usuário (zero = passou) |

**Novos testes no nível do modelo** (declarados em `schema.yml` → `tests`):

| Teste | Verificação |
|---|---|
| `row_count_between` | Total de linhas dentro de um intervalo `[min, max]` |
| `freshness` | Dado mais recente não é mais antigo do que `max_age_hours` horas |
| `no_duplicate_rows` | Sem linhas completamente duplicadas nas colunas especificadas |

Exemplo de configuração completa:

```yaml
models:
  - name: fct_pedidos
    tests:
      - row_count_between:
          min: 1000
          max: 5000000
      - freshness:
          field: criado_em
          max_age_hours: 25
      - no_duplicate_rows:
          columns: [pedido_id, criado_em]
    columns:
      - name: status
        tests:
          - accepted_values:
              values: [pending, complete, cancelled]
      - name: valor_total
        tests:
          - not_negative
          - between:
              min_value: 0.01
              max_value: 999999.99
      - name: cliente_id
        tests:
          - relationships:
              to: stg_clientes
              field: id
      - name: descricao
        tests:
          - custom_sql:
              sql: >
                SELECT COUNT(*) FROM {model}
                WHERE {column} IS NOT NULL AND LENGTH({column}) < 3
```

O `TestRunner` retorna um `TestReport` estruturado com listas de resultados
por status, contagens e flag `has_failures` para integração com CI/CD.

#### Seleção de Modelos no DAG (`dfg.selector`)

Execução seletiva do pipeline com sintaxe inspirada no dbt. O módulo
`selector.py` separa completamente o parsing (SelectorParser) da resolução
(SelectorResolver), usando BFS nos grafos direto e reverso de dependências.

```bash
dfg run --select stg_pedidos          # modelo exato
dfg run --select +fct_revenue         # modelo + todos os ancestrais
dfg run --select stg_pedidos+         # modelo + todos os descendentes
dfg run --select +fct_revenue+        # modelo + ancestrais + descendentes
dfg run --select tag:staging          # todos os modelos com a tag
dfg run --select +fct_kpis tag:seed   # combinação (união)
```

Disponível em `run`, `ingest`, `transform` e `test`.

#### `--target` — Troca de Ambiente via CLI

Sobrescreve o ambiente ativo definido em `dfg_project.toml` sem editar
nenhum arquivo. Útil para executar pipelines em produção de forma pontual.

```bash
dfg run --target prod
dfg test --target staging
dfg transform --target prod --select +fct_revenue
```

#### `--var` — Parâmetros Dinâmicos para Templates SQL

Injeta variáveis de runtime em templates Jinja2 via a macro `{{ var() }}`.

```bash
dfg run --var data_inicio=2024-01-01 --var status_filter=complete
```

```sql
-- models/fct_pedidos_periodo.sql
SELECT * FROM {{ ref('stg_pedidos') }}
WHERE criado_em >= '{{ var("data_inicio", "2020-01-01") }}'
  AND status    = '{{ var("status_filter", "complete") }}'
```

`var()` também está disponível no contexto Python dos modelos via
`context["var"]("chave", default)`.

### Modificado

- **`compiler.py`**: `ModelContext` recebe `runtime_vars` e expõe `var()`
  como macro Jinja2; `SQLCompiler` aceita `runtime_vars` no construtor;
  método interno `_render()` centraliza a renderização de templates.

- **`engine.py`**: `DFGEngine.__init__()` aceita `override_target` e
  `runtime_vars`; `_load_config()` usa `override_target` na resolução do
  target; `_execute_dag()` aceita `selected_nodes` e propaga para
  `_execute_node()`; `_execute_python_model()` expõe `var()` no contexto
  do modelo; `test()` delega para o novo `TestRunner`; `_enrich_with_yaml()`
  extrai `model_tests` via `extract_model_tests_from_yaml()`.

- **`cli.py`**: flags `--select`, `--target` e `--var` adicionados aos
  comandos `run`, `ingest`, `transform`, `test` e `compile` via helper
  `_add_execution_args()`; funções `_get_engine()`, `_parse_vars()` e
  `_get_select()` extraídas para reutilização.

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

#### Variáveis de Ambiente no `profiles.toml`

O `profiles.toml` agora suporta a sintaxe `{{ env('NOME_DA_VARIAVEL') }}` em
qualquer valor de credencial, mantendo senhas fora do código-fonte:

```toml
[meu_projeto.outputs.prod]
type     = "postgres"
host     = "{{ env('DB_HOST') }}"
password = "{{ env('DB_PASSWORD') }}"
```

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