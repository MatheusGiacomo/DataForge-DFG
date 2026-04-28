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
APIs REST       ──►     Modelos Python   ──►        DuckDB
Arquivos CSV    ──►     (ingestão)       ──►        PostgreSQL
Bancos legados  ──►                                 MySQL
                        Modelos SQL      ──►        SQLite
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
- Driver do banco escolhido (`duckdb`, `psycopg2`, etc.)

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

# 3. Instale o DataForge em modo de desenvolvimento
pip install -e .

# 4. Instale o driver do banco de dados desejado
pip install duckdb          # DuckDB (recomendado para começar)
pip install psycopg2-binary # PostgreSQL
pip install mysql-connector-python # MySQL
# SQLite já vem embutido no Python

# 5. Verifique a instalação
dfg --help
```

---

## Início Rápido

```bash
# Cria um novo projeto chamado 'meu_pipeline'
mkdir meu_pipeline && cd meu_pipeline
dfg init
```

O `dfg init` detecta automaticamente os drivers instalados e guia você pelo setup:

```
                       ████████████████████████████████
   ███████████████████▓▓████████████████████████████▒
    ...

Bem-vindo ao Data Forge!
--------------------------------------------------
Nome do projeto (ex: meu_projeto): meu_pipeline

Bancos de dados detectados no seu ambiente:
  [1] DuckDB
  [2] PostgreSQL (psycopg2)

Escolha o número do banco preferido: 1

ok  Configurando projeto 'meu_pipeline' com: DuckDB
--------------------------------------------------
ok  Projeto 'meu_pipeline' forjado com sucesso! 🔥

Próximos passos:
  1. Revise o profiles.toml com suas credenciais de banco
  2. Crie seus modelos na pasta models/
  3. Execute: dfg run
```

Após o `init`, execute o pipeline de exemplo:

```bash
dfg run
```

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
    ├── manifest.json        # Topologia do DAG
    ├── run_results.json     # Resultados da última execução
    ├── index.html           # Documentação visual do DAG
    └── compiled/            # SQL puro (após compilação Jinja)
        ├── stg_users.sql
        └── fct_vendas.sql
```

---

## Configuração

### dfg_project.toml

O arquivo principal de configuração do projeto.

```toml
[project]
name    = "meu_pipeline"   # Nome do projeto
profile = "meu_pipeline"   # Nome do bloco no profiles.toml
target  = "dev"            # Ambiente ativo (dev | prod)
threads = 4                # Número de threads paralelas no DAG
```

Para alternar entre ambientes sem editar o arquivo, use a convenção de nomear os targets como `dev` e `prod` no `profiles.toml`.

---

### profiles.toml

Define as credenciais de conexão para cada ambiente. Nunca versione este arquivo com senhas reais — adicione-o ao `.gitignore`.

```toml
[meu_pipeline]
target = "dev"

# Ambiente de desenvolvimento
[meu_pipeline.outputs.dev]
type     = "duckdb"
database = "meu_pipeline_dev.db"

# Ambiente de produção
[meu_pipeline.outputs.prod]
type     = "duckdb"
database = "meu_pipeline_prod.db"
```

---

### Bancos Suportados

O DataForge suporta qualquer driver que siga o padrão **PEP 249 (DB-API 2.0)**:

| Banco | Tipo no profiles.toml | Driver Python |
|---|---|---|
| DuckDB | `duckdb` | `pip install duckdb` |
| PostgreSQL | `postgres` ou `postgresql` | `pip install psycopg2-binary` |
| MySQL | `mysql` | `pip install mysql-connector-python` |
| SQLite | `sqlite` | Nativo (sem instalação) |

**Exemplo — PostgreSQL:**

```toml
[meu_pipeline.outputs.prod]
type     = "postgres"
host     = "db.meuservidor.com"
port     = 5432
user     = "admin"
password = "senha_secreta"
database = "analytics"
schema   = "public"
```

**Exemplo — SQLite:**

```toml
[meu_pipeline.outputs.dev]
type     = "sqlite"
database = "local_dev.db"
```

---

## Modelos SQL

Modelos SQL são arquivos `.sql` dentro de `models/`. Eles usam **Jinja2** como linguagem de template, permitindo configurações, referências e lógica dinâmica.

### Materializações

O DataForge suporta três formas de materializar um modelo no banco:

| Tipo | Comportamento | Quando usar |
|---|---|---|
| `table` | DROP + CREATE TABLE AS SELECT | Quando a query é pesada e o resultado precisa ser persistido |
| `view` | DROP + CREATE VIEW AS SELECT | Quando os dados são acessados raramente ou são simples |
| `incremental` | Insere apenas linhas novas (com chave) | Tabelas grandes onde só novos registros chegam |

```sql
-- models/stg_pedidos.sql
-- Materialização como tabela física
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

```sql
-- models/vw_resumo_diario.sql
-- Materialização como view (calculada na hora da consulta)
{{ config(materialized='view') }}

SELECT
    DATE(criado_em) AS data,
    COUNT(*)        AS total_pedidos,
    SUM(valor_total) AS receita
FROM {{ ref('stg_pedidos') }}
GROUP BY DATE(criado_em)
```

---

### Dependências com ref()

A função `{{ ref('nome_do_modelo') }}` é a forma de declarar dependências entre modelos. Ela faz duas coisas:

1. Substitui pelo nome da tabela no SQL compilado
2. Registra a dependência no DAG para que a execução ocorra na ordem correta

```sql
-- models/fct_clientes_ativos.sql
{{ config(materialized='table') }}

SELECT
    c.id,
    c.nome,
    c.email,
    COUNT(p.pedido_id) AS total_pedidos,
    SUM(p.valor_total) AS lifetime_value
FROM {{ ref('stg_clientes') }} c   -- depende de stg_clientes
JOIN {{ ref('stg_pedidos') }} p    -- depende de stg_pedidos
  ON c.id = p.cliente_id
GROUP BY c.id, c.nome, c.email
```

O DataForge garante que `stg_clientes` e `stg_pedidos` sejam executados **antes** de `fct_clientes_ativos`.

---

### Configuração com config()

A macro `{{ config(...) }}` define o comportamento do modelo. Ela é removida do SQL compilado e não aparece na query final.

```sql
{{ config(
    materialized='table',
    description='Fatos de vendas agregados por dia e produto'
) }}

SELECT ...
```

---

### Modelo Incremental

O modelo incremental é ideal para tabelas grandes onde apenas novos registros chegam periodicamente (logs, eventos, transações).

```sql
-- models/eventos_app.sql
{{
    config(
        materialized='incremental',
        unique_key='evento_id'
    )
}}

SELECT
    evento_id,
    usuario_id,
    tipo_evento,
    payload,
    ocorreu_em
FROM raw_eventos_app
```

**Como funciona internamente:**

1. Na primeira execução: cria a tabela `eventos_app` com todos os registros
2. Nas execuções seguintes:
   - Cria uma tabela temporária `eventos_app__dfg_tmp` com o resultado da query
   - Remove da tabela principal os registros cujo `unique_key` já existe na temp
   - Insere os registros da tabela temporária na principal
   - Remove a tabela temporária

Isso garante **idempotência**: rodar o modelo duas vezes não duplica os dados.

---

## Modelos Python

Modelos Python são arquivos `.py` dentro de `models/`. Eles são usados para a fase de **ingestão (Extract & Load)**: conectar em APIs, ler arquivos remotos, consumir bancos legados e retornar os dados para o DataForge carregar no banco de destino.

### Estrutura de um Modelo Python

Todo modelo Python deve exportar uma função chamada `model(context)` que retorna uma **lista de dicionários**.

```python
# models/ingest_produtos.py

# Dependências opcionais de outros modelos (SQL ou Python)
DEPENDENCIES = []

def model(context):
    """
    Ingere produtos de uma API REST e os retorna para o DataForge.
    O DataForge carrega automaticamente no banco com schema evolution.
    """
    import urllib.request
    import json

    url = "https://api.exemplo.com/v1/produtos"
    with urllib.request.urlopen(url) as resp:
        dados = json.loads(resp.read())

    # Retorne uma lista de dicionários.
    # O DataForge cria a tabela automaticamente se não existir,
    # e adiciona colunas novas se o schema evoluir.
    return dados["produtos"]
```

**O dicionário `context` disponível na função:**

| Chave | Tipo | Descrição |
|---|---|---|
| `context["config"]` | `dict` | Configurações completas do projeto (dfg_project.toml + profiles.toml) |
| `context["ref"]` | `callable` | Acessa dados de outro modelo já executado |
| `context["state"]` | `any` | Estado persistido da última execução (para ingestão incremental) |
| `context["set_state"]` | `callable` | Persiste um novo estado para a próxima execução |

---

### Estado Incremental com State

O mecanismo de **State** permite que um modelo Python lembre-se de onde parou na última execução — por exemplo, o último timestamp ingerido, a última página de uma API ou o último ID processado.

```python
# models/ingest_pedidos_incremental.py
from datetime import datetime, timezone

DEPENDENCIES = []

def model(context):
    import urllib.request
    import json

    # Recupera o estado da última execução.
    # Na primeira vez, retorna None (ou o valor padrão fornecido).
    ultimo_timestamp = context["state"] or "2020-01-01T00:00:00Z"

    url = f"https://api.loja.com/pedidos?since={ultimo_timestamp}"
    with urllib.request.urlopen(url) as resp:
        dados = json.loads(resp.read())

    pedidos = dados["pedidos"]

    if pedidos:
        # Persiste o timestamp do registro mais recente para a próxima execução
        novo_estado = max(p["atualizado_em"] for p in pedidos)
        context["set_state"](novo_estado)

    return pedidos
```

O estado é salvo em `.dfg_state.json` na raiz do projeto:

```json
{
    "ingest_pedidos_incremental": "2024-11-15T23:59:58Z"
}
```

---

### Dependências entre Modelos Python e SQL

Modelos Python podem depender de outros modelos (Python ou SQL) usando a variável `DEPENDENCIES`:

```python
# models/enriquecer_clientes.py

# Este modelo só roda DEPOIS de stg_clientes ter sido materializado
DEPENDENCIES = ["stg_clientes"]

def model(context):
    # Acessa os dados do modelo dependente em memória
    clientes_base = context["ref"]("stg_clientes")

    resultado = []
    for cliente in (clientes_base or []):
        # Enriquece cada cliente com dados externos
        resultado.append({
            **cliente,
            "score_credito": calcular_score(cliente["cpf"]),
            "enriquecido_em": datetime.now().isoformat(),
        })

    return resultado

def calcular_score(cpf):
    # Lógica de enriquecimento...
    return 850
```

---

## Contratos de Dados (schema.yml)

O arquivo `models/schema.yml` define os **contratos de dados** de cada modelo: descrições e testes que garantem a qualidade dos dados após a materialização.

```yaml
version: 1

models:
  - name: stg_pedidos
    description: "Pedidos limpos oriundos do sistema transacional."
    columns:
      - name: pedido_id
        tests:
          - not_null    # Nenhum valor NULL é permitido
          - unique      # Nenhum ID duplicado é permitido

      - name: valor_total
        tests:
          - not_null

      - name: status
        tests:
          - not_null

  - name: fct_clientes_ativos
    description: "Clientes com pelo menos um pedido nos últimos 90 dias."
    columns:
      - name: id
        tests:
          - not_null
          - unique
```

Execute os testes com:

```bash
dfg test
```

Saída de uma execução bem-sucedida:

```
info  --- Iniciando Validação de Contratos ---
invocation  Testando 'stg_pedidos'...
ok  Todos os contratos validados com sucesso!
```

Saída quando um teste falha:

```
invocation  Testando 'stg_pedidos'...
error   [FALHA] not_null: 'stg_pedidos.valor_total' tem 3 nulos.
error   [FALHA] unique: 'stg_pedidos.pedido_id' tem 2 chave(s) duplicada(s).
error  Validação concluída com 2 falha(s).
```

> O comando `dfg test` encerra com código de saída `1` em caso de falha, integrando naturalmente com pipelines de CI/CD (GitHub Actions, GitLab CI, etc.).

---

## Seeds — Dados Estáticos

Seeds são arquivos `.csv` em `seeds/` que representam dados de referência estáticos: países, categorias, parâmetros de configuração, etc. Cada CSV se torna uma tabela no banco com o mesmo nome do arquivo.

```bash
seeds/
├── paises.csv
├── categorias_produto.csv
└── taxas_impostos.csv
```

```csv
# seeds/paises.csv
codigo,nome,continente,idioma_oficial
BR,Brasil,América do Sul,Português
US,Estados Unidos,América do Norte,Inglês
DE,Alemanha,Europa,Alemão
JP,Japão,Ásia,Japonês
```

```bash
dfg seed
```

```
info  Iniciando plantio de 3 seed(s)...
invocation  Semeando 'paises'...
ok  ✓ Seed 'paises' plantada com sucesso (4 linha(s)).
invocation  Semeando 'categorias_produto'...
ok  ✓ Seed 'categorias_produto' plantada com sucesso (12 linha(s)).
info  Seeds: 3/3 carregada(s) com sucesso em 0.842s.
```

**Comportamento:**
- Detecta e converte tipos automaticamente: `"42"` → `42` (int), `"3.14"` → `3.14` (float)
- Remove o BOM de arquivos exportados pelo Excel
- Executa **Drop & Replace** (idempotente): cada `dfg seed` recria a tabela do zero
- Compatível com arquivos UTF-8 e UTF-8-BOM

---

## Snapshots — SCD Tipo 2

Snapshots implementam o padrão **Slowly Changing Dimensions Tipo 2 (SCD2)**: mantêm um histórico completo de todas as versões de um registro ao longo do tempo, sem sobrescrever os dados anteriores.

Isso é útil para auditar mudanças: "qual era o status do cliente João em 15/03/2024?"

### Criando um Snapshot

```sql
-- snapshots/hist_clientes.sql
{% snapshot hist_clientes %}

{{
    config(
        unique_key='cliente_id',
        updated_at='atualizado_em'
    )
}}

SELECT
    cliente_id,
    nome,
    email,
    plano,
    status,
    atualizado_em
FROM {{ ref('stg_clientes') }}

{% endsnapshot %}
```

**Parâmetros de configuração:**

| Parâmetro | Obrigatório | Descrição |
|---|---|---|
| `unique_key` | ✅ | Coluna que identifica unicamente cada entidade |
| `updated_at` | ✅ | Coluna de timestamp usada para detectar mudanças |

```bash
dfg snapshot
```

### Colunas de Controle Adicionadas

O DataForge adiciona automaticamente 4 colunas de controle à tabela de histórico:

| Coluna | Tipo | Descrição |
|---|---|---|
| `dfg_valid_from` | TIMESTAMP | Quando esta versão do registro entrou em vigor |
| `dfg_valid_to` | TIMESTAMP | Quando esta versão foi substituída (NULL = atual) |
| `dfg_is_active` | BOOLEAN | TRUE para a versão mais recente de cada entidade |
| `dfg_updated_at` | TIMESTAMP | Timestamp da última operação do DataForge |

### Exemplo de Resultado

Suponha que o cliente João mudou de plano Basic para Premium em 10/03/2024:

| cliente_id | nome | plano | dfg_valid_from | dfg_valid_to | dfg_is_active |
|---|---|---|---|---|---|
| 1 | João | Basic | 2024-01-01 | 2024-03-10 | FALSE |
| 1 | João | Premium | 2024-03-10 | NULL | TRUE |

Consultar o estado histórico:

```sql
-- Estado atual de todos os clientes
SELECT * FROM hist_clientes WHERE dfg_is_active = TRUE;

-- Estado de João em 01/02/2024
SELECT * FROM hist_clientes
WHERE cliente_id = 1
  AND dfg_valid_from <= '2024-02-01'
  AND (dfg_valid_to > '2024-02-01' OR dfg_valid_to IS NULL);
```

---

## Referência de Comandos

### dfg init

Inicializa um novo projeto DataForge no diretório atual.

```bash
dfg init
```

O comando:
- Solicita o nome do projeto interativamente
- Detecta automaticamente os drivers de banco instalados no ambiente
- Gera `dfg_project.toml` e `profiles.toml` com configurações pré-preenchidas
- Cria toda a estrutura de pastas (`models/`, `seeds/`, `snapshots/`, etc.)
- Gera arquivos de exemplo para começar imediatamente

---

### dfg run

Executa o pipeline completo: primeiro a ingestão (modelos Python), depois as transformações (modelos SQL), respeitando a ordem do DAG.

```bash
dfg run
```

```
info  Iniciando pipeline no ambiente: DEV
info  Iniciando pool de execução (4 thread(s) alocada(s)).
info  Conexão estabelecida via 'duckdb'.
invocation  Extraindo/Ingerindo [PYTHON] 'ingest_produtos'...
ok  ✓ 'ingest_produtos' concluído em 1.234s.
invocation  Materializando [TABLE] 'stg_produtos'...
ok  ✓ 'stg_produtos' concluído em 0.045s.
invocation  Materializando [VIEW] 'vw_catalogo'...
ok  ✓ 'vw_catalogo' concluído em 0.012s.
ok  --- Pipeline finalizado com sucesso em 2.891s! ---
```

Encerra com **código 1** se qualquer modelo falhar.

---

### dfg ingest

Executa apenas os modelos Python (fase de Extract & Load). Ignora todos os modelos SQL.

```bash
dfg ingest
```

Útil quando você quer apenas atualizar os dados brutos sem rodar as transformações.

---

### dfg transform

Executa apenas os modelos SQL. Ignora todos os modelos Python.

```bash
dfg transform
```

Útil para reprocessar transformações após corrigir um modelo SQL sem precisar reingerir todos os dados.

---

### dfg test

Valida os contratos de dados definidos no `schema.yml` contra os dados que estão no banco.

```bash
dfg test
```

Testes disponíveis:

| Teste | Verificação |
|---|---|
| `not_null` | Nenhum valor `NULL` na coluna |
| `unique` | Nenhum valor duplicado na coluna |

Encerra com **código 1** se algum teste falhar, permitindo uso em CI/CD.

---

### dfg compile

Renderiza todos os templates Jinja2 dos modelos SQL em SQL puro e os salva em `target/compiled/`. Não executa nada no banco (Dry Run).

```bash
dfg compile
```

```
info  Compilando modelos e gerando manifest.json...
ok  Compilado: fct_clientes_ativos.sql
ok  Compilado: stg_pedidos.sql
ok  Compilação concluída.
```

Útil para:
- Revisar o SQL gerado antes de executar
- Depurar templates Jinja2 complexos
- Auditar o que será executado em produção

---

### dfg seed

Carrega todos os arquivos `.csv` da pasta `seeds/` no banco de dados.

```bash
dfg seed
```

Cada CSV é carregado com **Drop & Replace**: a tabela existente é removida e recriada com os dados atuais do arquivo.

---

### dfg snapshot

Processa todos os arquivos `.sql` da pasta `snapshots/` executando a lógica de SCD Tipo 2.

```bash
dfg snapshot
```

Na primeira execução: cria a tabela de histórico. Nas execuções seguintes: invalida registros alterados e insere as novas versões.

---

### dfg docs

Gera a documentação técnica do projeto em HTML com um grafo de linhagem interativo (DAG).

```bash
# Apenas gera o arquivo target/index.html
dfg docs

# Gera e abre no navegador com um servidor local
dfg docs --serve
```

O grafo usa **Vis.js** para renderização interativa:
- Nós azuis (hexágono) = modelos Python (ingestão)
- Nós verdes (cilindro) = modelos SQL (transformação)
- Arestas com seta = direção da dependência
- Hover nos nós = tooltip com tipo, materialização e descrição

> Requer que `dfg compile` ou `dfg run` tenha sido executado antes (gera o `manifest.json`).

---

### dfg debug

Executa um diagnóstico completo do ambiente: verifica arquivos de configuração, valida o parsing das credenciais e testa a conexão com o banco.

```bash
dfg debug
```

```
info  Iniciando diagnóstico do DataForge...
--------------------------------------------------
info  Diretório atual: /home/usuario/meu_pipeline
info  Versão do Python: 3.12.0
ok  Arquivos de configuração encontrados.
ok  Configuração carregada. Profile ativo: 'dev'.
info  Tentando conectar ao banco do tipo 'duckdb'...
ok  Conexão com o banco de dados: OK (SELECT 1 retornou 1).
--------------------------------------------------
ok  Tudo certo! O DataForge está pronto para a forja. 🔥
```

Encerra com **código 1** em qualquer falha, útil para scripts de verificação.

---

### dfg log

Busca e filtra o arquivo de log diário por ID de sessão e opcionalmente por comando.

```bash
# Mostra todos os logs do dia 15/04/2026
dfg log 150426DFG

# Filtra apenas os logs do comando 'run' desse dia
dfg log 150426DFG --run

# Filtra e exporta para um arquivo .txt
dfg log 150426DFG --run -d
```

**Flags de filtro disponíveis:**

```
--run        Filtra registros do comando dfg run
--ingest     Filtra registros do comando dfg ingest
--transform  Filtra registros do comando dfg transform
--test       Filtra registros do comando dfg test
--compile    Filtra registros do comando dfg compile
--docs       Filtra registros do comando dfg docs
--snapshot   Filtra registros do comando dfg snapshot
--seed       Filtra registros do comando dfg seed
-d, --dump   Exporta o resultado para um arquivo .txt
```

**Formato do ID de sessão:** `DDMMAADFG`

Exemplos: `150426DFG`, `010124DFG`, `311223DFG`

---

## DAG — Grafo de Dependências

O DataForge constrói um **Grafo Acíclico Dirigido (DAG)** a partir das declarações `{{ ref() }}` nos modelos SQL e da variável `DEPENDENCIES` nos modelos Python. A ordem de execução é resolvida automaticamente via ordenação topológica (`graphlib.TopologicalSorter` da stdlib).

**Exemplo de DAG:**

```
ingest_clientes (Python)  ──►  stg_clientes (SQL)  ──►  fct_resumo (SQL)
ingest_pedidos (Python)   ──►  stg_pedidos  (SQL)  ──┘
```

O DataForge detecta e reporta dependências circulares antes de executar:

```
error  Dependência circular detectada no DAG: {('modelo_a', 'modelo_b', 'modelo_a')}.
       Verifique as chamadas ref() nos seus modelos.
```

---

## Paralelismo

O DataForge usa `ThreadPoolExecutor` para executar modelos **em paralelo** sempre que não houver dependência entre eles. O número de threads é configurável:

```toml
# dfg_project.toml
[project]
threads = 4   # Padrão: 4
```

Modelos independentes no mesmo "nível" do DAG são despachados simultaneamente. Modelos dependentes aguardam seus pais concluírem. Cada thread recebe sua **própria conexão** com o banco, evitando conflitos de cursor.

```
Nível 0 (paralelo):    ingest_clientes  |  ingest_pedidos  |  ingest_produtos
                              ↓                  ↓                  ↓
Nível 1 (paralelo):     stg_clientes    |   stg_pedidos    |   stg_produtos
                              ↓                  ↓                  ↓
Nível 2 (sequencial):              fct_resumo_vendas
```

---

## Observabilidade

### Logs

O DataForge registra tudo em `logs/dfg.log`. O arquivo é organizado por **sessão diária**, identificada por um ID no formato `DDMMAADFG`.

```
================================================================================
SESSÃO INICIADA EM: 15/04/2026 08:30:12 | ID: 150426DFG
================================================================================

[EXECUÇÃO] Comando: dfg run

08:30:12 [info] [MainThread]: Iniciando pipeline no ambiente: PROD
08:30:12 [invocation] [ThreadPoolExecutor-0_0]: Extraindo/Ingerindo [PYTHON] 'ingest_pedidos'...
08:30:15 [ok] [ThreadPoolExecutor-0_0]: ✓ 'ingest_pedidos' concluído em 3.124s.
08:30:15 [invocation] [ThreadPoolExecutor-0_0]: Materializando [TABLE] 'stg_pedidos'...
08:30:15 [ok] [ThreadPoolExecutor-0_0]: ✓ 'stg_pedidos' concluído em 0.231s.
08:30:15 [ok] [MainThread]: --- Pipeline finalizado com sucesso em 3.891s! ---
```

**Níveis de log:**

| Nível | Cor | Uso |
|---|---|---|
| `debug` | Cinza | Detalhes internos (geração de artefatos, etc.) |
| `info` | Azul | Progresso geral da execução |
| `invocation` | Laranja | Início de operações no banco |
| `ok` | Verde | Conclusão bem-sucedida |
| `warning` | Amarelo | Avisos não-críticos |
| `error` | Vermelho | Erros que exigem atenção |

---

### manifest.json

Gerado em `target/manifest.json` a cada execução. Representa a topologia completa do projeto.

```json
{
    "metadata": {
        "generated_at": "2026-04-15T08:30:12.000000+00:00",
        "tool": "DataForge (DFG)"
    },
    "nodes": {
        "stg_pedidos": {
            "type": "sql",
            "materialized": "table",
            "description": "Pedidos limpos do sistema transacional.",
            "depends_on": ["ingest_pedidos"]
        },
        "fct_resumo": {
            "type": "sql",
            "materialized": "table",
            "description": "",
            "depends_on": ["stg_pedidos", "stg_clientes"]
        }
    },
    "dependencies": {
        "stg_pedidos": ["ingest_pedidos"],
        "fct_resumo": ["stg_pedidos", "stg_clientes"]
    }
}
```

---

### run_results.json

Gerado em `target/run_results.json` após cada execução de `dfg run`, `dfg ingest` ou `dfg transform`.

```json
{
    "metadata": {
        "generated_at": "2026-04-15T08:30:15.000000+00:00",
        "command": "dfg run",
        "tool": "DataForge (DFG)"
    },
    "summary": {
        "total": 5,
        "success": 5,
        "error": 0,
        "skipped": 0
    },
    "results": [
        {
            "model": "ingest_pedidos",
            "status": "success",
            "execution_time": 3.124,
            "rows": 1502
        },
        {
            "model": "stg_pedidos",
            "status": "success",
            "execution_time": 0.231,
            "rows": 0
        }
    ]
}
```

---

## Casos de Uso Completos

### Pipeline de E-commerce

Um pipeline para ingerir dados de pedidos e clientes de uma API, transformar em modelos analíticos e gerar KPIs diários.

**Estrutura:**

```
models/
├── schema.yml
├── ingest_clientes.py     # Extrai clientes da API
├── ingest_pedidos.py      # Extrai pedidos da API
├── stg_clientes.sql       # Staging de clientes (limpeza)
├── stg_pedidos.sql        # Staging de pedidos (limpeza)
├── fct_kpis_diarios.sql   # KPIs agregados por dia
└── fct_ltv_clientes.sql   # Lifetime Value por cliente
seeds/
└── categorias.csv
```

```python
# models/ingest_clientes.py
DEPENDENCIES = []

def model(context):
    import urllib.request, json
    with urllib.request.urlopen("https://api.loja.com/clientes") as r:
        return json.loads(r.read())["data"]
```

```python
# models/ingest_pedidos.py
DEPENDENCIES = []

def model(context):
    import urllib.request, json

    # Incremental: busca apenas pedidos desde o último estado
    desde = context["state"] or "2020-01-01"
    url = f"https://api.loja.com/pedidos?since={desde}"

    with urllib.request.urlopen(url) as r:
        pedidos = json.loads(r.read())["data"]

    if pedidos:
        context["set_state"](max(p["criado_em"] for p in pedidos))

    return pedidos
```

```sql
-- models/stg_pedidos.sql
{{ config(materialized='table') }}

SELECT
    id                  AS pedido_id,
    cliente_id,
    CAST(total AS FLOAT) AS valor_total,
    LOWER(status)       AS status,
    criado_em::TIMESTAMP AS criado_em
FROM {{ ref('ingest_pedidos') }}
WHERE id IS NOT NULL
```

```sql
-- models/fct_kpis_diarios.sql
{{ config(materialized='table') }}

SELECT
    DATE(p.criado_em)     AS data,
    COUNT(*)              AS total_pedidos,
    COUNT(DISTINCT p.cliente_id) AS clientes_unicos,
    SUM(p.valor_total)    AS receita_total,
    AVG(p.valor_total)    AS ticket_medio
FROM {{ ref('stg_pedidos') }} p
WHERE p.status = 'completo'
GROUP BY DATE(p.criado_em)
ORDER BY data DESC
```

```yaml
# models/schema.yml
version: 1
models:
  - name: stg_pedidos
    description: "Pedidos limpos e tipados."
    columns:
      - name: pedido_id
        tests: [not_null, unique]
      - name: valor_total
        tests: [not_null]
```

```bash
# Execução completa
dfg seed        # Carrega categorias.csv
dfg run         # Ingere e transforma tudo
dfg test        # Valida qualidade dos dados
dfg docs --serve # Visualiza o DAG
```

---

### Ingestão de API com Estado

Um modelo que consome paginação de uma API pública, lembrando a última página processada entre execuções.

```python
# models/ingest_github_eventos.py
DEPENDENCIES = []

def model(context):
    import urllib.request, json

    # Lê o estado anterior (última página processada)
    estado = context["state"] or {"ultima_pagina": 0, "total_registros": 0}
    proxima_pagina = estado["ultima_pagina"] + 1

    url = f"https://api.github.com/events?page={proxima_pagina}&per_page=100"
    req = urllib.request.Request(url, headers={"User-Agent": "DataForge/1.0"})

    with urllib.request.urlopen(req) as r:
        eventos = json.loads(r.read())

    if not eventos:
        return []

    # Atualiza o estado com a página processada e o total acumulado
    context["set_state"]({
        "ultima_pagina": proxima_pagina,
        "total_registros": estado["total_registros"] + len(eventos),
    })

    return [
        {
            "evento_id": e["id"],
            "tipo": e["type"],
            "ator": e["actor"]["login"],
            "repositorio": e["repo"]["name"],
            "criado_em": e["created_at"],
        }
        for e in eventos
    ]
```

Cada execução de `dfg ingest` processa a próxima página, nunca reprocessando dados já ingeridos.

---

### Auditoria com Snapshot SCD2

Rastrear o histórico completo de mudanças nos planos de assinatura dos clientes.

```sql
-- snapshots/hist_planos_clientes.sql
{% snapshot hist_planos_clientes %}

{{
    config(
        unique_key='cliente_id',
        updated_at='atualizado_em'
    )
}}

SELECT
    cliente_id,
    nome,
    email,
    plano_atual,
    valor_mensal,
    data_adesao,
    atualizado_em
FROM {{ ref('stg_clientes') }}

{% endsnapshot %}
```

```bash
# Roda diariamente para capturar mudanças
dfg snapshot
```

```sql
-- Consultas analíticas sobre o histórico

-- Quantos clientes fizeram upgrade de plano em Março/2024?
SELECT COUNT(DISTINCT cliente_id) AS upgrades
FROM hist_planos_clientes
WHERE dfg_valid_from BETWEEN '2024-03-01' AND '2024-03-31'
  AND plano_atual IN ('Premium', 'Enterprise')
  AND dfg_is_active = FALSE;  -- versão que foi substituída

-- Qual era o plano de um cliente específico em uma data passada?
SELECT plano_atual, valor_mensal
FROM hist_planos_clientes
WHERE cliente_id = 42
  AND dfg_valid_from <= '2024-02-15'
  AND (dfg_valid_to > '2024-02-15' OR dfg_valid_to IS NULL);
```

---

## Boas Práticas

**Organização de modelos:**
- Prefixe modelos com `ingest_` (Python), `stg_` (staging SQL), `int_` (intermediário), `fct_` (fatos), `dim_` (dimensões) e `vw_` (views)
- Mantenha um arquivo `schema.yml` com descrições para todos os modelos finais

**Performance:**
- Use `materialized='view'` para modelos consultados raramente
- Use `materialized='incremental'` com `unique_key` para tabelas grandes de eventos/logs
- Aumente `threads` em `dfg_project.toml` para pipelines com muitos modelos independentes

**Segurança:**
- Adicione `profiles.toml` ao `.gitignore` — ele contém senhas
- Use variáveis de ambiente para senhas em produção:

```python
# profiles.toml não suporta env vars nativamente.
# Use um script de geração ou substitua com sed no CI/CD:
# sed -i "s/SENHA_PLACEHOLDER/$DB_PASSWORD/" profiles.toml
```

**Reprodutibilidade:**
- Sempre versione `models/`, `seeds/`, `snapshots/` e `schema.yml`
- Nunca versione `target/` e `.dfg_state.json`

**.gitignore recomendado:**

```
# DataForge
profiles.toml
.dfg_state.json
target/
logs/
*.db
*.duckdb
```

---

## Solução de Problemas

**Erro: `Fatal error in launcher: Unable to create process`**

O `dfg.exe` foi instalado fora do virtualenv. Solução:

```bash
pip uninstall dfg -y
pip install -e .
```

---

**Erro: `'dfg_project.toml' não encontrado`**

O comando foi executado fora da raiz do projeto. Execute:

```bash
cd /caminho/do/projeto
dfg run
```

---

**Erro: `Driver 'psycopg2' não encontrado`**

```bash
pip install psycopg2-binary
```

---

**Erro: `Dependência circular detectada no DAG`**

Dois ou mais modelos se referenciam mutuamente via `{{ ref() }}`. Revise as dependências:

```bash
dfg compile   # Ajuda a identificar o SQL final de cada modelo
dfg docs      # Visualize o grafo para encontrar o ciclo
```

---

**Logs duplicados aparecendo no terminal**

Ocorre se `DFGEngine` for instanciado múltiplas vezes no mesmo processo Python (raro no uso via CLI). Cada instância chama `logger.setup()`, que já é idempotente. Se estiver usando a engine como biblioteca, mantenha uma única instância por processo.

---

**Snapshot não detecta mudanças**

Verifique se a coluna `updated_at` está sendo atualizada corretamente na fonte. O SCD2 do DataForge compara o valor de `updated_at` — se a fonte não atualizar esse campo, as mudanças não serão detectadas.

---
