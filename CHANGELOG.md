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
