Aqui está uma documentação técnica concisa para a NexusData (sua "Data Forge"), focada na estrutura que consolidamos e nos comandos que você já implementou no CLI.

🛠️ NexusData: Documentação Técnica
A NexusData é uma ferramenta de CLI voltada para a orquestração e execução de processos de engenharia de dados (ETL/ELT). Ela automatiza a materialização de modelos, testes de qualidade e documentação de linhagem de dados.

📁 Estrutura do Projeto
Para que o sistema funcione corretamente, ele espera dois arquivos de configuração principais na raiz do diretório:

1. dfg_project.toml
É o cérebro do projeto. Ele define o nome do projeto, a versão e onde o NexusData deve procurar pelos modelos SQL e Python.

Campos principais: name, version, model-paths.

2. profiles.toml
Gerencia as conexões com os bancos de dados. Ele separa as credenciais por ambiente (ex: dev, prod), evitando que você rode scripts de teste em bancos de produção acidentalmente.

Exemplo de driver: duckdb, postgres.

⌨️ Referência de Comandos (CLI)
Comando	   Descrição
init	   Inicializa a estrutura de pastas (models/, logs/, seeds/) e arquivos .toml base.
run	       Executa a pipeline. Lê os modelos, resolve dependências e materializa os dados no destino.
test	   Executa as validações de dados (ex: not null, unique) definidas nos arquivos de schema.
debug	   Realiza um diagnóstico do ambiente: testa conexão com banco, verifica caminhos e permissões.
docs	   Gera uma interface (geralmente estática ou via servidor local) com o catálogo de dados.
compile	   Traduz os modelos (Jinja/SQL ou Python) em código executável pronto para o banco de dados.

Exportar para as Planilhas

📝 Sistema de Logging Inteligente
O logger da NexusData foi projetado para auditoria e facilidade de depuração via CLI.

IDs Diários: No primeiro comando do dia, o logger gera um cabeçalho com um ID no formato DDMMYYDFG.

Auditoria de Comandos: Cada execução pula uma linha e registra exatamente qual comando foi digitado (dfg run, dfg debug, etc.), permitindo saber quem fez o quê e quando.

Visual do Terminal: O timestamp é exibido em H:M:S. Apenas as tags de nível ([info], [error]) são coloridas para não poluir a leitura visual das mensagens.

🚀 Como iniciar um projeto rápido
Crie uma pasta para seu projeto: mkdir meu_projeto_dados && cd meu_projeto_dados.

Rode dfg init para criar o scaffold.

Configure seu banco de dados no profiles.toml.

Crie seu primeiro modelo SQL na pasta models/.

Execute dfg run para materializar seus dados.






1. Sistema de Logs e Auditoria (Logging)
A NexusData utiliza um sistema de log centralizado e determinístico. O objetivo é garantir que toda ação (de execução de modelos a simples visualização de docs) deixe um rastro auditável.

Arquitetura de Registro
Inicialização Global: O logger.setup() é invocado no ponto de entrada (cli.py), garantindo que todos os módulos (Engine, Docs, State) escrevam no mesmo arquivo de destino.

Prefixos de Contexto: Utilizamos prefixos como [DOCS], [RUN] e [TEST] para facilitar a filtragem via ferramentas de linha de comando ou grep.

Persistência: Os logs são salvos rotacionalmente por dia na pasta raiz do projeto.

Busca de Logs (dfg log)
O utilitário de busca foi atualizado para suportar a nova categoria de auditoria:

Comando: dfg log [LOG_ID] --docs

Funcionalidade: Filtra exclusivamente entradas relacionadas à geração de documentação e atividade do servidor web de linhagem.

2. Linhagem e Documentação (Docs)
A NexusData gera automaticamente uma representação visual da topologia do projeto (DAG - Directed Acyclic Graph).

Geração Estática
Ao executar dfg docs, o sistema:

Lê o manifest.json (gerado pelo compile).

Injeta a topologia em um template HTML moderno (Dark Mode).

Utiliza a biblioteca Vis.js para renderização reativa no lado do cliente.

Servidor de Visualização
O comando dfg docs --serve levanta um servidor http.server na porta 8080, permitindo a inspeção em tempo real da linhagem.

Azul (Hexágono): Modelos de Ingestão (Python/API).

Verde (Database): Modelos de Transformação (SQL).

3. Motor de Alta Performance (Engine)
O coração da NexusData foi reescrito para suportar paralelismo real e processamento incremental.

Execução Paralela baseada em Grafo
Em vez de uma lista linear, utilizamos o graphlib.TopologicalSorter do Python 3.9+.

Orquestração: O motor identifica quais modelos não possuem dependências pendentes e os despacha para um ThreadPoolExecutor.

Configuração de Threads: Definido via dfg_project.toml (padrão: 4 threads).

Thread-Safety: Cada worker thread instancia seu próprio adaptador de banco de dados, evitando corrupção de cursores SQL.

Materialização Incremental
Implementamos uma estratégia Idempotente para lidar com grandes volumes de dados sem reprocessar tabelas inteiras.

Configuração: Ativado via {{ config(materialized='incremental', unique_key='id') }}.

Lógica de Merge:

Criação de tabela temporária (__dfg_tmp).

Exclusão de registros existentes na tabela final que coincidam com a unique_key.

Inserção atômica dos novos dados.

4. Qualidade e Contratos de Dados (Test)
A validação de integridade agora é integrada ao ciclo de vida do pipeline.

Testes Nativos: not_null (validação de integridade) e unique (validação de chave primária).

Suporte Híbrido: Valida contratos definidos em módulos Python e prepara o terreno para definições em arquivos YAML para modelos SQL.

Garantia de Fluxo: Se um teste falha com erro crítico, o sys.exit(1) impede que processos subsequentes utilizem dados corrompidos.







📑 Governança com YAML (Data Contracts)
A NexusData adota o padrão de Contratos de Dados para garantir que as transformações sigam regras estritas de qualidade antes de serem disponibilizadas para consumo.

1. Separação de Responsabilidades
Para manter o projeto escalável, dividimos as definições em três camadas:

.toml: Configurações de infraestrutura, conexões e variáveis de ambiente (estático).

.sql / .py: Lógica bruta de transformação e extração (funcional).

.yml: Definições de metadados, descrições de colunas e testes de qualidade (governança).

2. Estrutura do Arquivo schema.yml
O arquivo de metadados deve ser colocado dentro da pasta models/. O parser da NexusData identifica automaticamente as definições e as vincula aos modelos correspondentes pelo nome.

YAML
version: 1

models:
  - name: stg_vendas
    description: "Dados brutos de vendas provenientes do ERP X."
    columns:
      - name: id_transacao
        tests:
          - unique
          - not_null
      - name: status
        tests:
          - not_null

  - name: dim_clientes
    description: "Tabela dimensional de clientes com dados higienizados."
    columns:
      - name: email
        tests:
          - not_null

⚙️ Funcionamento do Parser de Metadados
O motor da NexusData (engine.py) executa uma descoberta em duas fases para construir o models_registry:

Fase 1: Descoberta de Executáveis
O sistema mapeia todos os arquivos .sql e .py. Para modelos Python, ele importa o módulo dinamicamente usando importlib. Para SQL, ele extrai blocos de configuração interna (como materialized).

Fase 2: Enriquecimento (Overriding)
O parser YAML percorre os arquivos de metadados e realiza o "enxerto" das informações:

Descrições: Injeta a chave description no dicionário de configuração do modelo, que será usada para gerar o Catálogo de Dados visual.

Contratos: Mapeia a lista de tests de cada coluna para o formato interno de execução.

Precedência: As definições feitas no arquivo YAML têm prioridade total sobre configurações embutidas nos arquivos SQL ou Python, permitindo uma gestão centralizada da qualidade.

🔍 Validação em Tempo de Execução
Ao rodar o comando dfg test, a NexusData utiliza os metadados carregados para disparar queries de validação atômicas:

not_null: Executa um SELECT COUNT(*) buscando nulos na coluna especificada.

unique: Verifica se há duplicidade de chaves primárias usando agrupamento SQL.

Nota de Engenharia: Se um contrato falhar, o sistema interrompe o pipeline (sys.exit(1)), garantindo que dados de má qualidade não cheguem às camadas de análise (Silver/Gold).





🔍 Mecanismo de Auto-Discovery (Ambiente Adaptativo)
Diferente de ferramentas que exigem configuração manual prévia, o comando dfg init realiza uma varredura passiva no sistema para identificar capacidades de conexão.

1. Arquitetura de Verificação Silenciosa
O sistema utiliza a biblioteca importlib.util para inspecionar o sys.path. Para evitar erros fatais (como o ModuleNotFoundError em pacotes com submódulos), a verificação segue uma lógica de Validação de Raiz:

Extração de Base: O parser identifica o módulo raiz (ex: transforma mysql.connector em apenas mysql).

Inspeção de Spec: O find_spec verifica a existência dos binários ou scripts do pacote sem tentar carregá-los na memória RAM. Isso garante que o CLI permaneça leve e não cause efeitos colaterais durante a inicialização.

2. Catálogo Dinâmico de Adaptadores
A NexusData mantém um mapeamento interno (DB_CATALOG) que vincula bibliotecas Python a dialetos SQL e seus respectivos templates de configuração.

Banco de Dados, Biblioteca Alvo (Lib), Dialeto (Type)
DuckDB, duckdb, duckdb
PostgreSQL, psycopg2, postgres
MySQL, mysql, mysql
BigQuery, google, bigquery
Snowflake ,snowflake, snowflake
SQLite, sqlite3, sqlite

🛠️ Fluxo de Inicialização Inteligente
Ao executar o dfg init, o processo segue estas etapas:

Scanner de Dependências: O motor filtra o DB_CATALOG, retendo apenas as entradas onde is_lib_installed retorna True.

Interface de Seleção Numerada: Uma lista dinâmica é gerada. Se o usuário possuir apenas duckdb instalado, apenas esta opção (além do SQLite nativo) será exibida, evitando confusão.

Injeção de Boilerplate: Após a escolha, o sistema não gera um arquivo genérico. Ele utiliza o template de campos específico do banco escolhido (ex: host/port para Postgres vs database path para DuckDB).

Nota de Engenharia: Este mecanismo elimina o erro comum de "Comando não encontrado" ou "Driver ausente" no primeiro uso da ferramenta, garantindo uma experiência de usuário (UX) fluida desde o primeiro minuto.