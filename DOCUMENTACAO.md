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