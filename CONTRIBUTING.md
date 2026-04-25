# Contribuindo com o DataForge

Obrigado pelo interesse em contribuir! Este guia explica como configurar o ambiente de desenvolvimento, as convenções do projeto e o processo de envio de contribuições.

---

## Configurando o Ambiente

```bash
# 1. Fork e clone o repositório
git clone https://github.com/seu-usuario/dataforge
cd dataforge

# 2. Crie um ambiente virtual
python -m venv .venv
source .venv/bin/activate       # Linux/macOS
.venv\Scripts\Activate.ps1      # Windows

# 3. Instale em modo de desenvolvimento com dependências de dev
pip install -e ".[dev]"

# 4. Verifique que os testes passam
pytest

# 5. Verifique o linter
ruff check src/ tests/
```

---

## Estrutura do Repositório

```
dataforge/
├── src/dfg/            # Código-fonte principal
│   ├── adapters/       # Drivers de banco (base, generic, factory)
│   ├── engine.py       # Motor principal
│   ├── compiler.py     # Compilador Jinja2
│   ├── snapshot.py     # Motor SCD2
│   ├── seed.py         # Carga de CSVs
│   ├── cli.py          # Interface de linha de comando
│   └── ...
├── tests/              # Testes automatizados
├── pyproject.toml      # Configuração do projeto e ferramentas
├── README.md
├── DOCUMENTACAO.md
└── CHANGELOG.md
```

---

## Convenções

- **Código:** PEP 8, formatado com `ruff format`
- **Tipagem:** type hints em todos os métodos públicos
- **Docstrings:** obrigatório em classes e métodos públicos
- **Commits:** mensagens em português no imperativo (`Adiciona suporte a BigQuery`)
- **Testes:** todo novo código deve ter cobertura via pytest
- **Compatibilidade:** Python 3.11+, sem dependências além de jinja2 e pyyaml

---

## Adicionando um Novo Adaptador de Banco

1. Adicione o mapeamento `tipo → driver` em `adapters/factory.py` no dicionário `DRIVER_MAP`
2. Se o driver exigir comportamento diferente (ex: placeholder especial), sobrescreva os métodos necessários em uma subclasse de `GenericDBAPIAdapter`
3. Adicione testes em `tests/test_adapters.py`
4. Documente o novo banco em `DOCUMENTACAO.md` na seção "Bancos Suportados"

---

## Processo de Pull Request

1. Crie uma branch a partir de `main`: `git checkout -b feature/minha-feature`
2. Faça suas alterações com testes
3. Rode `ruff check src/ tests/` e `pytest` — ambos devem passar sem erros
4. Abra um Pull Request com descrição clara do que foi alterado e por quê
5. Aguarde revisão

---

## Reportando Bugs

Use o [GitHub Issues](https://github.com/seu-usuario/dataforge/issues) com:
- Versão do Python e do DataForge (`dfg --version`)
- Sistema operacional
- Banco de dados utilizado
- Comando executado
- Mensagem de erro completa
- Comportamento esperado vs. observado
