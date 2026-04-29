# src/dfg/sources/_env.py
"""
Utilitário interno para resolução de variáveis de ambiente em configurações.

Permite que valores em dicionários de configuração referenciem variáveis de
ambiente usando a sintaxe {{ env('NOME_DA_VARIAVEL') }}, mantendo credenciais
fora do código-fonte.

Exemplos de uso em configuração:
    token   = "{{ env('API_TOKEN') }}"
    headers = {"Authorization": "Bearer {{ env('API_SECRET') }}"}
"""
import os
import re

# Padrão: {{ env('VAR') }} ou {{ env("VAR") }}
_ENV_PATTERN = re.compile(r"\{\{\s*env\(['\"](\w+)['\"]\)\s*\}\}")


def resolve(value: object) -> object:
    """
    Resolve recursivamente todas as referências {{ env('VAR') }} em um valor.

    Aceita strings, dicionários e listas. Qualquer outro tipo é retornado
    sem modificação.

    Parâmetros
    ----------
    value : object
        Valor a ser processado. Pode ser string, dict, list ou qualquer tipo.

    Levanta
    -------
    ValueError
        Se uma variável de ambiente referenciada não estiver definida.
    """
    if isinstance(value, str):
        return _resolve_string(value)
    if isinstance(value, dict):
        return {k: resolve(v) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve(item) for item in value]
    return value


def _resolve_string(value: str) -> str:
    """Substitui todas as ocorrências de {{ env('VAR') }} em uma string."""

    def _replacer(match: re.Match) -> str:
        var_name = match.group(1)
        env_value = os.environ.get(var_name)
        if env_value is None:
            raise ValueError(
                f"Variável de ambiente '{var_name}' não está definida. "
                f"Defina-a antes de executar o DataForge: "
                f"export {var_name}=seu_valor"
            )
        return env_value

    return _ENV_PATTERN.sub(_replacer, value)
