# src/dfg/sources/base.py
"""
Interface abstrata para todas as Sources do DataForge.

Toda nova source deve herdar de BaseSource e implementar o método
`fetch()`, que retorna uma lista de dicionários prontos para serem
carregados no banco de dados pelo motor.

Contrato garantido por todas as sources:
    - Retornam list[dict] — compatível com adapter.load_data()
    - Suportam resolução de {{ env('VAR') }} nas configurações
    - Herdam retry com backoff exponencial via RetryMixin
    - São stateless: podem ser reutilizadas em múltiplas execuções
"""
from abc import ABC, abstractmethod

from dfg.sources._retry import RetryMixin


class BaseSource(ABC, RetryMixin):
    """
    Classe base para todas as sources de ingestão do DataForge.

    Subclasses devem implementar `fetch()` e podem sobrescrever os
    parâmetros de retry herdados de RetryMixin.

    Parâmetros
    ----------
    max_retries : int
        Número máximo de tentativas em caso de falha (padrão: 3).
    retry_delay : float
        Delay inicial em segundos entre tentativas (padrão: 1.0).
    retry_backoff : float
        Multiplicador aplicado ao delay a cada falha (padrão: 2.0).
    """

    def __init__(
        self,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        retry_backoff: float = 2.0,
    ):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.retry_backoff = retry_backoff

    @abstractmethod
    def fetch(self) -> list[dict]:
        """
        Extrai dados da fonte e retorna como lista de dicionários.

        Este é o método principal que o modelo Python deve chamar.
        A implementação deve ser idempotente: chamadas repetidas com
        o mesmo estado não devem causar efeitos colaterais indesejados.

        Retorna
        -------
        list[dict]
            Lista de registros, onde cada dicionário representa uma linha.
            Chaves são os nomes das colunas e valores são os dados.
        """

    def _extract_path(self, data: dict | list, path: str | None) -> list[dict]:
        """
        Extrai dados aninhados usando notação de ponto.

        Permite que a source navegue em respostas JSON estruturadas
        para chegar à lista de registros desejada.

        Parâmetros
        ----------
        data : dict | list
            Dados brutos retornados pela fonte.
        path : str | None
            Caminho em notação de ponto (ex: "data.items").
            None retorna os dados diretamente.

        Exemplos
        --------
        >>> self._extract_path({"data": {"items": [...]}}, "data.items")
        [...]
        >>> self._extract_path([...], None)
        [...]

        Levanta
        -------
        KeyError
            Se alguma chave do caminho não existir nos dados.
        TypeError
            Se os dados no caminho não forem uma lista.
        """
        if path is None:
            if isinstance(data, list):
                return data
            raise TypeError(
                f"Esperava uma lista no topo da resposta, mas recebeu {type(data).__name__}. "
                f"Use 'extract_path' para especificar onde está a lista de registros."
            )

        current = data
        for key in path.split("."):
            if not isinstance(current, dict):
                raise KeyError(
                    f"Não foi possível navegar para '{key}' em '{path}': "
                    f"valor atual não é um dicionário."
                )
            if key not in current:
                raise KeyError(
                    f"Chave '{key}' não encontrada na resposta. "
                    f"Chaves disponíveis: {list(current.keys())}"
                )
            current = current[key]

        if not isinstance(current, list):
            raise TypeError(
                f"O caminho '{path}' aponta para {type(current).__name__}, "
                f"mas era esperada uma lista de registros."
            )

        return current
