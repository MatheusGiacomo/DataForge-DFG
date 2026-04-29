# src/dfg/sources/_retry.py
"""
Mixin de Retry com Backoff Exponencial para Sources do DataForge.

Implementa a lógica de reexecução de operações falhas com espera
progressiva (exponential backoff) e jitter aleatório para evitar
thundering herd em chamadas paralelas.

Comportamento padrão:
    Tentativa 1 → falha → aguarda 1.0s
    Tentativa 2 → falha → aguarda 2.0s
    Tentativa 3 → falha → aguarda 4.0s
    Tentativa 4 → propaga a exceção

O jitter adiciona até 10% aleatório ao delay para distribuir
as requisições quando múltiplos workers reiniciam ao mesmo tempo.
"""
import random
import time

from dfg.logging import logger

# Códigos HTTP que justificam retry (erros transientes de servidor)
RETRYABLE_HTTP_CODES: frozenset[int] = frozenset({408, 429, 500, 502, 503, 504})


class RetryMixin:
    """
    Mixin que adiciona capacidade de retry com backoff exponencial.

    Parâmetros de configuração (definidos no __init__ da subclasse):
        max_retries  (int)   : número máximo de tentativas (padrão: 3)
        retry_delay  (float) : delay inicial em segundos (padrão: 1.0)
        retry_backoff(float) : multiplicador do delay a cada falha (padrão: 2.0)
    """

    # Valores padrão — subclasses podem sobrescrever na instanciação
    max_retries: int = 3
    retry_delay: float = 1.0
    retry_backoff: float = 2.0

    def _execute_with_retry(self, operation, *args, **kwargs):
        """
        Executa `operation(*args, **kwargs)` com retry automático.

        A operação é reexecutada em caso de qualquer exceção, até o limite
        de `max_retries`. Na última tentativa, a exceção é propagada.

        Parâmetros
        ----------
        operation : callable
            Função a ser executada com retry.
        *args, **kwargs
            Argumentos repassados à operação.

        Retorna
        -------
        O valor retornado por `operation` em caso de sucesso.

        Levanta
        -------
        Exception
            A última exceção capturada após esgotar todas as tentativas.
        """
        delay = self.retry_delay
        last_exception: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                return operation(*args, **kwargs)
            except Exception as exc:
                last_exception = exc
                is_last_attempt = attempt >= self.max_retries

                if is_last_attempt:
                    logger.error(
                        f"Operação falhou após {self.max_retries} tentativa(s). "
                        f"Último erro: {exc}"
                    )
                    break

                # Jitter: adiciona entre 0% e 10% aleatório ao delay
                jitter = delay * random.uniform(0, 0.1)
                wait_time = delay + jitter

                logger.warn(
                    f"Tentativa {attempt}/{self.max_retries} falhou: {exc}. "
                    f"Aguardando {wait_time:.2f}s antes de tentar novamente..."
                )
                time.sleep(wait_time)
                delay *= self.retry_backoff

        raise last_exception