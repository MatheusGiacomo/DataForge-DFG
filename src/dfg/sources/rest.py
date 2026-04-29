# src/dfg/sources/rest.py
"""
RestSource — Conector HTTP/REST para Sources do DataForge.

Fornece ingestão declarativa de APIs REST com suporte completo a:
    - Todos os métodos HTTP (GET, POST, PUT, PATCH)
    - Autenticação via BearerAuth, ApiKeyAuth, BasicAuth, OAuth2
    - Paginação automática (offset, page_number, cursor, link_header, next_url)
    - Retry com backoff exponencial em erros 4xx/5xx
    - Rate limiting configurável
    - Extração de dados aninhados via notação de ponto
    - Resolução de variáveis de ambiente {{ env('VAR') }}

Uso básico:
    from dfg.sources import RestSource

    source = RestSource(
        base_url="https://api.exemplo.com",
        auth={"type": "bearer", "token": "{{ env('API_TOKEN') }}"},
    )

    def model(context):
        return source.get("/v1/produtos", extract_path="data")

Com paginação automática:
    source = RestSource(
        base_url="https://api.github.com",
        auth={"type": "bearer", "token": "{{ env('GITHUB_TOKEN') }}"},
        pagination={"type": "link_header"},
        headers={"Accept": "application/vnd.github.v3+json"},
    )

    def model(context):
        return source.get("/orgs/minha-org/repos")
"""
import contextlib
import json
import time
import urllib.error
import urllib.parse
import urllib.request

from dfg.logging import logger
from dfg.sources._env import resolve
from dfg.sources.auth import AuthStrategy, build_auth
from dfg.sources.base import BaseSource
from dfg.sources.pagination import PaginationStrategy, build_pagination


class RestSource(BaseSource):
    """
    Conector declarativo para APIs REST.

    Parâmetros
    ----------
    base_url : str
        URL base da API (ex: "https://api.exemplo.com").
        Suporta {{ env('VAR') }}.
    auth : dict | AuthStrategy | None
        Configuração de autenticação. Aceita dicionário declarativo
        ou instância de AuthStrategy.
        Exemplos de dicionário:
            {"type": "bearer", "token": "{{ env('TOKEN') }}"}
            {"type": "api_key", "header": "X-API-Key", "key": "abc"}
            {"type": "basic", "username": "user", "password": "pass"}
            {"type": "oauth2", "client_id": "...", "client_secret": "...",
             "token_url": "https://auth.exemplo.com/token"}
    pagination : dict | PaginationStrategy | None
        Configuração de paginação. Aceita dicionário declarativo
        ou instância de PaginationStrategy.
        Exemplos de dicionário:
            {"type": "offset", "page_size": 200}
            {"type": "cursor", "cursor_param": "after", "cursor_path": "meta.cursor"}
            {"type": "link_header"}
    headers : dict | None
        Headers HTTP adicionais enviados em todos os requests.
    timeout : int
        Timeout em segundos para cada request (padrão: 30).
    rate_limit_rps : float | None
        Limite de requests por segundo. None = sem limite.
        Útil para respeitar limites de rate de APIs.
    max_retries : int
        Número máximo de tentativas em caso de falha (padrão: 3).
    retry_delay : float
        Delay inicial em segundos entre tentativas (padrão: 1.0).
    retry_backoff : float
        Multiplicador do delay a cada tentativa (padrão: 2.0).
    """

    _DEFAULT_HEADERS = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "DataForge-DFG/0.2.0",
    }

    def __init__(
        self,
        base_url: str,
        auth: dict | AuthStrategy | None = None,
        pagination: dict | PaginationStrategy | None = None,
        headers: dict | None = None,
        timeout: int = 30,
        rate_limit_rps: float | None = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        retry_backoff: float = 2.0,
    ):
        super().__init__(
            max_retries=max_retries,
            retry_delay=retry_delay,
            retry_backoff=retry_backoff,
        )
        self.base_url = resolve(base_url).rstrip("/")
        self.timeout = timeout
        self.rate_limit_rps = rate_limit_rps

        # Processa autenticação: dict → AuthStrategy ou mantém instância
        if isinstance(auth, dict):
            self._auth: AuthStrategy | None = build_auth(auth)
        else:
            self._auth = auth

        # Processa paginação: dict → PaginationStrategy ou mantém instância
        if isinstance(pagination, dict):
            self._pagination: PaginationStrategy | None = build_pagination(pagination)
        else:
            self._pagination = pagination

        # Merge de headers: padrões + customizados
        self._base_headers: dict = {
            **self._DEFAULT_HEADERS,
            **(resolve(headers) if headers else {}),
        }

        # Controle de rate limiting
        self._min_request_interval: float = (1.0 / rate_limit_rps) if rate_limit_rps else 0.0
        self._last_request_time: float = 0.0

    # ------------------------------------------------------------------
    # API Pública
    # ------------------------------------------------------------------

    def fetch(self) -> list[dict]:
        """
        Implementação de BaseSource.fetch().

        Executa um GET na `base_url` com paginação automática.
        Use `get()` para especificar um path e parâmetros.
        """
        return self.get("/")

    def get(
        self,
        path: str = "/",
        params: dict | None = None,
        extract_path: str | None = None,
    ) -> list[dict]:
        """
        Executa uma requisição GET com paginação automática.

        Parâmetros
        ----------
        path : str
            Path do endpoint relativo ao base_url (ex: "/v1/produtos").
        params : dict | None
            Query parameters adicionais fixos (além dos de paginação).
        extract_path : str | None
            Caminho em notação de ponto para extrair os registros da resposta
            (ex: "data", "results.items"). None extrai a resposta inteira.

        Retorna
        -------
        list[dict]
            Todos os registros coletados, incluindo todas as páginas.
        """
        return self._paginate("GET", path, params=params or {}, extract_path=extract_path)

    def post(
        self,
        path: str,
        body: dict | None = None,
        params: dict | None = None,
        extract_path: str | None = None,
    ) -> list[dict]:
        """
        Executa uma requisição POST.

        Útil para APIs GraphQL ou endpoints que recebem filtros via corpo.
        """
        return self._paginate(
            "POST", path, params=params or {}, body=body, extract_path=extract_path
        )

    # ------------------------------------------------------------------
    # Motor de Paginação
    # ------------------------------------------------------------------

    def _paginate(
        self,
        method: str,
        path: str,
        params: dict,
        extract_path: str | None,
        body: dict | None = None,
    ) -> list[dict]:
        """
        Loop principal de paginação.

        Itera pelas páginas usando a PaginationStrategy configurada até
        que não haja mais dados ou a estratégia sinalize o fim.
        """
        all_records: list[dict] = []

        if self._pagination:
            page_request = self._pagination.first_page()
            page_num = 1

            while page_request is not None:
                # Monta a URL: usa override_url se a estratégia fornecer
                if page_request.override_url:
                    url = page_request.override_url
                else:
                    merged_params = {**params, **page_request.params}
                    url = self._build_url(path, merged_params)

                response_data, response_headers = self._execute_with_retry(
                    self._request, method, url, body
                )

                records = self._extract_path(response_data, extract_path)

                if self._pagination.is_empty(records):
                    logger.debug(f"Página {page_num} vazia. Paginação encerrada.")
                    break

                all_records.extend(records)
                logger.debug(
                    f"Página {page_num}: {len(records)} registro(s) coletado(s). "
                    f"Total acumulado: {len(all_records)}."
                )

                page_request = self._pagination.next_page(response_data, response_headers)
                page_num += 1
        else:
            # Sem paginação: request único
            url = self._build_url(path, params)
            response_data, _ = self._execute_with_retry(self._request, method, url, body)
            all_records = self._extract_path(response_data, extract_path)

        logger.info(f"RestSource: {len(all_records)} registro(s) coletado(s) de '{path}'.")
        return all_records

    # ------------------------------------------------------------------
    # Execução de Request HTTP
    # ------------------------------------------------------------------

    def _request(self, method: str, url: str, body: dict | None) -> tuple[object, dict]:
        """
        Executa um único request HTTP e retorna (dados_decodificados, headers).

        Aplica autenticação, rate limiting e tratamento de erros HTTP.
        """
        self._apply_rate_limit()

        headers = dict(self._base_headers)
        extra_params: dict = {}

        # Aplica autenticação
        if self._auth:
            headers, extra_params = self._auth.apply(headers, extra_params)

        # Se auth adicionou params (ex: api_key via query param), mescla na URL
        if extra_params:
            parsed = urllib.parse.urlparse(url)
            existing = dict(urllib.parse.parse_qsl(parsed.query))
            merged = {**existing, **extra_params}
            url = parsed._replace(query=urllib.parse.urlencode(merged)).geturl()

        # Monta o request
        encoded_body = json.dumps(body).encode("utf-8") if body else None
        req = urllib.request.Request(url, data=encoded_body, headers=headers, method=method)

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw_body = resp.read().decode("utf-8")
                response_headers = dict(resp.headers)

            self._last_request_time = time.time()

            data = json.loads(raw_body) if raw_body.strip() else []
            return data, response_headers

        except urllib.error.HTTPError as e:
            error_body = ""
            with contextlib.suppress(Exception):
                error_body = e.read().decode("utf-8")[:500]
            raise RuntimeError(
                f"HTTP {e.code} ao acessar '{url}': {e.reason}. "
                f"Resposta: {error_body}"
            ) from e

        except urllib.error.URLError as e:
            raise RuntimeError(
                f"Falha de conexão ao acessar '{url}': {e.reason}"
            ) from e

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_url(self, path: str, params: dict) -> str:
        """Monta a URL completa com o path e os query parameters."""
        base = f"{self.base_url}/{path.lstrip('/')}"
        if params:
            query_string = urllib.parse.urlencode(
                {k: v for k, v in params.items() if v is not None}
            )
            return f"{base}?{query_string}"
        return base

    def _apply_rate_limit(self) -> None:
        """Aguarda o intervalo mínimo entre requests, se rate limiting estiver ativo."""
        if self._min_request_interval <= 0:
            return
        elapsed = time.time() - self._last_request_time
        wait_time = self._min_request_interval - elapsed
        if wait_time > 0:
            time.sleep(wait_time)
