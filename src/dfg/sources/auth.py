# src/dfg/sources/auth.py
"""
Estratégias de Autenticação para Sources HTTP do DataForge.

Implementa o padrão Strategy para encapsular diferentes mecanismos
de autenticação. Cada estratégia sabe como adicionar suas credenciais
a um request HTTP (headers ou query params).

Estratégias disponíveis:
    ApiKeyAuth    — chave de API via header ou query parameter
    BearerAuth    — token Bearer no header Authorization
    BasicAuth     — HTTP Basic Authentication (usuário + senha)
    OAuth2Auth    — OAuth2 Client Credentials com cache de token

Uso via dicionário de configuração (interface declarativa):
    auth = {"type": "bearer", "token": "{{ env('API_TOKEN') }}"}
    auth = {"type": "api_key", "header": "X-API-Key", "key": "abc123"}
    auth = {"type": "basic", "username": "user", "password": "pass"}
    auth = {
        "type": "oauth2",
        "client_id": "{{ env('CLIENT_ID') }}",
        "client_secret": "{{ env('CLIENT_SECRET') }}",
        "token_url": "https://auth.exemplo.com/token",
    }
"""
import base64
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from abc import ABC, abstractmethod

from dfg.sources._env import resolve


class AuthStrategy(ABC):
    """Interface abstrata para estratégias de autenticação HTTP."""

    @abstractmethod
    def apply(self, headers: dict, params: dict) -> tuple[dict, dict]:
        """
        Adiciona as credenciais ao request.

        Parâmetros
        ----------
        headers : dict
            Headers HTTP do request a ser modificado.
        params : dict
            Query parameters do request a ser modificado.

        Retorna
        -------
        tuple[dict, dict]
            Par (headers, params) com as credenciais adicionadas.
        """


# ------------------------------------------------------------------
# Estratégia: API Key
# ------------------------------------------------------------------


class ApiKeyAuth(AuthStrategy):
    """
    Autenticação via chave de API.

    Pode ser enviada como header HTTP ou como query parameter,
    dependendo do que a API exige.

    Parâmetros
    ----------
    key : str
        Valor da chave de API.
    header : str | None
        Nome do header onde a chave será enviada.
        Ex: "X-API-Key", "Api-Token". Mutuamente exclusivo com `param`.
    param : str | None
        Nome do query parameter onde a chave será enviada.
        Ex: "api_key", "token". Mutuamente exclusivo com `header`.

    Levanta
    -------
    ValueError
        Se nem `header` nem `param` forem especificados.
    """

    def __init__(self, key: str, header: str | None = None, param: str | None = None):
        if not header and not param:
            raise ValueError(
                "ApiKeyAuth requer 'header' ou 'param'. "
                "Exemplo: ApiKeyAuth(key='abc', header='X-API-Key')"
            )
        self.key = resolve(key)
        self.header = header
        self.param = param

    def apply(self, headers: dict, params: dict) -> tuple[dict, dict]:
        if self.header:
            headers = {**headers, self.header: self.key}
        if self.param:
            params = {**params, self.param: self.key}
        return headers, params


# ------------------------------------------------------------------
# Estratégia: Bearer Token
# ------------------------------------------------------------------


class BearerAuth(AuthStrategy):
    """
    Autenticação via Bearer Token (OAuth2, JWT, tokens de API modernos).

    Adiciona o header: `Authorization: Bearer <token>`

    Parâmetros
    ----------
    token : str
        Token de autenticação. Suporta {{ env('VAR') }}.
    """

    def __init__(self, token: str):
        self.token = resolve(token)

    def apply(self, headers: dict, params: dict) -> tuple[dict, dict]:
        return {**headers, "Authorization": f"Bearer {self.token}"}, params


# ------------------------------------------------------------------
# Estratégia: Basic Auth
# ------------------------------------------------------------------


class BasicAuth(AuthStrategy):
    """
    HTTP Basic Authentication (RFC 7617).

    Codifica `username:password` em Base64 e adiciona o header
    `Authorization: Basic <credenciais_codificadas>`.

    Parâmetros
    ----------
    username : str
        Nome de usuário. Suporta {{ env('VAR') }}.
    password : str
        Senha. Suporta {{ env('VAR') }}.
    """

    def __init__(self, username: str, password: str):
        self.username = resolve(username)
        self.password = resolve(password)

    def apply(self, headers: dict, params: dict) -> tuple[dict, dict]:
        credentials = f"{self.username}:{self.password}"
        encoded = base64.b64encode(credentials.encode("utf-8")).decode("ascii")
        return {**headers, "Authorization": f"Basic {encoded}"}, params


# ------------------------------------------------------------------
# Estratégia: OAuth2 Client Credentials
# ------------------------------------------------------------------


class OAuth2Auth(AuthStrategy):
    """
    OAuth2 Client Credentials Flow (RFC 6749 §4.4).

    Obtém automaticamente um access token via POST para o `token_url`
    e o reutiliza enquanto não expirar. Quando o token expira,
    um novo é obtido automaticamente de forma transparente.

    Parâmetros
    ----------
    client_id : str
        Client ID da aplicação. Suporta {{ env('VAR') }}.
    client_secret : str
        Client Secret da aplicação. Suporta {{ env('VAR') }}.
    token_url : str
        Endpoint de autenticação (ex: "https://auth.exemplo.com/token").
    scopes : list[str] | None
        Escopos de acesso solicitados (opcional).
    token_field : str
        Campo no JSON de resposta que contém o token (padrão: "access_token").
    expires_in_field : str
        Campo no JSON de resposta com o tempo de expiração em segundos
        (padrão: "expires_in").
    leeway_seconds : int
        Margem de segurança em segundos antes da expiração para renovar
        o token proativamente (padrão: 30).
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        token_url: str,
        scopes: list[str] | None = None,
        token_field: str = "access_token",
        expires_in_field: str = "expires_in",
        leeway_seconds: int = 30,
    ):
        self.client_id = resolve(client_id)
        self.client_secret = resolve(client_secret)
        self.token_url = token_url
        self.scopes = scopes or []
        self.token_field = token_field
        self.expires_in_field = expires_in_field
        self.leeway_seconds = leeway_seconds

        # Cache interno do token
        self._access_token: str | None = None
        self._expires_at: float = 0.0

    @property
    def _token_is_valid(self) -> bool:
        """Verifica se o token em cache ainda é válido considerando a margem de segurança."""
        return self._access_token is not None and time.time() < (self._expires_at - self.leeway_seconds)

    def _fetch_token(self) -> None:
        """Obtém um novo access token via Client Credentials."""
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        if self.scopes:
            payload["scope"] = " ".join(self.scopes)

        data = urllib.parse.urlencode(payload).encode("utf-8")
        req = urllib.request.Request(
            self.token_url,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                token_data = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            raise RuntimeError(
                f"OAuth2: falha ao obter token em '{self.token_url}': "
                f"HTTP {e.code} — {e.reason}"
            ) from e

        if self.token_field not in token_data:
            raise RuntimeError(
                f"OAuth2: campo '{self.token_field}' não encontrado na resposta. "
                f"Campos disponíveis: {list(token_data.keys())}"
            )

        self._access_token = token_data[self.token_field]
        expires_in = token_data.get(self.expires_in_field, 3600)
        self._expires_at = time.time() + float(expires_in)

    def apply(self, headers: dict, params: dict) -> tuple[dict, dict]:
        if not self._token_is_valid:
            self._fetch_token()
        return {**headers, "Authorization": f"Bearer {self._access_token}"}, params


# ------------------------------------------------------------------
# Factory: dict → AuthStrategy
# ------------------------------------------------------------------


def build_auth(config: dict | None) -> AuthStrategy | None:
    """
    Cria uma instância de AuthStrategy a partir de um dicionário de configuração.

    Interface declarativa que evita a necessidade de importar as classes
    diretamente em cada modelo.

    Parâmetros
    ----------
    config : dict | None
        Configuração da autenticação. Deve conter a chave "type" com um
        dos valores: "api_key", "bearer", "basic", "oauth2".
        None retorna None (sem autenticação).

    Exemplos
    --------
    >>> build_auth({"type": "bearer", "token": "{{ env('TOKEN') }}"})
    BearerAuth(...)

    >>> build_auth({"type": "api_key", "header": "X-API-Key", "key": "abc"})
    ApiKeyAuth(...)

    Levanta
    -------
    ValueError
        Se o tipo de autenticação não for reconhecido.
    """
    if not config:
        return None

    config = resolve(config)
    auth_type = config.get("type", "").lower()

    if auth_type == "bearer":
        return BearerAuth(token=config["token"])

    if auth_type == "api_key":
        return ApiKeyAuth(
            key=config["key"],
            header=config.get("header"),
            param=config.get("param"),
        )

    if auth_type == "basic":
        return BasicAuth(username=config["username"], password=config["password"])

    if auth_type == "oauth2":
        return OAuth2Auth(
            client_id=config["client_id"],
            client_secret=config["client_secret"],
            token_url=config["token_url"],
            scopes=config.get("scopes"),
        )

    supported = "bearer, api_key, basic, oauth2"
    raise ValueError(
        f"Tipo de autenticação '{auth_type}' não reconhecido. "
        f"Tipos suportados: {supported}."
    )
