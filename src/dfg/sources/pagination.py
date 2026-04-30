# src/dfg/sources/pagination.py
"""
Estratégias de Paginação para Sources HTTP do DataForge.

Implementa o padrão Strategy para os esquemas de paginação mais comuns
em APIs REST. Cada estratégia encapsula a lógica de:
    1. Construir os parâmetros do próximo request
    2. Extrair os dados da resposta atual
    3. Decidir quando parar (sem mais páginas)

Estratégias disponíveis:
    OffsetPagination     — ?offset=0&limit=100 (mais comum em APIs REST)
    PageNumberPagination — ?page=1&per_page=100
    CursorPagination     — ?cursor=<token> com cursor na resposta
    LinkHeaderPagination — header Link: <url>; rel="next" (GitHub, GitLab, etc.)
    NextUrlPagination    — próxima URL no corpo da resposta

Uso via dicionário de configuração:
    pagination = {"type": "offset", "page_size": 100}
    pagination = {"type": "cursor", "cursor_param": "after", "cursor_path": "meta.next_cursor"}
    pagination = {"type": "link_header"}
"""
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class PageRequest:
    """
    Representa os parâmetros necessários para requisitar uma página.

    Retornado por `PaginationStrategy.next_page()` para guiar o
    RestSource na construção do próximo request.
    """

    params: dict = field(default_factory=dict)
    # Quando a paginação usa URLs absolutas (ex: LinkHeader, NextUrl),
    # override_url sobrescreve completamente a URL base + path.
    override_url: str | None = None


class PaginationStrategy(ABC):
    """Interface abstrata para estratégias de paginação."""

    @abstractmethod
    def first_page(self) -> PageRequest:
        """Retorna os parâmetros para a primeira página."""

    @abstractmethod
    def next_page(self, response_data: dict | list, response_headers: dict) -> PageRequest | None:
        """
        Determina os parâmetros para a próxima página.

        Parâmetros
        ----------
        response_data : dict | list
            Corpo da resposta atual (já decodificado de JSON).
        response_headers : dict
            Headers da resposta atual.

        Retorna
        -------
        PageRequest
            Parâmetros para a próxima requisição.
        None
            Quando não há mais páginas (encerra a iteração).
        """

    def is_empty(self, records: list) -> bool:
        """Retorna True se a página não trouxe registros (condição de parada)."""
        return len(records) == 0


# ------------------------------------------------------------------
# Offset Pagination: ?offset=0&limit=100
# ------------------------------------------------------------------


class OffsetPagination(PaginationStrategy):
    """
    Paginação por offset numérico.

    Padrão: `GET /items?offset=0&limit=100` → `GET /items?offset=100&limit=100`

    Para quando:
    - A resposta retorna menos registros do que o `page_size`
    - A resposta contém um campo `total` que indica o total de registros

    Parâmetros
    ----------
    page_size : int
        Número de registros por página (padrão: 100).
    limit_param : str
        Nome do parâmetro de limite na query string (padrão: "limit").
    offset_param : str
        Nome do parâmetro de offset na query string (padrão: "offset").
    total_path : str | None
        Caminho em notação de ponto para o campo de total na resposta
        (ex: "meta.total"). None desabilita a verificação de total.
    """

    def __init__(
        self,
        page_size: int = 100,
        limit_param: str = "limit",
        offset_param: str = "offset",
        total_path: str | None = None,
    ):
        self.page_size = page_size
        self.limit_param = limit_param
        self.offset_param = offset_param
        self.total_path = total_path
        self._current_offset = 0
        self._total: int | None = None

    def first_page(self) -> PageRequest:
        self._current_offset = 0
        self._total = None
        return PageRequest(params={self.limit_param: self.page_size, self.offset_param: 0})

    def next_page(self, response_data: dict | list, response_headers: dict) -> PageRequest | None:
        # Tenta extrair o total da resposta na primeira página
        if self._total is None and self.total_path and isinstance(response_data, dict):
            self._total = _extract_nested(response_data, self.total_path)

        self._current_offset += self.page_size

        # Para se já buscamos tudo (pelo total conhecido)
        if self._total is not None and self._current_offset >= self._total:
            return None

        return PageRequest(
            params={
                self.limit_param: self.page_size,
                self.offset_param: self._current_offset,
            }
        )


# ------------------------------------------------------------------
# Page Number Pagination: ?page=1&per_page=100
# ------------------------------------------------------------------


class PageNumberPagination(PaginationStrategy):
    """
    Paginação por número de página.

    Padrão: `GET /items?page=1&per_page=100` → `GET /items?page=2&per_page=100`

    Para quando a resposta fica vazia ou quando um campo `total_pages`
    indica que chegamos ao fim.

    Parâmetros
    ----------
    page_size : int
        Número de registros por página (padrão: 100).
    page_param : str
        Nome do parâmetro de número de página (padrão: "page").
    per_page_param : str
        Nome do parâmetro de tamanho de página (padrão: "per_page").
    total_pages_path : str | None
        Caminho para o campo de total de páginas na resposta.
    first_page_number : int
        Número da primeira página (padrão: 1; algumas APIs usam 0).
    """

    def __init__(
        self,
        page_size: int = 100,
        page_param: str = "page",
        per_page_param: str = "per_page",
        total_pages_path: str | None = None,
        first_page_number: int = 1,
    ):
        self.page_size = page_size
        self.page_param = page_param
        self.per_page_param = per_page_param
        self.total_pages_path = total_pages_path
        self.first_page_number = first_page_number
        self._current_page = first_page_number
        self._total_pages: int | None = None

    def first_page(self) -> PageRequest:
        self._current_page = self.first_page_number
        self._total_pages = None
        return PageRequest(
            params={self.page_param: self._current_page, self.per_page_param: self.page_size}
        )

    def next_page(self, response_data: dict | list, response_headers: dict) -> PageRequest | None:
        if self._total_pages is None and self.total_pages_path and isinstance(response_data, dict):
            self._total_pages = _extract_nested(response_data, self.total_pages_path)

        self._current_page += 1

        if self._total_pages is not None and self._current_page > self._total_pages:
            return None

        return PageRequest(
            params={self.page_param: self._current_page, self.per_page_param: self.page_size}
        )


# ------------------------------------------------------------------
# Cursor Pagination: ?cursor=<token>
# ------------------------------------------------------------------


class CursorPagination(PaginationStrategy):
    """
    Paginação por cursor opaco.

    Cada resposta contém um cursor que aponta para a próxima página.
    Quando o cursor está ausente ou nulo, a paginação encerra.

    Comum em: Twitter/X API, Shopify, Stripe.

    Parâmetros
    ----------
    cursor_param : str
        Nome do query parameter onde o cursor é enviado (padrão: "cursor").
    cursor_path : str
        Caminho em notação de ponto para extrair o cursor da resposta
        (padrão: "next_cursor").
    """

    def __init__(self, cursor_param: str = "cursor", cursor_path: str = "next_cursor"):
        self.cursor_param = cursor_param
        self.cursor_path = cursor_path
        self._next_cursor: str | None = None

    def first_page(self) -> PageRequest:
        self._next_cursor = None
        # Primeira página não tem cursor
        return PageRequest(params={})

    def next_page(self, response_data: dict | list, response_headers: dict) -> PageRequest | None:
        if not isinstance(response_data, dict):
            return None

        cursor = _extract_nested(response_data, self.cursor_path)
        if not cursor:
            return None

        self._next_cursor = str(cursor)
        return PageRequest(params={self.cursor_param: self._next_cursor})


# ------------------------------------------------------------------
# Link Header Pagination (RFC 5988): header Link: <url>; rel="next"
# ------------------------------------------------------------------

_LINK_PATTERN = re.compile(r'<([^>]+)>;\s*rel="next"')


class LinkHeaderPagination(PaginationStrategy):
    """
    Paginação via header HTTP `Link` (RFC 5988).

    A resposta inclui um header no formato:
        `Link: <https://api.exemplo.com/items?page=2>; rel="next"`

    Quando o header não contém `rel="next"`, a paginação encerra.

    Comum em: GitHub API, GitLab API, Jira.
    """

    def first_page(self) -> PageRequest:
        return PageRequest(params={})

    def next_page(self, response_data: dict | list, response_headers: dict) -> PageRequest | None:
        link_header = response_headers.get("Link") or response_headers.get("link", "")
        match = _LINK_PATTERN.search(link_header)
        if not match:
            return None
        return PageRequest(override_url=match.group(1))


# ------------------------------------------------------------------
# Next URL Pagination: próxima URL no corpo da resposta
# ------------------------------------------------------------------


class NextUrlPagination(PaginationStrategy):
    """
    Paginação onde a próxima URL está no corpo da resposta JSON.

    Parâmetros
    ----------
    next_url_path : str
        Caminho em notação de ponto para a próxima URL
        (padrão: "next"). Exemplos: "links.next", "pagination.next_url".
    """

    def __init__(self, next_url_path: str = "next"):
        self.next_url_path = next_url_path

    def first_page(self) -> PageRequest:
        return PageRequest(params={})

    def next_page(self, response_data: dict | list, response_headers: dict) -> PageRequest | None:
        if not isinstance(response_data, dict):
            return None

        next_url = _extract_nested(response_data, self.next_url_path)
        if not next_url:
            return None

        return PageRequest(override_url=str(next_url))


# ------------------------------------------------------------------
# Factory: dict → PaginationStrategy
# ------------------------------------------------------------------


def build_pagination(config: dict | None) -> PaginationStrategy | None:
    """
    Cria uma instância de PaginationStrategy a partir de um dicionário.

    Parâmetros
    ----------
    config : dict | None
        Configuração da paginação. Deve conter "type" com um dos valores:
        "offset", "page_number", "cursor", "link_header", "next_url".
        None indica que a API não é paginada.

    Levanta
    -------
    ValueError
        Se o tipo de paginação não for reconhecido.
    """
    if not config:
        return None

    pagination_type = config.get("type", "").lower()

    if pagination_type == "offset":
        return OffsetPagination(
            page_size=config.get("page_size", 100),
            limit_param=config.get("limit_param", "limit"),
            offset_param=config.get("offset_param", "offset"),
            total_path=config.get("total_path"),
        )

    if pagination_type == "page_number":
        return PageNumberPagination(
            page_size=config.get("page_size", 100),
            page_param=config.get("page_param", "page"),
            per_page_param=config.get("per_page_param", "per_page"),
            total_pages_path=config.get("total_pages_path"),
            first_page_number=config.get("first_page_number", 1),
        )

    if pagination_type == "cursor":
        return CursorPagination(
            cursor_param=config.get("cursor_param", "cursor"),
            cursor_path=config.get("cursor_path", "next_cursor"),
        )

    if pagination_type == "link_header":
        return LinkHeaderPagination()

    if pagination_type == "next_url":
        return NextUrlPagination(next_url_path=config.get("next_url_path", "next"))

    supported = "offset, page_number, cursor, link_header, next_url"
    raise ValueError(
        f"Tipo de paginação '{pagination_type}' não reconhecido. "
        f"Tipos suportados: {supported}."
    )


# ------------------------------------------------------------------
# Utilitário interno
# ------------------------------------------------------------------


def _extract_nested(data: dict, path: str) -> object:
    """Navega em um dicionário usando notação de ponto. Retorna None se o caminho não existir."""
    current = data
    for key in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current
