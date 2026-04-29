# src/dfg/sources/__init__.py
"""
DataForge Sources — Conectores Nativos de Ingestão.

Fornece uma API declarativa e reutilizável para as fontes de dados
mais comuns, eliminando a necessidade de escrever código de ingestão
do zero em cada modelo Python.

Uso em um modelo Python:
    from dfg.sources import RestSource, FileSource, DatabaseSource
    from dfg.sources import S3Source, GCSSource, AzureBlobSource

Conectores disponíveis:
    RestSource      — APIs REST com autenticação e paginação automáticas
    FileSource      — Arquivos CSV, JSON, JSONL e Parquet (local ou remoto)
    DatabaseSource  — Extração de dados de bancos relacionais (DB-API 2.0)
    S3Source        — AWS S3 e storages S3-compatíveis (MinIO, R2, etc.)
    GCSSource       — Google Cloud Storage
    AzureBlobSource — Azure Blob Storage

Estratégias de autenticação (para uso direto com RestSource):
    BearerAuth    — Token Bearer / JWT
    ApiKeyAuth    — Chave de API via header ou query param
    BasicAuth     — HTTP Basic Authentication
    OAuth2Auth    — OAuth2 Client Credentials com cache de token

Estratégias de paginação (para uso direto com RestSource):
    OffsetPagination      — ?offset=0&limit=100
    PageNumberPagination  — ?page=1&per_page=100
    CursorPagination      — ?cursor=<token>
    LinkHeaderPagination  — header Link: <url>; rel="next"
    NextUrlPagination     — próxima URL no corpo da resposta
"""

from dfg.sources.auth import ApiKeyAuth, BasicAuth, BearerAuth, OAuth2Auth
from dfg.sources.cloud import AzureBlobSource, GCSSource, S3Source
from dfg.sources.database import DatabaseSource
from dfg.sources.file import FileSource
from dfg.sources.pagination import (
    CursorPagination,
    LinkHeaderPagination,
    NextUrlPagination,
    OffsetPagination,
    PageNumberPagination,
)
from dfg.sources.rest import RestSource

__all__ = [
    # Conectores principais
    "RestSource",
    "FileSource",
    "DatabaseSource",
    # Cloud storage
    "S3Source",
    "GCSSource",
    "AzureBlobSource",
    # Estratégias de autenticação
    "BearerAuth",
    "ApiKeyAuth",
    "BasicAuth",
    "OAuth2Auth",
    # Estratégias de paginação
    "OffsetPagination",
    "PageNumberPagination",
    "CursorPagination",
    "LinkHeaderPagination",
    "NextUrlPagination",
]
