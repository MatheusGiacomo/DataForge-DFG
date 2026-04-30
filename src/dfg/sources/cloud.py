# src/dfg/sources/cloud.py
"""
Cloud Storage Sources para o DataForge.

Fornece conectores nativos para os três principais provedores de
object storage, com suporte a arquivo único e ingestão em batch
(múltiplos objetos via prefixo + glob).

Conectores disponíveis:
    S3Source        — AWS S3 e storages compatíveis (MinIO, Cloudflare R2,
                      DigitalOcean Spaces, Oracle OCI, etc.)
    GCSSource       — Google Cloud Storage
    AzureBlobSource — Azure Blob Storage

Esquemas de URL suportados:
    S3    : s3://bucket-name/path/to/file.csv
    GCS   : gs://bucket-name/path/to/file.json
    Azure : az://container-name/path/to/file.parquet

Todos os conectores:
    - Importam o SDK do provedor de forma LAZY (só quando usado), sem
      adicionar dependências obrigatórias ao DataForge.
    - Suportam {{ env('VAR') }} em qualquer parâmetro de configuração.
    - Herdam retry com backoff exponencial de BaseCloudSource.
    - Detectam o formato do arquivo pela extensão (CSV, JSON, JSONL, Parquet).
    - Suportam fetch_many() para ingestão em batch via prefixo + glob.

Instalação dos SDKs (apenas o provedor que você usa):
    pip install boto3                          # S3 / AWS
    pip install google-cloud-storage           # GCS
    pip install azure-storage-blob             # Azure Blob

Uso básico:
    from dfg.sources.cloud import S3Source

    source = S3Source(
        uri="s3://meu-bucket/dados/clientes.csv",
        region="us-east-1",
        access_key="{{ env('AWS_ACCESS_KEY_ID') }}",
        secret_key="{{ env('AWS_SECRET_ACCESS_KEY') }}",
    )

    def model(context):
        return source.fetch()

Ingestão em batch (múltiplos arquivos):
    source = S3Source(
        uri="s3://meu-bucket/dados/",
        region="us-east-1",
        access_key="{{ env('AWS_ACCESS_KEY_ID') }}",
        secret_key="{{ env('AWS_SECRET_ACCESS_KEY') }}",
    )

    def model(context):
        # Lê todos os .csv no prefixo dados/pedidos/ e mescla em uma lista
        return source.fetch_many(prefix="dados/pedidos/", pattern="*.csv")
"""
import fnmatch
import io
import os
from abc import abstractmethod
from urllib.parse import urlparse

from dfg.logging import logger
from dfg.sources._env import resolve
from dfg.sources.file import FileSource

# =============================================================================
# Base: comportamento comum a todos os conectores cloud
# =============================================================================


class BaseCloudSource(FileSource):
    """
    Classe base para conectores de cloud object storage.

    Estende FileSource sobrescrevendo o método `_read_content()` para
    usar o SDK do provedor ao invés de HTTP genérico. Toda a lógica de
    parsing de formatos (CSV, JSON, JSONL, Parquet) é herdada de FileSource.

    Adiciona `fetch_many()` para ingestão em batch de múltiplos objetos.

    Parâmetros
    ----------
    uri : str
        URI do objeto no formato do provedor:
        s3://bucket/key | gs://bucket/key | az://container/blob
    format : str | None
        Formato explícito. None = detecção automática pela extensão.
    extract_path : str | None
        Caminho em notação de ponto para extrair registros em JSONs aninhados.
    encoding : str
        Encoding para arquivos de texto (padrão: "utf-8-sig").
    delimiter : str
        Delimitador CSV (padrão: ",").
    max_retries : int
        Tentativas em caso de falha (padrão: 3).
    """

    def __init__(
        self,
        uri: str,
        format: str | None = None,  # noqa: A002
        extract_path: str | None = None,
        encoding: str = "utf-8-sig",
        delimiter: str = ",",
        max_retries: int = 3,
        retry_delay: float = 1.0,
        retry_backoff: float = 2.0,
        **provider_kwargs,
    ):
        resolved_uri = resolve(uri)
        super().__init__(
            path=resolved_uri,
            format=format,
            extract_path=extract_path,
            encoding=encoding,
            delimiter=delimiter,
            max_retries=max_retries,
            retry_delay=retry_delay,
            retry_backoff=retry_backoff,
        )
        self.uri = resolved_uri
        self._provider_kwargs = {k: resolve(v) if isinstance(v, str) else v
                                 for k, v in provider_kwargs.items()}
        self._bucket, self._key = self._parse_uri(resolved_uri)
        self._client = None

    # ------------------------------------------------------------------
    # URI Parsing
    # ------------------------------------------------------------------

    def _parse_uri(self, uri: str) -> tuple[str, str]:
        """
        Extrai bucket (ou container) e key (ou blob path) do URI.

        Exemplos:
            s3://my-bucket/data/file.csv  → ("my-bucket", "data/file.csv")
            gs://my-bucket/path/          → ("my-bucket", "path/")
        """
        parsed = urlparse(uri)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")

        if not bucket:
            raise ValueError(
                f"URI inválido: '{uri}'. "
                f"Formato esperado: s3://bucket/key, gs://bucket/key ou az://container/blob"
            )

        return bucket, key

    # ------------------------------------------------------------------
    # Interface a implementar por cada provedor
    # ------------------------------------------------------------------

    @abstractmethod
    def _get_client(self):
        """Retorna o cliente autenticado do SDK do provedor (lazy init com cache)."""

    @abstractmethod
    def _download_object(self, bucket: str, key: str) -> bytes:
        """Faz o download de um único objeto e retorna seus bytes."""

    @abstractmethod
    def _list_objects(self, bucket: str, prefix: str) -> list[str]:
        """
        Lista as chaves de todos os objetos em um bucket com o prefixo dado.

        Retorna uma lista de strings com os caminhos completos dos objetos
        (sem o nome do bucket), ex: ["dados/jan.csv", "dados/fev.csv"].
        """

    # ------------------------------------------------------------------
    # FileSource: override de _read_content()
    # ------------------------------------------------------------------

    def _read_content(self) -> bytes:
        """Faz o download do objeto cloud ao invés de ler do filesystem local."""
        logger.debug(f"{self.__class__.__name__}: baixando '{self.uri}'...")
        return self._execute_with_retry(self._download_object, self._bucket, self._key)

    # ------------------------------------------------------------------
    # API Pública: fetch_many() — ingestão em batch
    # ------------------------------------------------------------------

    def fetch_many(
        self,
        prefix: str = "",
        pattern: str = "*",
        merge: bool = True,
    ) -> list[dict] | dict[str, list[dict]]:
        """
        Lê e parseia múltiplos objetos que correspondem ao prefixo e padrão.

        Útil para ingerir um conjunto de arquivos de uma só vez:
        todos os CSVs de um diretório, todos os JSONs de um prefixo, etc.

        Parâmetros
        ----------
        prefix : str
            Prefixo do caminho dos objetos a listar (ex: "dados/pedidos/").
            String vazia lista todos os objetos no bucket.
        pattern : str
            Padrão glob para filtrar objetos pelo nome do arquivo
            (ex: "*.csv", "vendas_2024*.json"). Padrão: "*" (todos).
        merge : bool
            True  → mescla todos os registros em uma única lista (padrão).
            False → retorna um dicionário {chave: lista_de_registros}.

        Retorna
        -------
        list[dict]
            Quando merge=True: todos os registros de todos os arquivos mesclados.
        dict[str, list[dict]]
            Quando merge=False: mapeamento {chave_do_objeto: registros}.

        Exemplo
        -------
        >>> # Ingere todos os CSVs diários e mescla em uma tabela
        >>> source.fetch_many(prefix="exports/daily/", pattern="*.csv")
        [{"id": 1, ...}, {"id": 2, ...}, ...]
        """
        logger.info(
            f"{self.__class__.__name__}: listando objetos em "
            f"'{self._bucket}/{prefix}' com padrão '{pattern}'..."
        )

        all_keys = self._execute_with_retry(self._list_objects, self._bucket, prefix)

        # Filtra pelo padrão glob usando apenas o nome do arquivo (não o path completo)
        matched_keys = [
            k for k in all_keys
            if fnmatch.fnmatch(os.path.basename(k), pattern)
        ]

        if not matched_keys:
            logger.warn(
                f"{self.__class__.__name__}: nenhum objeto encontrado em "
                f"'{self._bucket}/{prefix}' com padrão '{pattern}'."
            )
            return [] if merge else {}

        logger.info(
            f"{self.__class__.__name__}: {len(matched_keys)} objeto(s) encontrado(s). "
            f"Iniciando download e parsing..."
        )

        results: dict[str, list[dict]] = {}
        total_records = 0

        for key in sorted(matched_keys):
            try:
                content = self._execute_with_retry(self._download_object, self._bucket, key)

                # Usa o formato da extensão do arquivo atual, não o da URI principal
                _, ext = os.path.splitext(key.split("?")[0].lower())
                file_format = self._FORMAT_MAP.get(ext, self._format)

                # Cria um FileSource temporário apenas para parsear o conteúdo
                temp = _InMemoryFileParser(
                    content=content,
                    file_format=file_format,
                    extract_path=self.extract_path,
                    encoding=self.encoding,
                    delimiter=self.delimiter,
                )
                records = temp.parse()
                results[key] = records
                total_records += len(records)

                logger.debug(f"  {key}: {len(records)} registro(s).")

            except Exception as e:
                logger.error(
                    f"{self.__class__.__name__}: falha ao processar '{key}': {e}. "
                    f"Objeto ignorado."
                )

        logger.info(
            f"{self.__class__.__name__}: {total_records} registro(s) total "
            f"de {len(results)}/{len(matched_keys)} objeto(s)."
        )

        if merge:
            return [record for records in results.values() for record in records]

        return results


# =============================================================================
# S3Source — AWS S3 e storages S3-compatíveis
# =============================================================================


class S3Source(BaseCloudSource):
    """
    Conector para AWS S3 e storages S3-compatíveis.

    S3-compatíveis suportados via `endpoint_url`:
        MinIO            : endpoint_url="http://localhost:9000"
        Cloudflare R2    : endpoint_url="https://<account_id>.r2.cloudflarestorage.com"
        DigitalOcean Spaces: endpoint_url="https://<region>.digitaloceanspaces.com"
        Backblaze B2     : endpoint_url="https://s3.<region>.backblazeb2.com"
        Oracle OCI       : endpoint_url="https://<namespace>.compat.objectstorage.<region>.oraclecloud.com"

    Autenticação (em ordem de prioridade):
        1. Parâmetros explícitos: `access_key` + `secret_key`
        2. Variáveis de ambiente: AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY
        3. AWS profiles (~/.aws/credentials)
        4. IAM Role (EC2, ECS, Lambda — sem credenciais necessárias)

    Parâmetros
    ----------
    uri : str
        URI do objeto: "s3://bucket-name/path/to/file.csv"
    region : str | None
        Região AWS (ex: "us-east-1", "sa-east-1"). Padrão: AWS_DEFAULT_REGION.
    access_key : str | None
        AWS Access Key ID. Suporta {{ env('VAR') }}.
    secret_key : str | None
        AWS Secret Access Key. Suporta {{ env('VAR') }}.
    session_token : str | None
        Token de sessão para credenciais temporárias (STS AssumeRole).
    endpoint_url : str | None
        Endpoint customizado para storages S3-compatíveis.
        None = AWS S3 padrão.
    use_ssl : bool
        Usa HTTPS na conexão (padrão: True).

    Exemplo
    -------
    >>> from dfg.sources.cloud import S3Source
    >>>
    >>> # AWS S3 com credenciais explícitas
    >>> source = S3Source(
    ...     uri="s3://meu-bucket/vendas/2024/clientes.csv",
    ...     region="sa-east-1",
    ...     access_key="{{ env('AWS_ACCESS_KEY_ID') }}",
    ...     secret_key="{{ env('AWS_SECRET_ACCESS_KEY') }}",
    ... )
    >>>
    >>> # MinIO local (S3-compatível)
    >>> source = S3Source(
    ...     uri="s3://meu-bucket/dados.json",
    ...     endpoint_url="http://localhost:9000",
    ...     access_key="{{ env('MINIO_USER') }}",
    ...     secret_key="{{ env('MINIO_PASSWORD') }}",
    ... )
    """

    def __init__(
        self,
        uri: str,
        region: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        session_token: str | None = None,
        endpoint_url: str | None = None,
        use_ssl: bool = True,
        format: str | None = None,  # noqa: A002
        extract_path: str | None = None,
        encoding: str = "utf-8-sig",
        delimiter: str = ",",
        max_retries: int = 3,
        retry_delay: float = 1.0,
        retry_backoff: float = 2.0,
    ):
        super().__init__(
            uri=uri,
            format=format,
            extract_path=extract_path,
            encoding=encoding,
            delimiter=delimiter,
            max_retries=max_retries,
            retry_delay=retry_delay,
            retry_backoff=retry_backoff,
        )
        self._region = resolve(region) if region else None
        self._access_key = resolve(access_key) if access_key else None
        self._secret_key = resolve(secret_key) if secret_key else None
        self._session_token = resolve(session_token) if session_token else None
        self._endpoint_url = resolve(endpoint_url) if endpoint_url else None
        self._use_ssl = use_ssl

    def _get_client(self):
        """Cria e armazena em cache o cliente boto3 S3."""
        if self._client is not None:
            return self._client

        try:
            import boto3  # noqa: PLC0415
        except ImportError as e:
            raise ImportError(
                "S3Source requer 'boto3'. Instale com: pip install boto3"
            ) from e

        kwargs: dict = {"use_ssl": self._use_ssl}

        if self._region:
            kwargs["region_name"] = self._region
        if self._endpoint_url:
            kwargs["endpoint_url"] = self._endpoint_url
        if self._access_key and self._secret_key:
            kwargs["aws_access_key_id"] = self._access_key
            kwargs["aws_secret_access_key"] = self._secret_key
        if self._session_token:
            kwargs["aws_session_token"] = self._session_token

        self._client = boto3.client("s3", **kwargs)
        return self._client

    def _download_object(self, bucket: str, key: str) -> bytes:
        """Download de um objeto S3 como bytes."""
        client = self._get_client()
        buffer = io.BytesIO()
        try:
            client.download_fileobj(bucket, key, buffer)
        except Exception as e:
            # Extrai mensagem de erro legível do ClientError do boto3
            error_code = getattr(getattr(e, "response", {}).get("Error", {}), "get", lambda k, d=None: d)("Code", "")
            if error_code == "NoSuchKey":
                raise FileNotFoundError(f"S3Source: objeto não encontrado: 's3://{bucket}/{key}'") from e
            if error_code == "NoSuchBucket":
                raise FileNotFoundError(f"S3Source: bucket não encontrado: '{bucket}'") from e
            raise RuntimeError(f"S3Source: falha ao baixar 's3://{bucket}/{key}': {e}") from e

        return buffer.getvalue()

    def _list_objects(self, bucket: str, prefix: str) -> list[str]:
        """
        Lista todos os objetos no bucket com o prefixo dado.

        Usa paginação automática via S3 list_objects_v2 para suportar
        buckets com mais de 1.000 objetos.
        """
        client = self._get_client()
        keys: list[str] = []
        paginator = client.get_paginator("list_objects_v2")

        try:
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    # Exclui "diretórios" virtuais (chaves que terminam em /)
                    if not key.endswith("/"):
                        keys.append(key)
        except Exception as e:
            raise RuntimeError(
                f"S3Source: falha ao listar objetos em 's3://{bucket}/{prefix}': {e}"
            ) from e

        return keys


# =============================================================================
# GCSSource — Google Cloud Storage
# =============================================================================


class GCSSource(BaseCloudSource):
    """
    Conector para Google Cloud Storage (GCS).

    Autenticação (em ordem de prioridade):
        1. Parâmetro explícito: `credentials_path` (caminho para o JSON da service account)
        2. Variável de ambiente: GOOGLE_APPLICATION_CREDENTIALS
        3. Application Default Credentials (ADC) — gcloud auth, Workload Identity, etc.

    Parâmetros
    ----------
    uri : str
        URI do objeto: "gs://bucket-name/path/to/file.csv"
    credentials_path : str | None
        Caminho para o arquivo JSON da service account.
        Suporta {{ env('VAR') }}.
        None = usa Application Default Credentials.
    project : str | None
        ID do projeto GCP. Geralmente inferido das credenciais.

    Exemplo
    -------
    >>> from dfg.sources.cloud import GCSSource
    >>>
    >>> source = GCSSource(
    ...     uri="gs://meu-bucket/dados/vendas.parquet",
    ...     credentials_path="{{ env('GOOGLE_SA_KEY_PATH') }}",
    ... )
    >>>
    >>> # Com ADC (Application Default Credentials)
    >>> source = GCSSource(uri="gs://meu-bucket/dados/")
    """

    def __init__(
        self,
        uri: str,
        credentials_path: str | None = None,
        project: str | None = None,
        format: str | None = None,  # noqa: A002
        extract_path: str | None = None,
        encoding: str = "utf-8-sig",
        delimiter: str = ",",
        max_retries: int = 3,
        retry_delay: float = 1.0,
        retry_backoff: float = 2.0,
    ):
        super().__init__(
            uri=uri,
            format=format,
            extract_path=extract_path,
            encoding=encoding,
            delimiter=delimiter,
            max_retries=max_retries,
            retry_delay=retry_delay,
            retry_backoff=retry_backoff,
        )
        self._credentials_path = resolve(credentials_path) if credentials_path else None
        self._project = resolve(project) if project else None

    def _get_client(self):
        """Cria e armazena em cache o cliente GCS."""
        if self._client is not None:
            return self._client

        try:
            from google.cloud import storage as gcs  # noqa: PLC0415
            from google.oauth2 import service_account  # noqa: PLC0415
        except ImportError as e:
            raise ImportError(
                "GCSSource requer 'google-cloud-storage'. "
                "Instale com: pip install google-cloud-storage"
            ) from e

        if self._credentials_path:
            # Resolve caminho: suporta ~ e variáveis de caminho
            creds_path = os.path.expanduser(self._credentials_path)
            if not os.path.exists(creds_path):
                raise FileNotFoundError(
                    f"GCSSource: arquivo de credenciais não encontrado: '{creds_path}'"
                )
            credentials = service_account.Credentials.from_service_account_file(creds_path)
            self._client = gcs.Client(credentials=credentials, project=self._project)
        else:
            # Application Default Credentials
            self._client = gcs.Client(project=self._project)

        return self._client

    def _download_object(self, bucket: str, key: str) -> bytes:
        """Download de um objeto GCS como bytes."""
        client = self._get_client()
        try:
            bucket_obj = client.bucket(bucket)
            blob = bucket_obj.blob(key)
            return blob.download_as_bytes()
        except Exception as e:
            error_str = str(e).lower()
            if "not found" in error_str or "404" in error_str:
                raise FileNotFoundError(
                    f"GCSSource: objeto não encontrado: 'gs://{bucket}/{key}'"
                ) from e
            raise RuntimeError(
                f"GCSSource: falha ao baixar 'gs://{bucket}/{key}': {e}"
            ) from e

    def _list_objects(self, bucket: str, prefix: str) -> list[str]:
        """Lista todos os blobs no bucket com o prefixo dado."""
        client = self._get_client()
        try:
            blobs = client.list_blobs(bucket, prefix=prefix)
            return [
                blob.name
                for blob in blobs
                if not blob.name.endswith("/")
            ]
        except Exception as e:
            raise RuntimeError(
                f"GCSSource: falha ao listar objetos em 'gs://{bucket}/{prefix}': {e}"
            ) from e


# =============================================================================
# AzureBlobSource — Azure Blob Storage
# =============================================================================


class AzureBlobSource(BaseCloudSource):
    """
    Conector para Azure Blob Storage.

    O "bucket" no URI az:// corresponde ao nome do **container** no Azure.

    Autenticação — escolha um dos métodos:
        1. `connection_string` — connection string completa do storage account
        2. `account_name` + `account_key` — autenticação por chave compartilhada
        3. `account_name` + `sas_token` — Shared Access Signature
        4. `account_name` sem credenciais — tenta DefaultAzureCredential (Azure SDK)

    Parâmetros
    ----------
    uri : str
        URI do objeto: "az://container-name/path/to/file.csv"
    connection_string : str | None
        Connection string completa. Suporta {{ env('VAR') }}.
        Exemplo: "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;"
    account_name : str | None
        Nome da storage account. Suporta {{ env('VAR') }}.
    account_key : str | None
        Chave de acesso da storage account. Suporta {{ env('VAR') }}.
    sas_token : str | None
        Token SAS para acesso restrito. Suporta {{ env('VAR') }}.

    Exemplo
    -------
    >>> from dfg.sources.cloud import AzureBlobSource
    >>>
    >>> # Via connection string (recomendado para desenvolvimento)
    >>> source = AzureBlobSource(
    ...     uri="az://meu-container/dados/clientes.csv",
    ...     connection_string="{{ env('AZURE_STORAGE_CONNECTION_STRING') }}",
    ... )
    >>>
    >>> # Via account name + key
    >>> source = AzureBlobSource(
    ...     uri="az://meu-container/dados/",
    ...     account_name="{{ env('AZURE_STORAGE_ACCOUNT') }}",
    ...     account_key="{{ env('AZURE_STORAGE_KEY') }}",
    ... )
    """

    def __init__(
        self,
        uri: str,
        connection_string: str | None = None,
        account_name: str | None = None,
        account_key: str | None = None,
        sas_token: str | None = None,
        format: str | None = None,  # noqa: A002
        extract_path: str | None = None,
        encoding: str = "utf-8-sig",
        delimiter: str = ",",
        max_retries: int = 3,
        retry_delay: float = 1.0,
        retry_backoff: float = 2.0,
    ):
        super().__init__(
            uri=uri,
            format=format,
            extract_path=extract_path,
            encoding=encoding,
            delimiter=delimiter,
            max_retries=max_retries,
            retry_delay=retry_delay,
            retry_backoff=retry_backoff,
        )
        self._connection_string = resolve(connection_string) if connection_string else None
        self._account_name = resolve(account_name) if account_name else None
        self._account_key = resolve(account_key) if account_key else None
        self._sas_token = resolve(sas_token) if sas_token else None

    def _get_client(self):
        """Cria e armazena em cache o cliente BlobServiceClient do Azure SDK."""
        if self._client is not None:
            return self._client

        try:
            from azure.storage.blob import BlobServiceClient  # noqa: PLC0415
        except ImportError as e:
            raise ImportError(
                "AzureBlobSource requer 'azure-storage-blob'. "
                "Instale com: pip install azure-storage-blob"
            ) from e

        if self._connection_string:
            self._client = BlobServiceClient.from_connection_string(self._connection_string)

        elif self._account_name and self._account_key:
            account_url = f"https://{self._account_name}.blob.core.windows.net"
            self._client = BlobServiceClient(
                account_url=account_url,
                credential=self._account_key,
            )

        elif self._account_name and self._sas_token:
            token = self._sas_token.lstrip("?")
            account_url = f"https://{self._account_name}.blob.core.windows.net?{token}"
            self._client = BlobServiceClient(account_url=account_url)

        elif self._account_name:
            # DefaultAzureCredential: Managed Identity, environment, CLI, etc.
            try:
                from azure.identity import DefaultAzureCredential  # noqa: PLC0415
            except ImportError as e:
                raise ImportError(
                    "AzureBlobSource com DefaultAzureCredential requer 'azure-identity'. "
                    "Instale com: pip install azure-identity"
                ) from e
            account_url = f"https://{self._account_name}.blob.core.windows.net"
            self._client = BlobServiceClient(
                account_url=account_url,
                credential=DefaultAzureCredential(),
            )

        else:
            raise ValueError(
                "AzureBlobSource requer ao menos um método de autenticação: "
                "connection_string, account_name+account_key, account_name+sas_token "
                "ou account_name (para DefaultAzureCredential)."
            )

        return self._client

    def _download_object(self, bucket: str, key: str) -> bytes:
        """
        Download de um blob Azure como bytes.

        No Azure, `bucket` é o nome do container e `key` é o nome do blob.
        """
        client = self._get_client()
        try:
            blob_client = client.get_blob_client(container=bucket, blob=key)
            return blob_client.download_blob().readall()
        except Exception as e:
            error_str = str(e).lower()
            if "blobnotfound" in error_str or "404" in error_str:
                raise FileNotFoundError(
                    f"AzureBlobSource: blob não encontrado: 'az://{bucket}/{key}'"
                ) from e
            raise RuntimeError(
                f"AzureBlobSource: falha ao baixar 'az://{bucket}/{key}': {e}"
            ) from e

    def _list_objects(self, bucket: str, prefix: str) -> list[str]:
        """
        Lista todos os blobs no container com o prefixo dado.

        Usa list_blobs() que já pagina automaticamente pelo SDK do Azure.
        """
        client = self._get_client()
        try:
            container_client = client.get_container_client(bucket)
            return [
                blob.name
                for blob in container_client.list_blobs(name_starts_with=prefix or None)
                if not blob.name.endswith("/")
            ]
        except Exception as e:
            raise RuntimeError(
                f"AzureBlobSource: falha ao listar blobs em 'az://{bucket}/{prefix}': {e}"
            ) from e


# =============================================================================
# Helper interno: parser de conteúdo em memória (usado em fetch_many)
# =============================================================================


class _InMemoryFileParser(FileSource):
    """
    Adaptador interno para parsear conteúdo bytes já em memória.

    Usado em fetch_many() para reutilizar os parsers de FileSource
    sem precisar escrever o conteúdo em disco. Sobrescreve _read_content()
    para retornar os bytes diretamente.
    """

    def __init__(
        self,
        content: bytes,
        file_format: str,
        extract_path: str | None,
        encoding: str,
        delimiter: str,
    ):
        # Inicializa com um path fictício para satisfazer o construtor de FileSource.
        # O _read_content() sobrescrito abaixo nunca usa esse path.
        super().__init__(
            path=f"__memory__.{file_format}",
            format=file_format,
            extract_path=extract_path,
            encoding=encoding,
            delimiter=delimiter,
        )
        self._content = content

    def _read_content(self) -> bytes:
        return self._content

    def parse(self) -> list[dict]:
        """Parseia o conteúdo em memória e retorna os registros."""
        return self._parse(self._content)
