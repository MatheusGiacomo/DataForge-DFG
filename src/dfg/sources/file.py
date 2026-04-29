# src/dfg/sources/file.py
"""
FileSource — Conector de Arquivos para Sources do DataForge.

Suporta ingestão de arquivos locais e remotos (via URL HTTP/HTTPS) nos
formatos mais comuns em pipelines de dados:

    CSV      — stdlib csv.DictReader, suporte a BOM (Excel), delimitadores customizados
    JSON     — lista de objetos ou objeto com lista aninhada (via extract_path)
    JSONL    — JSON Lines / NDJSON, um objeto por linha
    Parquet  — via pyarrow (importação lazy, só se necessário)

Acesso a arquivos:
    Local  : FileSource(path="/dados/arquivo.csv")
    Remoto : FileSource(path="https://exemplo.com/dados.csv")
    S3/GCS : FileSource(path="https://storage.googleapis.com/bucket/arquivo.csv",
                        headers={"Authorization": "Bearer ..."})

Uso:
    from dfg.sources import FileSource

    source = FileSource(path="/dados/clientes.csv")

    def model(context):
        return source.fetch()

Com opções:
    source = FileSource(
        path="https://api.exemplo.com/export/produtos.json",
        format="json",
        extract_path="data.products",
        encoding="utf-8",
    )
"""
import csv
import io
import json
import os
import urllib.request

from dfg.logging import logger
from dfg.sources._env import resolve
from dfg.sources.base import BaseSource


class FileSource(BaseSource):
    """
    Conector para ingestão de arquivos CSV, JSON, JSONL e Parquet.

    O formato é detectado automaticamente pela extensão do arquivo quando
    `format` não é especificado.

    Parâmetros
    ----------
    path : str
        Caminho local ou URL remota do arquivo. Suporta {{ env('VAR') }}.
    format : str | None
        Formato explícito: "csv", "json", "jsonl" ou "parquet".
        None = detecção automática pela extensão.
    extract_path : str | None
        Caminho em notação de ponto para extrair registros de JSONs aninhados
        (ex: "data.items"). Ignorado para CSV, JSONL e Parquet.
    encoding : str
        Encoding do arquivo de texto (padrão: "utf-8-sig" para suporte a BOM).
    delimiter : str
        Delimitador de campos para arquivos CSV (padrão: ",").
    headers : dict | None
        Headers HTTP adicionais para downloads remotos.
    timeout : int
        Timeout em segundos para downloads remotos (padrão: 60).
    max_retries : int
        Número máximo de tentativas em caso de falha (padrão: 3).
    """

    # Mapeamento de extensões para formatos
    _FORMAT_MAP: dict[str, str] = {
        ".csv": "csv",
        ".tsv": "csv",
        ".json": "json",
        ".jsonl": "jsonl",
        ".ndjson": "jsonl",
        ".parquet": "parquet",
    }

    def __init__(
        self,
        path: str,
        format: str | None = None,  # noqa: A002
        extract_path: str | None = None,
        encoding: str = "utf-8-sig",
        delimiter: str = ",",
        headers: dict | None = None,
        timeout: int = 60,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        retry_backoff: float = 2.0,
    ):
        super().__init__(
            max_retries=max_retries,
            retry_delay=retry_delay,
            retry_backoff=retry_backoff,
        )
        self.path = resolve(path)
        self.extract_path = extract_path
        self.encoding = encoding
        self.delimiter = delimiter
        self.headers = resolve(headers) if headers else {}
        self.timeout = timeout

        # Resolve o formato: explícito > extensão do arquivo
        self._format = self._resolve_format(format)

    # ------------------------------------------------------------------
    # API Pública
    # ------------------------------------------------------------------

    def fetch(self) -> list[dict]:
        """
        Lê o arquivo e retorna os registros como lista de dicionários.

        Detecta automaticamente se o arquivo é local ou remoto e delega
        ao parser correto conforme o formato.
        """
        logger.info(f"FileSource: lendo '{self.path}' como '{self._format}'...")

        raw_content = self._execute_with_retry(self._read_content)
        records = self._parse(raw_content)

        logger.info(f"FileSource: {len(records)} registro(s) lido(s) de '{self.path}'.")
        return records

    # ------------------------------------------------------------------
    # Leitura de Conteúdo (local vs remoto)
    # ------------------------------------------------------------------

    def _read_content(self) -> bytes:
        """Lê o conteúdo do arquivo (local ou remoto) como bytes."""
        if self._is_remote(self.path):
            return self._download(self.path)
        return self._read_local(self.path)

    @staticmethod
    def _is_remote(path: str) -> bool:
        return path.startswith("http://") or path.startswith("https://")

    def _download(self, url: str) -> bytes:
        """Faz o download de um arquivo remoto via HTTP/HTTPS."""
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "DataForge-DFG/0.2.0", **self.headers},
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return resp.read()
        except Exception as e:
            raise RuntimeError(f"FileSource: falha ao baixar '{url}': {e}") from e

    @staticmethod
    def _read_local(path: str) -> bytes:
        """Lê um arquivo local como bytes."""
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"FileSource: arquivo não encontrado: '{path}'"
            )
        with open(path, "rb") as f:
            return f.read()

    # ------------------------------------------------------------------
    # Parsers por Formato
    # ------------------------------------------------------------------

    def _parse(self, content: bytes) -> list[dict]:
        """Delega ao parser correto baseado no formato detectado."""
        parsers = {
            "csv": self._parse_csv,
            "json": self._parse_json,
            "jsonl": self._parse_jsonl,
            "parquet": self._parse_parquet,
        }
        parser = parsers.get(self._format)
        if not parser:
            raise ValueError(
                f"FileSource: formato '{self._format}' não suportado. "
                f"Formatos disponíveis: {', '.join(parsers.keys())}."
            )
        return parser(content)

    def _parse_csv(self, content: bytes) -> list[dict]:
        """
        Parseia um arquivo CSV em lista de dicionários.

        Usa csv.DictReader para lidar com headers automaticamente.
        O encoding utf-8-sig remove o BOM de arquivos exportados pelo Excel.
        """
        text = content.decode(self.encoding)
        reader = csv.DictReader(io.StringIO(text), delimiter=self.delimiter)
        return [
            {k.strip(): self._infer_csv_type(v) for k, v in row.items() if k}
            for row in reader
        ]

    def _parse_json(self, content: bytes) -> list[dict]:
        """
        Parseia um arquivo JSON.

        Suporta:
        - Lista no topo nível: [{"id": 1}, ...]
        - Objeto com lista aninhada: {"data": {"items": [...]}} via extract_path
        """
        text = content.decode(self.encoding)
        data = json.loads(text)
        return self._extract_path(data, self.extract_path)

    def _parse_jsonl(self, content: bytes) -> list[dict]:
        """
        Parseia um arquivo JSON Lines (NDJSON): um objeto JSON por linha.

        Linhas em branco e comentários (começando com #) são ignorados.
        """
        text = content.decode(self.encoding)
        records = []
        for line_num, line in enumerate(text.splitlines(), start=1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    records.append(obj)
                else:
                    logger.warn(
                        f"FileSource (JSONL): linha {line_num} não é um objeto. Ignorada."
                    )
            except json.JSONDecodeError as e:
                logger.warn(f"FileSource (JSONL): erro ao parsear linha {line_num}: {e}. Ignorada.")
        return records

    def _parse_parquet(self, content: bytes) -> list[dict]:
        """Parseia um arquivo Parquet usando pyarrow (importação lazy)."""
        try:
            import pyarrow.parquet as pq  # noqa: PLC0415
        except ImportError as e:
            raise ImportError(
                "FileSource (Parquet): 'pyarrow' não está instalado. "
                "Instale-o com: pip install pyarrow"
            ) from e

        table = pq.read_table(io.BytesIO(content))
        return table.to_pylist()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_format(self, explicit_format: str | None) -> str:
        """Resolve o formato a partir do argumento explícito ou da extensão do arquivo."""
        if explicit_format:
            return explicit_format.lower()

        _, ext = os.path.splitext(self.path.split("?")[0].lower())
        detected = self._FORMAT_MAP.get(ext)

        if not detected:
            raise ValueError(
                f"FileSource: não foi possível detectar o formato do arquivo '{self.path}'. "
                f"Especifique explicitamente com format='csv' (ou 'json', 'jsonl', 'parquet')."
            )
        return detected

    @staticmethod
    def _infer_csv_type(value: str | None) -> int | float | str | None:
        """
        Converte valores de string CSV para tipos Python nativos.

        Ordem: int → float → str. Valores vazios retornam None.
        """
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return int(stripped)
        except ValueError:
            pass
        try:
            return float(stripped)
        except ValueError:
            pass
        return stripped
