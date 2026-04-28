# src/dfg/logging.py
"""
Sistema de logging centralizado do DataForge.

Projetado para ser um singleton global que funciona tanto antes quanto
depois da inicialização do projeto (setup). Logs de arquivo só são
gerados após logger.setup() ser invocado.
"""
import logging
import os
import sys
import time
from datetime import datetime
from threading import current_thread


class AnsiColors:
    GRAY = "\033[90m"
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    ORANGE = "\033[33m"
    WARN = "\033[93m"
    ERROR = "\033[91m"
    RESET = "\033[0m"


class DFGLogger:
    """
    Logger singleton com suporte a:
    - Saída colorida no terminal
    - Persistência em arquivo (após setup())
    - Cabeçalho de sessão diário (ID único no formato DDMMYYDFG)
    - Thread-safety via o logger padrão do Python
    """

    def __init__(self):
        self._logger = logging.getLogger("dfg")
        self._project_dir: str | None = None
        self.log_path: str | None = None
        self._header_written = False
        self._session_started = False
        self._command_logged = False
        self._is_configured = False

    # ------------------------------------------------------------------
    # Configuração
    # ------------------------------------------------------------------

    def setup(self, project_dir: str) -> None:
        """
        Inicializa o handler de arquivo. Idempotente: chamadas repetidas
        com o mesmo diretório são silenciosamente ignoradas.
        """
        if self._is_configured and self._project_dir == project_dir:
            return

        self._project_dir = project_dir
        logs_dir = os.path.join(project_dir, "logs")
        os.makedirs(logs_dir, exist_ok=True)

        self.log_path = os.path.join(logs_dir, "dfg.log")

        # Remove handlers antigos para evitar duplicação
        for handler in self._logger.handlers[:]:
            handler.close()
            self._logger.removeHandler(handler)

        file_handler = logging.FileHandler(self.log_path, encoding="utf-8")
        file_handler.setFormatter(logging.Formatter("%(message)s"))
        file_handler.setLevel(logging.DEBUG)

        self._logger.addHandler(file_handler)
        self._logger.setLevel(logging.DEBUG)
        self._logger.propagate = False

        # Reinicia flags de sessão ao trocar de projeto
        self._header_written = False
        self._session_started = False
        self._command_logged = False
        self._is_configured = True

    # ------------------------------------------------------------------
    # Controle de Sessão
    # ------------------------------------------------------------------

    def _write_daily_header(self) -> None:
        """Escreve o cabeçalho de sessão no arquivo, apenas uma vez por dia."""
        if not self.log_path:
            return

        today_id = f"{datetime.now().strftime('%d%m%y')}DFG"

        # Verifica se o ID do dia já existe no arquivo
        if os.path.exists(self.log_path):
            with open(self.log_path, encoding="utf-8") as f:
                if f"ID: {today_id}" in f.read():
                    self._header_written = True
                    return

        now = datetime.now()
        separator = "=" * 80
        header = (
            f"\n{separator}\n"
            f"SESSÃO INICIADA EM: {now.strftime('%d/%m/%Y %H:%M:%S')} | ID: {today_id}\n"
            f"{separator}"
        )
        self._logger.info(header)
        # Exibe no terminal também (sem ANSI para o arquivo)
        print(f"{AnsiColors.ORANGE}{header}{AnsiColors.RESET}")
        self._header_written = True

    def _initialize_session(self) -> None:
        """
        Garante que o cabeçalho do dia e o registro do comando atual
        sejam escritos no arquivo e exibidos no terminal uma única vez
        por invocação do CLI.
        """
        # Terminal: exibe o comando atual na primeira mensagem
        if not self._session_started:
            current_command = " ".join(sys.argv)
            print(f"\n{AnsiColors.GRAY}[EXECUÇÃO] Comando: {current_command}{AnsiColors.RESET}")
            self._session_started = True

        # Arquivo: escreve cabeçalho diário e registro do comando
        if self.log_path and not self._command_logged:
            if not self._header_written:
                self._write_daily_header()

            current_command = " ".join(sys.argv)
            self._logger.info(f"\n[EXECUÇÃO] Comando: {current_command}")
            self._command_logged = True

    # ------------------------------------------------------------------
    # Métodos de Log
    # ------------------------------------------------------------------

    def _log(self, level_num: int, level_name: str, color: str, message: str) -> None:
        self._initialize_session()

        timestamp = time.strftime("%H:%M:%S", time.localtime())
        thread_name = current_thread().name

        formatted = (
            f"{AnsiColors.RESET}{timestamp} "
            f"{color}[{level_name}]{AnsiColors.RESET} "
            f"[{thread_name}]: {message}"
        )

        if self.log_path:
            self._logger.log(level_num, formatted)

        print(formatted)

    def debug(self, message: str) -> None:
        self._log(logging.DEBUG, "debug", AnsiColors.GRAY, message)

    def info(self, message: str) -> None:
        self._log(logging.INFO, "info", AnsiColors.BLUE, message)

    def success(self, message: str) -> None:
        self._log(logging.INFO, "ok", AnsiColors.GREEN, message)

    def forge(self, message: str) -> None:
        """Nível visual para operações de materialização (execuções no banco)."""
        self._log(logging.INFO, "invocation", AnsiColors.ORANGE, message)

    def warn(self, message: str) -> None:
        self._log(logging.WARNING, "warning", AnsiColors.WARN, message)

    # Alias para consistência com o módulo padrão do Python
    warning = warn

    def error(self, message: str) -> None:
        self._log(logging.ERROR, "error", AnsiColors.ERROR, message)


# Singleton global: importado por todos os módulos
logger = DFGLogger()