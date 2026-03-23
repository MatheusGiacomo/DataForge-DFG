import logging
import os
import time
import sys
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
    def __init__(self):
        self._logger = logging.getLogger("dfg")
        self._project_dir = None
        self.log_path = None
        self._header_checked = False
        self._session_started = False
        self._command_logged_to_file = False # Nova flag para garantir registro no arquivo
        
        self.BLUE = AnsiColors.BLUE
        self.GREEN = AnsiColors.GREEN
        self.GRAY = AnsiColors.GRAY
        self.RESET = AnsiColors.RESET

    def setup(self, project_dir):
        self._project_dir = project_dir
        logs_dir = os.path.join(self._project_dir, "logs")
        os.makedirs(logs_dir, exist_ok=True)
        
        self.log_path = os.path.join(logs_dir, "dfg.log")
        
        file_handler = logging.FileHandler(self.log_path, encoding="utf-8")
        file_handler.setFormatter(logging.Formatter("%(message)s"))
        file_handler.setLevel(logging.INFO)
        
        self._logger.addHandler(file_handler)
        self._logger.setLevel(logging.DEBUG)

    def _should_print_daily_header(self):
        if not self.log_path or self._header_checked:
            return False
            
        today_id = f"{datetime.now().strftime('%d%m%y')}DFG"
        
        if os.path.exists(self.log_path):
            with open(self.log_path, 'r', encoding='utf-8') as f:
                if f"ID: {today_id}" in f.read():
                    self._header_checked = True
                    return False
        return True

    def _initialize_session(self):
        now = datetime.now()
        current_command = " ".join(sys.argv)

        # 1. TRATAMENTO DO TERMINAL (Sempre acontece na primeira chamada)
        if not self._session_started:
            # Pula linha e mostra o comando no terminal
            print(f"\n{AnsiColors.GRAY}[EXECUÇÃO] Comando: {current_command}{AnsiColors.RESET}")
            self._session_started = True

        # 2. TRATAMENTO DO ARQUIVO DE LOG (Só acontece se o setup já foi feito)
        if self.log_path and not self._command_logged_to_file:
            # Verifica se precisa do ID do dia no arquivo
            if self._should_print_daily_header():
                daily_id = f"{now.strftime('%d%m%y')}DFG"
                header_line = "=" * 80
                header_msg = (
                    f"\n{AnsiColors.ORANGE}{header_line}\n"
                    f"SESSÃO INICIADA EM: {now.strftime('%d/%m/%Y %H:%M:%S')} | ID: {daily_id}\n"
                    f"{header_line}{AnsiColors.RESET}"
                )
                print(header_msg)
                self._logger.info(header_msg)
                self._header_checked = True

            # Registra o comando no arquivo
            command_log = f"\n[EXECUÇÃO] Comando: {current_command}"
            self._logger.info(command_log)
            self._command_logged_to_file = True

    def _log(self, level_num, level_name, color, message):
        self._initialize_session()

        t = time.localtime()
        timestamp = time.strftime("%H:%M:%S", t)
        thread_name = current_thread().name
        
        formatted_message = (
            f"{AnsiColors.RESET}{timestamp} "
            f"{color}[{level_name}]{AnsiColors.RESET} "
            f"[{thread_name}]: {message}"
        )
        
        if self.log_path:
            self._logger.log(level_num, formatted_message)
        
        print(formatted_message)

    def debug(self, message): self._log(logging.DEBUG, "debug", AnsiColors.GRAY, message)
    def info(self, message): self._log(logging.INFO, "info", AnsiColors.BLUE, message)
    def success(self, message): self._log(logging.INFO, "info", AnsiColors.GREEN, message)
    def forge(self, message): self._log(logging.INFO, "invocation", AnsiColors.ORANGE, message)
    def warn(self, message): self._log(logging.WARNING, "warning", AnsiColors.WARN, message)
    def error(self, message): self._log(logging.ERROR, "error", AnsiColors.ERROR, message)

logger = DFGLogger()