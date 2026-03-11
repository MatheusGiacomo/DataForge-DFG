# src/dfg/logger.py
import logging
import sys
from datetime import datetime

class DFGLogger:
    # Cores ANSI
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    RESET = "\033[0m"

    @staticmethod
    def _get_time():
        return datetime.now().strftime("%H:%M:%S")

    def info(self, msg):
        print(f"{self.BLUE}{self._get_time()} [INFO]{self.RESET} {msg}")

    def success(self, msg):
        print(f"{self.GREEN}{self._get_time()} [SUCCESS]{self.RESET} {self.BOLD}{msg}{self.RESET}")

    def warn(self, msg):
        print(f"{self.YELLOW}{self._get_time()} [WARN]{self.RESET} {msg}")

    def error(self, msg):
        print(f"{self.RED}{self._get_time()} [ERROR]{self.RESET} {self.BOLD}{msg}{self.RESET}", file=sys.stderr)

    def forge(self, model_name):
        print(f"{self.BLUE}{self._get_time()} {self.RESET}Forge >> {self.BOLD}{model_name}{self.RESET}...")

# Instância global para facilitar o uso
logger = DFGLogger()