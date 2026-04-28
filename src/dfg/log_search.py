# src/dfg/log_search.py
"""
Utilitário de busca de logs do DataForge.

Implementa uma máquina de estados finitos para filtrar o arquivo
dfg.log por ID de sessão (DDMMYYDFG) e opcionalmente por comando
(run, ingest, transform, test, compile, docs).

Uso via CLI:
    dfg log 150426DFG
    dfg log 150426DFG --run
    dfg log 150426DFG --run -d     (exporta para arquivo)
"""
import os
from contextlib import nullcontext

from dfg.logging import logger


class LogSearcher:
    """
    Busca e filtra entradas no arquivo de log diário do DataForge.

    Parâmetros
    ----------
    project_dir : str
        Diretório raiz do projeto (onde fica a pasta logs/).
    """

    def __init__(self, project_dir: str):
        self.project_dir = project_dir
        self.log_path = os.path.join(project_dir, "logs", "dfg.log")

    def search(
        self,
        log_id: str,
        command_filter: str | None = None,
        dump: bool = False,
    ) -> bool:
        """
        Filtra o log pelo ID do dia e opcionalmente por comando.

        Parâmetros
        ----------
        log_id : str
            ID da sessão no formato DDMMYYDFG (ex: '150426DFG').
        command_filter : str | None
            Filtra apenas as entradas do comando informado
            (ex: 'run', 'test'). None = mostra tudo do dia.
        dump : bool
            Se True, exporta o resultado para um arquivo .txt
            no diretório do projeto.

        Retorna
        -------
        bool: True se encontrou registros, False caso contrário.
        """
        if not os.path.exists(self.log_path):
            logger.error(f"Arquivo de log não encontrado: '{self.log_path}'.")
            return False

        dump_path = ""
        if dump:
            clean_id = log_id.replace("DFG", "").strip()
            suffix = f"_{command_filter}" if command_filter else ""
            dump_path = os.path.join(self.project_dir, f"{clean_id}{suffix}.txt")

        # SIM115: usa context manager para o arquivo de saída.
        # nullcontext() é usado quando não há dump, evitando o open() solto.
        dump_ctx = open(dump_path, "w", encoding="utf-8") if dump else nullcontext()  # noqa: SIM115

        try:
            with dump_ctx as out_file:
                logs_found = self._parse_log(log_id, command_filter, out_file)
        except OSError as e:
            logger.error(f"Não foi possível criar o arquivo de exportação: {e}")
            return False

        if not logs_found:
            msg = f"Nenhum registro encontrado para o ID '{log_id}'"
            if command_filter:
                msg += f" com o filtro de comando '{command_filter}'"
            logger.warn(msg + ".")
            if dump and dump_path and os.path.exists(dump_path):
                os.remove(dump_path)
            return False

        if dump and dump_path:
            logger.success(f"Log exportado para: '{dump_path}'.")

        return True

    def _parse_log(
        self,
        log_id: str,
        command_filter: str | None,
        out_file,
    ) -> bool:
        """
        Máquina de estados finitos para parsing do arquivo de log.

        Retorna True se encontrou algum registro para o ID/filtro fornecido.
        """
        in_target_day = False
        in_target_cmd = False
        logs_found = False

        try:
            with open(self.log_path, encoding="utf-8") as f:
                for line in f:
                    # Estado 1: Detecta o cabeçalho do dia alvo
                    if "SESSÃO INICIADA EM:" in line and "ID:" in line:
                        in_target_day = log_id in line
                        in_target_cmd = False
                        if in_target_day and not command_filter:
                            self._output(line, out_file)
                            logs_found = True
                        continue

                    if not in_target_day:
                        continue

                    # Estado 2: Detecta blocos de comando
                    if "[EXECUÇÃO] Comando:" in line:
                        if command_filter:
                            in_target_cmd = command_filter in line.split()
                            if in_target_cmd:
                                self._output(line, out_file)
                                logs_found = True
                        else:
                            in_target_cmd = True
                            self._output(line, out_file)
                            logs_found = True
                        continue

                    # Estado 3: Linhas de log dos comandos
                    if not command_filter or in_target_cmd:
                        if command_filter and "=" * 10 in line:
                            continue
                        self._output(line, out_file)
                        logs_found = True

        except OSError as e:
            logger.error(f"Falha ao ler o arquivo de log: {e}")

        return logs_found

    @staticmethod
    def _output(line: str, out_file) -> None:
        """Direciona a linha para o terminal ou para o arquivo de saída."""
        if out_file is not None:
            out_file.write(line)
        else:
            print(line, end="")