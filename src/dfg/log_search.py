import os
import sys
from dfg.logging import logger

class LogSearcher:
    def __init__(self, project_dir):
        self.project_dir = project_dir
        self.log_path = os.path.join(project_dir, "logs", "dfg.log")

    def search(self, log_id: str, command_filter: str = None, dump: bool = False):
        if not os.path.exists(self.log_path):
            logger.error(f"Arquivo de log não encontrado em: {self.log_path}")
            return False

        # Prepara o arquivo de saída caso a flag -d tenha sido usada
        out_file = None
        dump_path = ""
        if dump:
            # Pega o ID (ex: 220326DFG) e remove a string 'DFG' para nomear o arquivo
            clean_id = log_id.replace("DFG", "").strip()
            dump_path = os.path.join(self.project_dir, f"{clean_id}.txt")
            out_file = open(dump_path, "w", encoding="utf-8")

        # Variáveis de Estado (Máquina de Estados Finitos)
        in_target_day = False
        in_target_cmd = False
        logs_found = False

        try:
            with open(self.log_path, "r", encoding="utf-8") as f:
                for line in f:
                    # 1. Detecta o cabeçalho do dia
                    if "SESSÃO INICIADA EM:" in line and "ID:" in line:
                        if log_id in line:
                            in_target_day = True
                            if not command_filter:
                                self._output_line(line, out_file)
                                logs_found = True
                        else:
                            in_target_day = False
                        continue

                    # Se não estamos no dia alvo, ignora a linha e vai pra próxima
                    if not in_target_day:
                        continue

                    # 2. Detecta um novo bloco de comando
                    if "[EXECUÇÃO] Comando:" in line:
                        if command_filter:
                            # Divide a linha em palavras para evitar falsos positivos
                            # Ex: garante que acha "run" e não algo como "running_test"
                            words = line.split() 
                            if command_filter in words:
                                in_target_cmd = True
                                self._output_line(line, out_file)
                                logs_found = True
                            else:
                                in_target_cmd = False
                        else:
                            # Se não tem filtro de comando, imprime tudo do dia
                            self._output_line(line, out_file)
                            logs_found = True
                        continue

                    # 3. Processa linhas normais (logs dos comandos)
                    if not command_filter or in_target_cmd:
                        # Limpa linhas estéticas do cabeçalho caso esteja filtrando um comando específico
                        if command_filter and "=======" in line:
                            continue
                            
                        self._output_line(line, out_file)
                        logs_found = True

        except Exception as e:
            logger.error(f"Falha ao ler o arquivo de log: {e}")
        finally:
            if out_file:
                out_file.close()

        # Feedback final para o usuário
        if not logs_found:
            msg = f"Nenhum registro encontrado para o ID '{log_id}'"
            if command_filter: msg += f" com o comando '{command_filter}'"
            logger.warn(msg + ".")
            
            # Remove o arquivo vazio se ele foi criado atoa
            if dump and os.path.exists(dump_path):
                os.remove(dump_path)
            return False

        if dump:
            logger.success(f"Log exportado com sucesso para: {dump_path}")
            
        return True

    def _output_line(self, line: str, out_file):
        """Helper para direcionar a linha para o terminal ou para o arquivo."""
        if out_file:
            out_file.write(line)
        else:
            print(line, end="")
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#