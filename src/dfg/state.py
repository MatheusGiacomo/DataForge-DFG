# src/dfg/state.py
"""
Gerenciador de Estado Incremental do DataForge.

Persiste o estado de cada modelo Python em um arquivo JSON local
(.dfg_state.json), permitindo que execuções incrementais saibam
quais registros já foram processados (ex: último timestamp de ingestion).
"""
import json
import os

from dfg.logging import logger


class StateManager:
    """
    Armazena e recupera o estado de modelos Python de forma persistente.

    O arquivo de estado é armazenado na raiz do projeto como
    ``.dfg_state.json``. Cada chave é o nome de um modelo e o
    valor é qualquer dado serializável (data, cursor, página, …).
    """

    def __init__(self, project_dir: str):
        self.state_path = os.path.join(project_dir, ".dfg_state.json")
        self._state = self._load()

    def _load(self) -> dict:
        """Carrega o estado do arquivo JSON, retornando {} se não existir."""
        if not os.path.exists(self.state_path):
            return {}
        try:
            with open(self.state_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warn(f"Não foi possível carregar o estado anterior: {e}. Iniciando com estado vazio.")
            return {}

    def get(self, model_name: str, default=None):
        """Retorna o estado salvo para o modelo, ou `default` se não houver."""
        return self._state.get(model_name, default)

    def set(self, model_name: str, value) -> None:
        """Atualiza e persiste imediatamente o estado do modelo."""
        self._state[model_name] = value
        self._save()

    def delete(self, model_name: str) -> None:
        """Remove o estado de um modelo específico."""
        if model_name in self._state:
            del self._state[model_name]
            self._save()

    def clear(self) -> None:
        """Limpa todo o estado persisitido."""
        self._state = {}
        self._save()

    def _save(self) -> None:
        """Persiste o estado atual no arquivo JSON."""
        try:
            with open(self.state_path, "w", encoding="utf-8") as f:
                json.dump(self._state, f, indent=4, default=str)
        except OSError as e:
            logger.error(f"Falha ao salvar o estado: {e}")
