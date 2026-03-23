# src/dfg/state.py
import json
import os
from dfg.logging import logger

class StateManager:
    def __init__(self, project_dir):
        self.state_path = os.path.join(project_dir, ".dfg_state.json")
        self.state = self._load()

    def _load(self):
        if os.path.exists(self.state_path):
            with open(self.state_path, "r") as f:
                return json.load(f)
        return {}

    def get(self, model_name, default=None):
        return self.state.get(model_name, default)

    def set(self, model_name, value):
        self.state[model_name] = value
        self._save()

    def _save(self):
        with open(self.state_path, "w") as f:
            json.dump(self.state, f, indent=4)