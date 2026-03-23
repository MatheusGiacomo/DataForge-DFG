# src/dfg/artifacts.py
import json
import os
from datetime import datetime
from dfg.logging import logger

class ArtifactManager:
    def __init__(self, project_dir: str):
        self.target_dir = os.path.join(project_dir, "target")
        os.makedirs(self.target_dir, exist_ok=True)

    def save_manifest(self, models_registry: dict, dependencies_map: dict):
        """Gera o manifest.json com a topologia do projeto."""
        manifest = {
            "metadata": {
                "generated_at": datetime.utcnow().isoformat() + "Z",
                "tool": "Data Forge (DFG)"
            },
            "nodes": {},
            "dependencies": dependencies_map
        }

        for name, info in models_registry.items():
            manifest["nodes"][name] = {
                "type": info.get("type", "unknown"),
                "materialized": info.get("materialized", "table" if info.get("type") == "sql" else "memory"),
                "depends_on": dependencies_map.get(name, [])
            }

        manifest_path = os.path.join(self.target_dir, "manifest.json")
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=4)
        
        logger.debug(f"Artefato gerado: {manifest_path}")

    def save_run_results(self, command_name: str, results: list):
        """Gera o run_results.json com estatísticas da execução."""
        run_results = {
            "metadata": {
                "generated_at": datetime.utcnow().isoformat() + "Z",
                "command": f"dfg {command_name}"
            },
            "results": results
        }

        results_path = os.path.join(self.target_dir, "run_results.json")
        with open(results_path, "w", encoding="utf-8") as f:
            json.dump(run_results, f, indent=4)
            
        logger.debug(f"Artefato gerado: {results_path}")