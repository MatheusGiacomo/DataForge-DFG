# src/dfg/artifacts.py
"""
Gerenciador de Artefatos de Observabilidade do DataForge.

Gera dois arquivos JSON na pasta ``target/`` após cada execução:

- ``manifest.json``:  topologia completa do projeto (nodes + dependências).
- ``run_results.json``: estatísticas de execução (status, tempo, linhas).
"""
import json
import os
from datetime import UTC, datetime

from dfg.logging import logger

_TOOL_NAME = "DataForge (DFG)"


class ArtifactManager:
    """
    Gera e persiste os artefatos de metadados e observabilidade.

    Parâmetros
    ----------
    project_dir : str
        Diretório raiz do projeto. Os artefatos são salvos em
        ``{project_dir}/target/``.
    """

    def __init__(self, project_dir: str):
        self.target_dir = os.path.join(project_dir, "target")
        os.makedirs(self.target_dir, exist_ok=True)

    def _utc_now_iso(self) -> str:
        return datetime.now(tz=UTC).isoformat()

    def save_manifest(self, models_registry: dict, dependencies_map: dict) -> None:
        """
        Gera o ``manifest.json`` com a topologia completa do projeto.

        O manifest é consumido pelo comando ``dfg docs`` para gerar
        o grafo de linhagem interativo.
        """
        manifest = {
            "metadata": {
                "generated_at": self._utc_now_iso(),
                "tool": _TOOL_NAME,
            },
            "nodes": {},
            "dependencies": dependencies_map,
        }

        for name, info in models_registry.items():
            model_type = info.get("type", "unknown")
            default_materialized = "table" if model_type == "sql" else "memory"
            manifest["nodes"][name] = {
                "type": model_type,
                "materialized": info.get("config", {}).get("materialized", default_materialized),
                "description": info.get("config", {}).get("description", ""),
                "depends_on": dependencies_map.get(name, []),
            }

        manifest_path = os.path.join(self.target_dir, "manifest.json")
        self._write_json(manifest_path, manifest)
        logger.debug(f"Artefato gerado: {manifest_path}")

    def save_run_results(self, command_name: str, results: list) -> None:
        """
        Gera o ``run_results.json`` com as estatísticas da execução atual.

        Cada entrada em ``results`` deve ser um dicionário com pelo menos
        as chaves ``model``, ``status`` e ``execution_time``.
        """
        # Calcula totais para o sumário
        total = len(results)
        success = sum(1 for r in results if r.get("status") == "success")
        error = sum(1 for r in results if r.get("status") == "error")
        skipped = total - success - error

        run_results = {
            "metadata": {
                "generated_at": self._utc_now_iso(),
                "command": f"dfg {command_name}",
                "tool": _TOOL_NAME,
            },
            "summary": {
                "total": total,
                "success": success,
                "error": error,
                "skipped": skipped,
            },
            "results": results,
        }

        results_path = os.path.join(self.target_dir, "run_results.json")
        self._write_json(results_path, run_results)
        logger.debug(f"Artefato gerado: {results_path}")

    @staticmethod
    def _write_json(path: str, data: dict) -> None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)