# src/dfg/compile.py
import sys
import os
from dfg.logger import logger
from dfg.engine import DFGEngine

def compile_command(args):
    current_dir = os.getcwd()
    
    try:
        engine = DFGEngine(project_dir=current_dir)
        engine.compile()
    except Exception as e:
        logger.error(f"Falha ao compilar o projeto: {e}")
        sys.exit(1)