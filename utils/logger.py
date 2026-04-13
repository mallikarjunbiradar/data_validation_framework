# utils/logger.py
from rich.console import Console
from datetime import datetime

console = Console()

def info(msg: str):
    console.log(f"[{datetime.utcnow().isoformat()}] INFO: {msg}")

def error(msg: str):
    console.log(f"[{datetime.utcnow().isoformat()}] ERROR: {msg}")

def warn(msg: str):
    console.log(f"[{datetime.utcnow().isoformat()}] WARN: {msg}")
