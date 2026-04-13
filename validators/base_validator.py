import yaml
from pathlib import Path
from rich.console import Console

console = Console()

class BaseValidator:
    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config not found: {config_path}")
        with open(self.config_path, "r") as f:
            self.config = yaml.safe_load(f)

    def get_dataset_config(self, dataset_name):
        return self.config.get("datasets", {}).get(dataset_name)
