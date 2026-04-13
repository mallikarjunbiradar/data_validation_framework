# validators/schema_validator.py
from difflib import SequenceMatcher, get_close_matches
import pandas as pd
from utils.ai_reporter import get_ai_schema_suggestions
from validators.base_validator import BaseValidator
from rich.console import Console

console = Console()

class SchemaValidator(BaseValidator):
    def __init__(self, config_path: str):
        super().__init__(config_path)

    def _suggest_column_mappings(self, missing_columns, actual_columns):
        """
        AI-model-backed suggestions with fuzzy-match fallback.
        """
        ai_suggestions = get_ai_schema_suggestions(
            missing_columns=sorted(list(missing_columns)),
            actual_columns=sorted(list(actual_columns)),
        )
        if ai_suggestions:
            return ai_suggestions

        suggestions = {}
        actual_list = sorted(list(actual_columns))
        for missing_col in sorted(list(missing_columns)):
            close = get_close_matches(missing_col, actual_list, n=1, cutoff=0.6)
            if close:
                match = close[0]
                confidence = round(SequenceMatcher(None, missing_col, match).ratio(), 2)
                suggestions[missing_col] = {"suggested_column": match, "confidence": confidence}
        return suggestions

    def run_schema_checks(self, dataset_name: str, ai_assist: bool = False):
        cfg = self.get_dataset_config(dataset_name)
        if cfg is None:
            raise ValueError("Dataset not found in config: " + dataset_name)
        path = cfg["path"]
        df = pd.read_csv(path)
        expected_columns = []
        # derive expected columns from expectations that reference columns
        for e in cfg.get("expectations", []):
            if isinstance(e, dict):
                params = list(e.values())[0]
                # Handle both 'column' and 'column_list' fields
                col = params.get("column") or params.get("column_list")
                if col:
                    # Handle both list and string cases
                    if isinstance(col, list):
                        expected_columns.extend(col)
                    else:
                        expected_columns.append(col)
        # print(f"expected_columns: {expected_columns}")
        expected = set(expected_columns)
        actual = set(df.columns.tolist())
        missing = list(expected - actual)
        extra = list(actual - expected)
        ai_suggestions = {}

        if ai_assist and missing:
            ai_suggestions = self._suggest_column_mappings(missing, actual)

        if missing or extra:
            console.print(f"[red]Schema check for {dataset_name} failed[/red]")
            if ai_assist and ai_suggestions:
                console.print("[yellow]AI suggestions for missing columns:[/yellow]")
                for expected_col, suggestion in ai_suggestions.items():
                    console.print(
                        f"  - {expected_col} -> {suggestion['suggested_column']} "
                        f"(confidence={suggestion['confidence']})"
                    )
        else:
            console.print(f"[green]Schema check for {dataset_name} passed[/green]")
        return {
            "dataset": dataset_name,
            "actual_columns": actual,
            "expected_columns": expected,
            "missing_columns": missing,
            "extra_columns": extra,
            "ai_suggestions": ai_suggestions,
        }
