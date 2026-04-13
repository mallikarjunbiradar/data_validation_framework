# validators/reconciliation_validator.py
import pandas as pd
from utils.compare_utils import dataframe_deep_compare
from utils.ai_reporter import get_ai_reconciliation_insights
from validators.base_validator import BaseValidator
from rich.console import Console
from pathlib import Path

console = Console()

class ReconciliationValidator(BaseValidator):
    def __init__(self, config_path: str):
        super().__init__(config_path)

    def reconcile(self, dataset_name: str, ai_assist: bool = True):
        cfg = self.get_dataset_config(dataset_name)
        if cfg is None:
            raise ValueError("Dataset not found: " + dataset_name)
        source_path = Path(cfg["path"])
        sink_path = Path(cfg.get("sink_path") or cfg["path"])
        console.print(f"[blue]Reconciliation for {dataset_name}[/blue]")
        console.print(f"  source_path: {source_path}")
        console.print(f"  sink_path: {sink_path}")
        key_columns = cfg.get("reconciliation", {}).get("key_columns", [])
        console.print(f"  key_columns: {key_columns}")
        df_src = pd.read_csv(source_path)
        df_sink = pd.read_csv(sink_path)

        # Validate that key columns exist in both dataframes
        missing_in_src = [col for col in key_columns if col not in df_src.columns]
        missing_in_sink = [col for col in key_columns if col not in df_sink.columns]

        if missing_in_src:
            raise KeyError(f"Key columns {missing_in_src} not found in source file. Available columns: {list(df_src.columns)}")
        if missing_in_sink:
            raise KeyError(f"Key columns {missing_in_sink} not found in sink file. Available columns: {list(df_sink.columns)}")

        result = dataframe_deep_compare(df_src, df_sink, key_columns)
        if ai_assist:
            ai_insights = get_ai_reconciliation_insights(
                dataset_name=dataset_name,
                only_in_source_count=len(result["only_in_source"]),
                only_in_sink_count=len(result["only_in_sink"]),
                mismatch_count=len(result["mismatches"]),
                key_columns=key_columns,
            )
            if ai_insights:
                result["ai_insights"] = ai_insights
                console.print(f"[cyan]AI reconciliation summary: {ai_insights.get('summary', '')}[/cyan]")
        return result

