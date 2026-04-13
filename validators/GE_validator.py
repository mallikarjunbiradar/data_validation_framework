# validators/GE_validator.py
import pandas as pd
from great_expectations.data_context import FileDataContext, set_context
from great_expectations.core import ExpectationSuite
from great_expectations.expectations.expectation_configuration import ExpectationConfiguration
from utils.ai_reporter import get_ai_ge_insights
from validators.base_validator import BaseValidator
from pathlib import Path
from rich.console import Console
import json


console = Console()

DATA_DOCS_DIR = Path("reports/ge_data_docs")

class GEValidator(BaseValidator):
    def __init__(self, config_path: str, ge_root_dir: str = None):
        super().__init__(config_path)
        self.ge_root = ge_root_dir or "great_expectations"
        # initialize a basic GE project if not exists
        ge_datasource_path = Path(self.ge_root)
        if not ge_datasource_path.exists():
            console.print(f"[yellow]Initializing Great Expectations project at {self.ge_root}[/yellow]")
        # FileDataContext will automatically scaffold the project if it doesn't exist
        self.context = FileDataContext(context_root_dir=self.ge_root)
        # Set the context in the project manager so ExpectationSuite can access it
        set_context(self.context)
        DATA_DOCS_DIR.mkdir(parents=True, exist_ok=True)

    def build_suite_from_config(self, dataset_name: str):
        cfg = self.get_dataset_config(dataset_name)
        if cfg is None:
            raise ValueError(f"Dataset not found in config: {dataset_name}")

        suite_name = f"{dataset_name}_suite"
        expectation_configs = []

        for e in cfg.get("expectations", []):
            if not isinstance(e, dict):
                continue

            expectation_name, params = list(e.items())[0]
            params = params if isinstance(params, dict) else {}

            # Special case: handle column_pairs for compound column expectations
            # Convert column_pairs to column_list (which GE expects)
            if "column_pairs" in params:
                column_pairs = params.pop("column_pairs")
                # column_pairs can be a list of pairs, create one expectation per pair
                if isinstance(column_pairs, list) and len(column_pairs) > 0:
                    # Check if it's a list of lists (multiple pairs) or a single pair
                    if isinstance(column_pairs[0], list):
                        # Multiple pairs: create one expectation per pair
                        for pair in column_pairs:
                            pair_params = params.copy()
                            pair_params["column_list"] = pair
                            console.print(f"[cyan]Adding compound column expectation {expectation_name} with column_list: {pair}[/cyan]")
                            expectation_configs.append(
                                ExpectationConfiguration(
                                    type=expectation_name,
                                    kwargs=pair_params
                                )
                            )
                    else:
                        # Single pair: use directly
                        params["column_list"] = column_pairs
                        console.print(f"[cyan]Adding compound column expectation {expectation_name} with column_list: {column_pairs}[/cyan]")
                        expectation_configs.append(
                            ExpectationConfiguration(
                                type=expectation_name,
                                kwargs=params
                            )
                        )

            # Case 1: user provided a list of columns
            elif "column_list" in params:
                column_list = params.pop("column_list")
                for col in column_list:
                    col_params = params.copy()
                    col_params["column"] = col
                    console.print(f"[cyan]Adding expectation {expectation_name} for column: {col}[/cyan]")
                    expectation_configs.append(
                        ExpectationConfiguration(
                            type=expectation_name,
                            kwargs=col_params
                        )
                    )

            # Case 2: column is present - check if it's a list or single value
            elif "column" in params:
                column_value = params.get("column")
                # If column is a list, create one expectation per column
                if isinstance(column_value, list):
                    for col in column_value:
                        col_params = params.copy()
                        col_params["column"] = col
                        console.print(f"[cyan]Adding expectation {expectation_name} for column: {col}[/cyan]")
                        expectation_configs.append(
                            ExpectationConfiguration(
                                type=expectation_name,
                                kwargs=col_params
                            )
                        )
                else:
                    # Single column value
                    console.print(f"[cyan]Adding expectation {expectation_name} {params}[/cyan]")
                    expectation_configs.append(
                        ExpectationConfiguration(
                            type=expectation_name,
                            kwargs=params
                        )
                    )

            else:
                # Case 3: table-level expectations (no column key)
                console.print(f"[cyan]Adding table-level expectation {expectation_name}[/cyan]")
                expectation_configs.append(
                    ExpectationConfiguration(
                        type=expectation_name,
                        kwargs=params
                    )
                )

        # Create and register suite
        suite = ExpectationSuite(name=suite_name, expectations=expectation_configs)
        self.context.suites.add_or_update(suite)

        return suite_name

    def run_suite(self, dataset_name: str, ai_assist: bool = True):
        cfg = self.get_dataset_config(dataset_name)
        suite_name = self.build_suite_from_config(dataset_name)
        df = pd.read_csv(cfg["path"])

        # Get or create pandas datasource using Fluent API
        datasource_name = "default_pandas"
        try:
            pandas_datasource = self.context.data_sources.get(datasource_name)
        except KeyError:
            # Create a new pandas datasource
            pandas_datasource = self.context.data_sources.add_pandas(name=datasource_name)

        # Check if asset exists and delete it to avoid conflicts
        asset_name = f"{dataset_name}_asset"
        try:
            pandas_datasource.get_asset(asset_name)
            pandas_datasource.delete_asset(asset_name)
        except (LookupError, KeyError):
            # Asset doesn't exist, which is fine
            pass

        # Read dataframe to get a Batch (creates ephemeral asset if needed)
        batch = pandas_datasource.read_dataframe(
            dataframe=df,
            asset_name=asset_name
        )

        # Get the expectation suite and validate
        suite = self.context.suites.get(name=suite_name)
        results = batch.validate(expect=suite)

        # Save Data Docs locally (HTML)
        self.context.build_data_docs()
        # also produce JSON summary
        result_json = results.to_json_dict()
        if ai_assist:
            stats = result_json.get("statistics", {})
            success_percent = float(stats.get("success_percent", 0.0))
            evaluated = int(stats.get("evaluated_expectations", 0))
            successful = int(stats.get("successful_expectations", 0))
            failed = max(0, evaluated - successful)
            ai_insights = get_ai_ge_insights(
                dataset_name=dataset_name,
                success_percent=success_percent,
                evaluated_expectations=evaluated,
                failed_expectations=failed,
            )
            if ai_insights:
                result_json["ai_insights"] = ai_insights
                console.print(f"[cyan]AI GE summary: {ai_insights.get('summary', '')}[/cyan]")

        out_path = Path("reports") / f"{dataset_name}_ge_result_{pd.Timestamp.now().strftime('%Y-%m-%d_%H:%M:%S')}.json"
        out_path.parent.mkdir(exist_ok=True)
        with open(out_path, "w") as f:
            json.dump(result_json, f, indent=2)
        console.print(f"[green]GE validation for {dataset_name} saved to {out_path}[/green]")
        return result_json

