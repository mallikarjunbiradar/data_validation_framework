import argparse
import json
import re
from datetime import datetime
from pathlib import Path

from rich.console import Console
from utils.ai_reporter import generate_ai_report_summary
from validators.GE_validator import GEValidator
from validators.dqr_validator import DQRValidator
from validators.reconciliation_validator import ReconciliationValidator
from validators.schema_validator import SchemaValidator


def _to_records_preview(df_like, limit: int = 10):
    if hasattr(df_like, "head") and hasattr(df_like, "to_dict"):
        return df_like.head(limit).to_dict(orient="records")
    return []


def _prepare_result_for_report(action: str, result: dict) -> dict:
    if action != "reconcile":
        return result

    only_in_source = result.get("only_in_source")
    only_in_sink = result.get("only_in_sink")
    mismatches = result.get("mismatches", [])

    presentable = {
        "summary": {
            "only_in_source_count": len(only_in_source) if hasattr(only_in_source, "__len__") else 0,
            "only_in_sink_count": len(only_in_sink) if hasattr(only_in_sink, "__len__") else 0,
            "mismatch_count": len(mismatches) if isinstance(mismatches, list) else 0,
        },
        "samples": {
            "only_in_source": _to_records_preview(only_in_source),
            "only_in_sink": _to_records_preview(only_in_sink),
            "mismatches": mismatches[:10] if isinstance(mismatches, list) else [],
        },
    }
    if "ai_insights" in result:
        presentable["ai_insights"] = result["ai_insights"]
    return presentable


def _write_report(
    report_file: str,
    action: str,
    result: dict,
    console: Console,
    ai_summary: str | None = None,
) -> None:
    report_path = Path(report_file)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"action": action, "result": _prepare_result_for_report(action, result)}
    if ai_summary:
        payload["ai_summary"] = ai_summary
    report_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    console.print(f"[bold green]Report saved to {report_path}[/bold green]")


def _safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]+", "_", value).strip("_") or "unknown"


def _build_default_report_file(action: str, dataset_hint: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{_safe_name(action)}_{_safe_name(dataset_hint)}_{timestamp}.json"
    return str(Path("reports") / filename)


def main():
    parser = argparse.ArgumentParser("data-validator")
    parser.add_argument("--config", "-c", default="config/rules.yaml")
    parser.add_argument("--file_path", "-f", default="data/sample_source.csv")
    parser.add_argument("action", choices=["profile", "ge_validate", "schema_check", "reconcile"])
    parser.add_argument("--dataset", "-d", required=False)
    parser.add_argument("--report_file", "-r", required=False, help="Path to save JSON report")
    parser.add_argument("--ai_report", action="store_true", help="Add AI summary to report")
    parser.add_argument("--ai_model", required=False, help="Model name for AI summary generation")
    parser.add_argument("--no_ai", action="store_true", help="Disable all AI-assisted analysis")
    args = parser.parse_args()

    console = Console()
    res = None
    ai_enabled = not args.no_ai

    if args.action == "profile":
        v = DQRValidator(args.config)
        res = v.profile(args.file_path, ai_assist=ai_enabled)
        for key, value in res.items():
            if key == "columns":
                for col_key, col_value in value.items():
                    console.print(f"[blue]{col_key}: {col_value}[/blue]")
            else:
                console.print(f"[bold blue]{key}: {value}[/bold blue]")
        console.print(f"[bold green]Profile completed for {args.file_path}[/bold green]")

    elif args.action == "schema_check":
        v = SchemaValidator(args.config)
        res = v.run_schema_checks(args.dataset, ai_assist=ai_enabled)
        console.print(f"[bold yellow]Source columns: {res['actual_columns']}[/bold yellow]")
        console.print(f"[bold yellow]Destination columns: {res['expected_columns']}[/bold yellow]")
        console.print(f"[bold yellow]Missing columns: {res['missing_columns']}[/bold yellow]")
        console.print(f"[bold yellow]Extra columns: {res['extra_columns']}[/bold yellow]")
        if "ai_suggestions" in res:
            console.print(f"[bold yellow]AI suggestions: {res['ai_suggestions']}[/bold yellow]")
        console.print(f"[bold green]Schema check completed for {args.dataset}[/bold green]")

    elif args.action == "ge_validate":
        v = GEValidator(args.config)
        res = v.run_suite(args.dataset, ai_assist=ai_enabled)
        console.print(f"[bold green]GE validation completed for {args.dataset}[/bold green]")
        console.print(f"[bold yellow]GE result summary keys: {list(res.keys())}[/bold yellow]")

    elif args.action == "reconcile":
        v = ReconciliationValidator(args.config)
        res = v.reconcile(args.dataset, ai_assist=ai_enabled)
        console.print(f"[bold yellow]Only in source count: {len(res['only_in_source'])}[/bold yellow]")
        console.print(f"[bold yellow]Only in sink count: {len(res['only_in_sink'])}[/bold yellow]")
        console.print(f"[bold yellow]Mismatch count: {len(res['mismatches'])}[/bold yellow]")
        console.print("[bold cyan]Reconciliation samples (up to 5 rows) printed above[/bold cyan]")
        console.print(f"[bold green]Reconciliation completed for {args.dataset}[/bold green]")
    else:
        parser.print_help()

    if res is not None:
        dataset_hint = args.dataset or Path(args.file_path).stem
        report_file = args.report_file or _build_default_report_file(args.action, dataset_hint)
        ai_summary = None
        if args.ai_report and ai_enabled:
            ai_summary = generate_ai_report_summary(args.action, res, args.ai_model)
            if ai_summary:
                console.print("[bold cyan]AI summary generated and included in report[/bold cyan]")
            else:
                console.print(
                    "[bold yellow]AI summary skipped (missing setup or API request failed)[/bold yellow]"
                )
        elif args.ai_report and not ai_enabled:
            console.print("[bold yellow]AI summary skipped (--no_ai enabled)[/bold yellow]")
        _write_report(report_file, args.action, res, console, ai_summary)


if __name__ == "__main__":
    main()
