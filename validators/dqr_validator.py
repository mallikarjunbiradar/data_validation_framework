# validators/dqr_validator.py
import pandas as pd
from utils.ai_reporter import get_ai_quality_risk
from validators.base_validator import BaseValidator
from rich.console import Console

console = Console()

class DQRValidator(BaseValidator):
    def __init__(self, config_path: str):
        super().__init__(config_path)

    def _numeric_outlier_ratio(self, series: pd.Series) -> float:
        numeric = pd.to_numeric(series, errors="coerce").dropna().astype("float64")
        if len(numeric) < 4:
            return 0.0
        q1 = numeric.quantile(0.25)
        q3 = numeric.quantile(0.75)
        iqr = q3 - q1
        if iqr == 0:
            return 0.0
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        outliers = ((numeric < lower) | (numeric > upper)).mean()
        return float(outliers)

    def _quality_risk_score(self, null_ratio: float, outlier_ratio: float, distinct_ratio: float) -> int:
        # A lightweight AI-style risk model: higher score means worse data quality.
        score = (null_ratio * 60.0) + (outlier_ratio * 30.0) + ((1.0 - distinct_ratio) * 10.0)
        return int(max(0, min(100, round(score))))

    def _risk_label(self, risk_score: int) -> str:
        if risk_score >= 70:
            return "high"
        if risk_score >= 40:
            return "medium"
        return "low"

    def profile(self, file_path: str, ai_assist: bool = True):
        df = pd.read_csv(file_path)
        rows = len(df)
        summary = {"Data_File_Path": file_path, "rows": rows, "columns": {}}

        for col in df.columns:
            ser = df[col]
            null_ratio = float(ser.isna().mean())
            distinct_count = int(ser.nunique(dropna=True))
            distinct_ratio = float(distinct_count / rows) if rows else 0.0

            col_summary = {
                "DataType": str(ser.dtype),
                "Total Null Values": int(ser.isna().sum()),
                "% of Null Values": null_ratio,
                "Distinct Values Count": distinct_count,
            }

            if ai_assist:
                outlier_ratio = self._numeric_outlier_ratio(ser)
                risk_score = self._quality_risk_score(null_ratio, outlier_ratio, distinct_ratio)
                risk_label = self._risk_label(risk_score)
                ai_risk = get_ai_quality_risk(
                    column_name=col,
                    null_ratio=null_ratio,
                    outlier_ratio=outlier_ratio,
                    distinct_ratio=distinct_ratio,
                    dtype=str(ser.dtype),
                )
                if ai_risk:
                    risk_score = ai_risk["risk_score"]
                    risk_label = ai_risk["risk_label"]
                col_summary["AI Insights"] = {
                    "Outlier Ratio": outlier_ratio,
                    "Distinct Ratio": distinct_ratio,
                    "Quality Risk Score": risk_score,
                    "Risk Label": risk_label,
                }
                if ai_risk and ai_risk.get("reason"):
                    col_summary["AI Insights"]["AI Reason"] = ai_risk["reason"]

            summary["columns"][col] = col_summary

        console.print(f"[bold green]Profiling Results for {file_path} [/bold green]")
        if ai_assist:
            console.print("[bold yellow]AI-assisted profiling enabled[/bold yellow]")
        return summary

