import importlib
from typing import Any, Dict, Optional

from pydantic import BaseModel
from rich.console import Console

from cli import _build_default_report_file, _prepare_result_for_report, _write_report
from utils.ai_reporter import generate_ai_report_summary, get_last_ai_status, reset_ai_status
from validators.GE_validator import GEValidator
from validators.dqr_validator import DQRValidator
from validators.reconciliation_validator import ReconciliationValidator
from validators.schema_validator import SchemaValidator

fastapi_module = importlib.import_module("fastapi")
FastAPI = fastapi_module.FastAPI
HTTPException = fastapi_module.HTTPException

app = FastAPI(title="Data Validation Framework API", version="1.0.0")
console = Console()


class BaseRequest(BaseModel):
    config: str = "config/rules.yaml"
    report_file: Optional[str] = None
    ai_report: bool = False
    ai_model: Optional[str] = None
    no_ai: bool = False


class ProfileRequest(BaseRequest):
    file_path: str = "data/sample_source.csv"


class DatasetRequest(BaseRequest):
    dataset: str


def _finalize(
    action: str,
    dataset_hint: str,
    result: Dict[str, Any],
    report_file: Optional[str],
    ai_report: bool,
    no_ai: bool,
    ai_model: Optional[str],
) -> Dict[str, Any]:
    output_report = report_file or _build_default_report_file(action, dataset_hint)
    response_result = _prepare_result_for_report(action, result)
    ai_summary = None
    if ai_report and not no_ai:
        ai_summary = generate_ai_report_summary(action, result, ai_model)
    _write_report(output_report, action, result, console, ai_summary)
    ai_status = get_last_ai_status() or "failed_api"
    return {
        "action": action,
        "report_file": output_report,
        "result": response_result,
        "ai_summary": ai_summary,
        "ai_status": ai_status,
    }


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/profile")
def profile(req: ProfileRequest) -> Dict[str, Any]:
    try:
        reset_ai_status()
        validator = DQRValidator(req.config)
        result = validator.profile(req.file_path, ai_assist=not req.no_ai)
        return _finalize(
            "profile",
            dataset_hint=req.file_path.split("/")[-1].split(".")[0],
            result=result,
            report_file=req.report_file,
            ai_report=req.ai_report,
            no_ai=req.no_ai,
            ai_model=req.ai_model,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/schema_check")
def schema_check(req: DatasetRequest) -> Dict[str, Any]:
    try:
        reset_ai_status()
        validator = SchemaValidator(req.config)
        result = validator.run_schema_checks(req.dataset, ai_assist=not req.no_ai)
        return _finalize(
            "schema_check",
            dataset_hint=req.dataset,
            result=result,
            report_file=req.report_file,
            ai_report=req.ai_report,
            no_ai=req.no_ai,
            ai_model=req.ai_model,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/ge_validate")
def ge_validate(req: DatasetRequest) -> Dict[str, Any]:
    try:
        reset_ai_status()
        validator = GEValidator(req.config)
        result = validator.run_suite(req.dataset, ai_assist=not req.no_ai)
        return _finalize(
            "ge_validate",
            dataset_hint=req.dataset,
            result=result,
            report_file=req.report_file,
            ai_report=req.ai_report,
            no_ai=req.no_ai,
            ai_model=req.ai_model,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/reconcile")
def reconcile(req: DatasetRequest) -> Dict[str, Any]:
    try:
        reset_ai_status()
        validator = ReconciliationValidator(req.config)
        result = validator.reconcile(req.dataset, ai_assist=not req.no_ai)
        return _finalize(
            "reconcile",
            dataset_hint=req.dataset,
            result=result,
            report_file=req.report_file,
            ai_report=req.ai_report,
            no_ai=req.no_ai,
            ai_model=req.ai_model,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
