import json
import os
from typing import Any, Dict, List, Optional

_LAST_AI_STATUS: Optional[str] = None


def _set_ai_status(status: str) -> None:
    global _LAST_AI_STATUS
    _LAST_AI_STATUS = status


def get_last_ai_status() -> Optional[str]:
    return _LAST_AI_STATUS


def reset_ai_status() -> None:
    global _LAST_AI_STATUS
    _LAST_AI_STATUS = None


def _request_timeout_seconds() -> float:
    raw = os.getenv("OPENAI_TIMEOUT_SECONDS", "15")
    try:
        return max(1.0, float(raw))
    except ValueError:
        return 15.0


def _get_openai_client():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    try:
        from openai import OpenAI
    except ImportError:
        return None
    return OpenAI(api_key=api_key)


def _safe_json_loads(text: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(text)
    except Exception:
        return None


def generate_ai_report_summary(
    action: str,
    result: Dict[str, Any],
    model: Optional[str] = None,
) -> Optional[str]:
    client = _get_openai_client()
    if client is None:
        _set_ai_status("failed_api")
        return None

    selected_model = model or os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
    payload = json.dumps(result, default=str)
    prompt = (
        "You are a data quality analyst. Provide a concise summary in 4-6 bullet points. "
        "Highlight key risks, anomalies, and the top recommended next checks.\n\n"
        f"Validation action: {action}\n"
        f"Validation result JSON: {payload}"
    )

    try:
        response = client.responses.create(
            model=selected_model,
            input=prompt,
            timeout=_request_timeout_seconds(),
        )
        summary = getattr(response, "output_text", None)
        _set_ai_status("generated")
        return summary.strip() if summary else None
    except Exception:
        # Keep report generation non-blocking if API calls fail (e.g., quota/rate-limit).
        _set_ai_status("failed_api")
        return None


def get_ai_schema_suggestions(
    missing_columns: List[str],
    actual_columns: List[str],
    model: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    client = _get_openai_client()
    if client is None or not missing_columns:
        _set_ai_status("failed_api")
        return {}

    selected_model = model or os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
    prompt = (
        "You are helping map expected schema columns to available columns.\n"
        "Return strict JSON object only with this exact shape:\n"
        '{"suggestions": {"<missing_col>": {"suggested_column": "<actual_col>", "confidence": 0.0}}}\n'
        "Only include suggestions where confidence >= 0.6 and suggested column exists in actual columns.\n"
        f"Missing columns: {missing_columns}\n"
        f"Actual columns: {actual_columns}\n"
    )
    try:
        response = client.responses.create(
            model=selected_model,
            input=prompt,
            timeout=_request_timeout_seconds(),
        )
        text = getattr(response, "output_text", "") or ""
        data = _safe_json_loads(text)
        if not data or not isinstance(data.get("suggestions"), dict):
            _set_ai_status("failed_parse")
            return {}
        suggestions = data["suggestions"]
        valid_actual = set(actual_columns)
        cleaned: Dict[str, Dict[str, Any]] = {}
        for key, value in suggestions.items():
            if not isinstance(value, dict):
                continue
            suggested = value.get("suggested_column")
            confidence = value.get("confidence")
            if suggested in valid_actual and isinstance(confidence, (int, float)):
                cleaned[key] = {"suggested_column": suggested, "confidence": round(float(confidence), 2)}
        _set_ai_status("generated")
        return cleaned
    except Exception:
        _set_ai_status("failed_api")
        return {}


def get_ai_quality_risk(
    column_name: str,
    null_ratio: float,
    outlier_ratio: float,
    distinct_ratio: float,
    dtype: str,
    model: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    client = _get_openai_client()
    if client is None:
        _set_ai_status("failed_api")
        return None

    selected_model = model or os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
    prompt = (
        "Assess data quality risk for one column. Return strict JSON only:\n"
        '{"risk_score": 0, "risk_label": "low|medium|high", "reason": "short reason"}\n'
        "risk_score must be integer 0-100.\n"
        f"Column: {column_name}\n"
        f"dtype: {dtype}\n"
        f"null_ratio: {null_ratio}\n"
        f"outlier_ratio: {outlier_ratio}\n"
        f"distinct_ratio: {distinct_ratio}\n"
    )
    try:
        response = client.responses.create(
            model=selected_model,
            input=prompt,
            timeout=_request_timeout_seconds(),
        )
        text = getattr(response, "output_text", "") or ""
        data = _safe_json_loads(text)
        if not data:
            _set_ai_status("failed_parse")
            return None
        score = int(max(0, min(100, int(data.get("risk_score", 0)))))
        label = str(data.get("risk_label", "low")).lower()
        if label not in {"low", "medium", "high"}:
            label = "low"
        reason = str(data.get("reason", "")).strip()
        _set_ai_status("generated")
        return {"risk_score": score, "risk_label": label, "reason": reason}
    except Exception:
        _set_ai_status("failed_api")
        return None


def get_ai_reconciliation_insights(
    dataset_name: str,
    only_in_source_count: int,
    only_in_sink_count: int,
    mismatch_count: int,
    key_columns: List[str],
    model: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    client = _get_openai_client()
    if client is None:
        _set_ai_status("failed_api")
        return None

    selected_model = model or os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
    prompt = (
        "Assess reconciliation outcome and return strict JSON only with this shape:\n"
        '{"risk_label":"low|medium|high","summary":"one short sentence","next_steps":["step1","step2"]}\n'
        f"dataset_name: {dataset_name}\n"
        f"key_columns: {key_columns}\n"
        f"only_in_source_count: {only_in_source_count}\n"
        f"only_in_sink_count: {only_in_sink_count}\n"
        f"mismatch_count: {mismatch_count}\n"
    )
    try:
        response = client.responses.create(
            model=selected_model,
            input=prompt,
            timeout=_request_timeout_seconds(),
        )
        text = getattr(response, "output_text", "") or ""
        data = _safe_json_loads(text)
        if not data:
            _set_ai_status("failed_parse")
            return None
        label = str(data.get("risk_label", "low")).lower()
        if label not in {"low", "medium", "high"}:
            label = "low"
        summary = str(data.get("summary", "")).strip()
        next_steps = data.get("next_steps", [])
        if not isinstance(next_steps, list):
            next_steps = []
        next_steps = [str(step).strip() for step in next_steps if str(step).strip()]
        _set_ai_status("generated")
        return {"risk_label": label, "summary": summary, "next_steps": next_steps}
    except Exception:
        _set_ai_status("failed_api")
        return None


def get_ai_ge_insights(
    dataset_name: str,
    success_percent: float,
    evaluated_expectations: int,
    failed_expectations: int,
    model: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    client = _get_openai_client()
    if client is None:
        _set_ai_status("failed_api")
        return None

    selected_model = model or os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
    prompt = (
        "Assess Great Expectations validation outcome and return strict JSON only:\n"
        '{"risk_label":"low|medium|high","summary":"one short sentence","next_steps":["step1","step2"]}\n'
        f"dataset_name: {dataset_name}\n"
        f"success_percent: {success_percent}\n"
        f"evaluated_expectations: {evaluated_expectations}\n"
        f"failed_expectations: {failed_expectations}\n"
    )
    try:
        response = client.responses.create(
            model=selected_model,
            input=prompt,
            timeout=_request_timeout_seconds(),
        )
        text = getattr(response, "output_text", "") or ""
        data = _safe_json_loads(text)
        if not data:
            _set_ai_status("failed_parse")
            return None
        label = str(data.get("risk_label", "low")).lower()
        if label not in {"low", "medium", "high"}:
            label = "low"
        summary = str(data.get("summary", "")).strip()
        next_steps = data.get("next_steps", [])
        if not isinstance(next_steps, list):
            next_steps = []
        next_steps = [str(step).strip() for step in next_steps if str(step).strip()]
        _set_ai_status("generated")
        return {"risk_label": label, "summary": summary, "next_steps": next_steps}
    except Exception:
        _set_ai_status("failed_api")
        return None
