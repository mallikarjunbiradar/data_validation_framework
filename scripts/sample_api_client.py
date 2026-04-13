# sample_api_client.py
# Run API first:
#   uvicorn api:app --reload --host 0.0.0.0 --port 8000

import argparse
import json
import requests

BASE_URL = "http://127.0.0.1:8000"


def post(endpoint: str, payload: dict):
    url = f"{BASE_URL}{endpoint}"
    resp = requests.post(url, json=payload, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    print(f"\n=== {endpoint} ===")
    print(f"Report: {data.get('report_file')}")
    print(f"AI Status: {data.get('ai_status')}")
    print(f"AI Summary: {data.get('ai_summary')}")
    ai_insights = data.get("result", {}).get("ai_insights")
    print(f"AI Insights: {json.dumps(ai_insights, indent=2, default=str) if ai_insights else None}")
    print(json.dumps(data.get("result", {}), indent=2, default=str)[:1200], "...\n")
    return data


def health_check():
    return requests.get(f"{BASE_URL}/health", timeout=30).json()


def run_profile(config="config/rules.yaml", file_path="data/sample_source.csv", no_ai=False, ai_report=False):
    return post(
        "/profile",
        {
            "config": config,
            "file_path": file_path,
            "no_ai": no_ai,
            "ai_report": ai_report,
        },
    )


def run_schema_check(config="config/rules.yaml", dataset="customer_data", no_ai=False, ai_report=False):
    return post(
        "/schema_check",
        {
            "config": config,
            "dataset": dataset,
            "no_ai": no_ai,
            "ai_report": ai_report,
        },
    )


def run_ge_validate(config="config/rules.yaml", dataset="customer_data", no_ai=False, ai_report=False):
    return post(
        "/ge_validate",
        {
            "config": config,
            "dataset": dataset,
            "no_ai": no_ai,
            "ai_report": ai_report,
        },
    )


def run_reconcile(config="config/rules.yaml", dataset="customer_data", no_ai=False, ai_report=False):
    return post(
        "/reconcile",
        {
            "config": config,
            "dataset": dataset,
            "no_ai": no_ai,
            "ai_report": ai_report,
        },
    )


def main():
    parser = argparse.ArgumentParser("sample-api-client")
    parser.add_argument(
        "operation",
        choices=["health", "profile", "schema_check", "ge_validate", "reconcile", "all"],
        help="API operation to call",
    )
    parser.add_argument("--config", default="config/rules.yaml")
    parser.add_argument("--file_path", default="data/sample_source.csv")
    parser.add_argument("--dataset", default="customer_data")
    parser.add_argument("--no_ai", action="store_true")
    parser.add_argument("--ai_report", action="store_true")
    args = parser.parse_args()

    if args.operation == "health":
        print("Health:", health_check())
    elif args.operation == "profile":
        run_profile(
            config=args.config,
            file_path=args.file_path,
            no_ai=args.no_ai,
            ai_report=args.ai_report,
        )
    elif args.operation == "schema_check":
        run_schema_check(
            config=args.config,
            dataset=args.dataset,
            no_ai=args.no_ai,
            ai_report=args.ai_report,
        )
    elif args.operation == "ge_validate":
        run_ge_validate(
            config=args.config,
            dataset=args.dataset,
            no_ai=args.no_ai,
            ai_report=args.ai_report,
        )
    elif args.operation == "reconcile":
        run_reconcile(
            config=args.config,
            dataset=args.dataset,
            no_ai=args.no_ai,
            ai_report=args.ai_report,
        )
    else:
        print("Health:", health_check())
        run_profile(config=args.config, file_path=args.file_path, no_ai=args.no_ai, ai_report=args.ai_report)
        run_schema_check(config=args.config, dataset=args.dataset, no_ai=args.no_ai, ai_report=args.ai_report)
        run_ge_validate(config=args.config, dataset=args.dataset, no_ai=args.no_ai, ai_report=args.ai_report)
        run_reconcile(config=args.config, dataset=args.dataset, no_ai=args.no_ai, ai_report=args.ai_report)


if __name__ == "__main__":
    main()