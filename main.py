from validators.dqr_validator import DQRValidator
from validators.schema_validator import SchemaValidator
from validators.GE_validator import GEValidator
from validators.reconciliation_validator import ReconciliationValidator
from utils.logger import info, error

def run_full_pipeline(dataset_name: str, config_path: str = "config/rules.yaml"):
    info(f"Starting full validation pipeline for {dataset_name}")
    try:
        # 1. Schema check
        sv = SchemaValidator(config_path)
        schema_res = sv.run_schema_checks(dataset_name)
        info(f"Schema check: missing={schema_res['missing_columns']} extra={schema_res['extra_columns']}")

        # 2. Profile / DQR
        dqr = DQRValidator(config_path)
        profile = dqr.profile(dataset_name)
        info(f"DQR rows={profile['rows']}")

        # 3. Great Expectations validations (rules)
        ge = GEValidator(config_path)
        ge_res = ge.run_suite(dataset_name)
        info("GE validation completed")

        # 4. Reconciliation (if configured)
        rec = ReconciliationValidator(config_path)
        rec_res = rec.reconcile(dataset_name)
        info(f"Reconciliation results only_in_source={len(rec_res['only_in_source'])} only_in_sink={len(rec_res['only_in_sink'])}")

        return {"schema": schema_res, "profile": profile, "ge": ge_res, "reconciliation": rec_res}
    except Exception as e:
        error(f"Validation pipeline failed: {e}")
        raise

if __name__ == "__main__":
    import sys
    ds = sys.argv[1] if len(sys.argv) > 1 else "customer_data"
    run_full_pipeline(ds)
