# utils/file_utils.py
import pandas as pd
import json
from pathlib import Path

def read_csv(path: str, **kwargs):
    return pd.read_csv(path, **kwargs)

def write_csv(df, path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)

def save_json(obj, path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)


