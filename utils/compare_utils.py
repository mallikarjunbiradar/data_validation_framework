# utils/compare_utils.py
import pandas as pd
from rich.console import Console

console = Console()

def dataframe_deep_compare(df_src: pd.DataFrame, df_sink: pd.DataFrame, key_columns: list):
    """
    Compare two dataframes in an unordered, duplicate-aware way.
    Returns dict: {only_in_source, only_in_sink, mismatched_rows}
    Each is a DataFrame.
    """
    if not key_columns:
        raise ValueError("key_columns required for reconciliation")

    # Prepare keys as tuple to support duplicates
    def key_series(df):
        return df[key_columns].astype(str).agg("||".join, axis=1)

    src_keys = key_series(df_src)
    sink_keys = key_series(df_sink)

    # Add an _row_id to preserve duplicates
    src = df_src.copy()
    src["_src_key"] = src_keys
    src["_src_row_id"] = src.groupby("_src_key").cumcount()

    sink = df_sink.copy()
    sink["_sink_key"] = sink_keys
    sink["_sink_row_id"] = sink.groupby("_sink_key").cumcount()

    # Merge on key + ordinal to treat duplicates as separate rows when possible
    merged = src.merge(
        sink,
        left_on=("_src_key", "_src_row_id"),
        right_on=("_sink_key", "_sink_row_id"),
        how="outer",
        indicator=True,
        suffixes=("_src", "_sink"),
    )

    only_in_source = merged[merged["_merge"] == "left_only"].drop(columns=[c for c in merged.columns if c.endswith("_sink")])
    only_in_sink = merged[merged["_merge"] == "right_only"].drop(columns=[c for c in merged.columns if c.endswith("_src")])
    both = merged[merged["_merge"] == "both"]

    # For rows that 'both' matched on key+ordinal, check for content mismatches (non-key columns)
    mismatches = []
    for _, row in both.iterrows():
        # compare non-key columns on src vs sink sides
        src_part = row[[col for col in merged.columns if col.endswith("_src") and not col.startswith("_")]]
        sink_part = row[[col for col in merged.columns if col.endswith("_sink") and not col.startswith("_")]]
        # normalize names (remove suffix)
        src_part.index = [c[:-4] for c in src_part.index]
        sink_part.index = [c[:-5] for c in sink_part.index]
        if not src_part.equals(sink_part):
            mismatches.append({"key": {k: row[f"{k}_src"] for k in key_columns},
                               "src": src_part.to_dict(),
                               "sink": sink_part.to_dict()})
    console.print(
        f"[blue]Reconciliation summary: only_in_source={len(only_in_source)}, "
        f"only_in_sink={len(only_in_sink)}, mismatches={len(mismatches)}[/blue]"
    )
    if len(only_in_source) > 0:
        console.print("[blue]Only in source sample (up to 5 rows):[/blue]")
        console.print(only_in_source.head(5).to_string(index=False))
    if len(only_in_sink) > 0:
        console.print("[blue]Only in sink sample (up to 5 rows):[/blue]")
        console.print(only_in_sink.head(5).to_string(index=False))
    if len(mismatches) > 0:
        console.print("[blue]Mismatches sample (up to 5 rows):[/blue]")
        console.print(mismatches[:5])
    return {"only_in_source": only_in_source, "only_in_sink": only_in_sink, "mismatches": mismatches}


