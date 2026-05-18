"""
Session state helpers for the 05_workflow bundles.

State lives in a CSV per session, NOT JSON. JupyterLab opens CSVs in a
native click-to-edit table view, so users can override cells (e.g. wipe
`apply_status` to force a rerun, or correct a stale row) without writing
code. The migrate.py driver uses only stdlib `csv` so it works on the
ops VM without pandas; this module is the pandas-flavoured side used
from the notebook.

Two CSV schemas (one file each per session):

table_state.csv  (driven by migrate.py + the validate cell)
    table_key          (PK; "<src_db>__<src_table>", unique across schemas
                        so two rows with the same bare table name in
                        different src schemas don't collide)
    source_table           full db.table on QA side
    target_table         full db.table on prod side
    apply_status       "" | success | failed | timeout | applying
    apply_at           ISO UTC timestamp or ""
    k8s_state          COMPLETED | FAILED | SUBMITTED | ... | ""
    k8s_name           RFC-1123 metadata.name
    apply_error        free-text error or ""
    validation_status  "" | ok | mismatch | error
    validation_at      ISO UTC timestamp or ""
    source_count           int or "" (post-filter COUNT(*) on QA)
    target_count         int or "" (post-filter COUNT(*) on prod)
    target_count_total   int or "" (UN-filtered COUNT(*) on prod;
                       sanity check that prod isn't carrying rows beyond
                       the cutoff used for this migrate)
    partition_source       partition spec, migrate.py format e.g. "updated_at_ts:day"
    partition_target     same, on prod side
    partition_match    "true" / "false"
    validation_error   free-text or ""

view_state.csv  (driven by view_workflow.migrate_view + validate_view)
    view_suffix
    source_view_fqn    full catalog-qualified source view (e.g. iceberg_catalog1.kg.<view>)
    target_view_fqn    full catalog-qualified target view (e.g. iceberg_catalog2.kg.<view>)
    migrate_status     "" | success | failed
    migrate_at
    migrate_error
    validation_status  "" | ok | mismatch | error
    validation_at
    source_count
    target_count
    mismatched_rows
    validation_error

Edit cells freely in JupyterLab's CSV viewer. Re-runs honour your edits:
clear `apply_status` for a row -> next migrate.py run re-applies it.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path


TABLE_STATE_NAME = "table_state.csv"
VIEW_STATE_NAME  = "view_state.csv"

TABLE_STATE_COLUMNS = [
    # Identity
    "table_key",
    "source_table", "target_table",
    # Migration (stamped by migrate.py on the ops VM)
    "apply_status", "apply_at", "k8s_state", "k8s_name", "apply_error",
    # Validation (this is what you'll re-run after a migrate)
    "validation_status", "validation_at",
    "source_count", "target_count", "target_count_total",
    "partition_source", "partition_target", "partition_match",
    "validation_error",
]

VIEW_STATE_COLUMNS = [
    "view_suffix",
    "source_view_fqn", "target_view_fqn",
    "migrate_status", "migrate_at", "migrate_error",
    "validation_status", "validation_at",
    "source_count", "target_count", "mismatched_rows", "validation_error",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ---------------------------------------------------------------------------
# pandas-flavoured load/save (notebook side)
# ---------------------------------------------------------------------------

def _empty_df(columns: list[str]):
    import pandas as pd
    return pd.DataFrame({c: pd.Series(dtype="object") for c in columns})


def load_state(bundle_dir: str | Path, *, kind: str = "table"):
    """Load <bundle_dir>/{table,view}_state.csv as a pandas DataFrame.

    Returns an empty DataFrame with the canonical schema if the file is
    missing (so callers can write into a fresh session).
    """
    import pandas as pd
    columns = TABLE_STATE_COLUMNS if kind == "table" else VIEW_STATE_COLUMNS
    name = TABLE_STATE_NAME if kind == "table" else VIEW_STATE_NAME
    path = Path(bundle_dir) / name
    if not path.is_file():
        return _empty_df(columns)
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    # Ensure all canonical columns exist (covers older bundles that may
    # have fewer columns); preserves any extra columns the user added.
    for c in columns:
        if c not in df.columns:
            df[c] = ""
    return df


def save_state(df, bundle_dir: str | Path, *, kind: str = "table") -> Path:
    """Write the DataFrame back to <bundle_dir>/{table,view}_state.csv."""
    name = TABLE_STATE_NAME if kind == "table" else VIEW_STATE_NAME
    path = Path(bundle_dir) / name
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = TABLE_STATE_COLUMNS if kind == "table" else VIEW_STATE_COLUMNS
    # Preserve user-added columns by keeping originals when present.
    cols = columns + [c for c in df.columns if c not in columns]
    df.to_csv(path, index=False, columns=cols)
    return path


def upsert_row(df, key_col: str, key_val: str, fields: dict):
    """Set or insert a row keyed by `key_col=key_val` with the given fields."""
    import pandas as pd
    mask = df[key_col] == key_val
    if mask.any():
        for k, v in fields.items():
            df.loc[mask, k] = "" if v is None else str(v)
        return df
    row = {c: "" for c in df.columns}
    row[key_col] = key_val
    for k, v in fields.items():
        row[k] = "" if v is None else str(v)
    return pd.concat([df, pd.DataFrame([row])], ignore_index=True)


def is_pending(df, key_col: str, key_val: str, status_col: str) -> bool:
    """A row is 'pending' if no state OR status != 'success'/'ok'."""
    mask = df[key_col] == key_val
    if not mask.any():
        return True
    status = str(df.loc[mask, status_col].iloc[0]).strip().lower()
    return status not in {"success", "ok"}


__all__ = [
    "TABLE_STATE_NAME", "VIEW_STATE_NAME",
    "TABLE_STATE_COLUMNS", "VIEW_STATE_COLUMNS",
    "utc_now_iso",
    "load_state", "save_state",
    "upsert_row", "is_pending",
]
