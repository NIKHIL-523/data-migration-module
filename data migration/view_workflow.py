"""
View migration + validation workflow for 05_workflow sessions.

Views are Spark-side (not kubectl), so the entire flow runs in JupyterHub.
State lives in `view_state.csv` inside the session bundle so JupyterLab's
CSV viewer can edit cells (e.g. wipe `migrate_status` to retry a row).

Public functions:
    migrate_views(spark, bundle_dir, view_specs, *, skip_if_ok=True)
    validate_views(spark, bundle_dir, view_specs, *, skip_if_ok=True)

`view_specs` shape (list of dicts):
    {
        "view":              "<src_db>.<src_view>",            (required)
        "target_view":       "<tgt_view>",                     (default = src_view)
        "target_db":         "<tgt_db>",                       (default = src_db)
        "base_table":        "<full_base_table>",              (default = src.<view>__srdm_inv)
        "target_location":   "abfs://...",                     (default = computed)
        "filter_expression": "<sql predicate>",                (validation only)
        "join_key":          "<col>",                          (validation only; default p_id)
        "cast_to_string":    ["last_updated_attrs", ...],      (validation only)
    }

State CSV schema is defined in session_state.VIEW_STATE_COLUMNS.
"""

from __future__ import annotations

import re
import sys
import traceback
from pathlib import Path

from session_state import (
    VIEW_STATE_COLUMNS, VIEW_STATE_NAME,
    is_pending, load_state, save_state, upsert_row, utc_now_iso,
)


# Match the existing 03_view_migration defaults.
SRC_CATALOG = "iceberg_catalog1"
TGT_CATALOG = "iceberg_catalog2"
DEFAULT_DB  = "kg"
DEFAULT_BASE_TABLE_SUFFIX = "__srdm_inv"
DEFAULT_PROD_LOCATION_ROOT = (
    "abfs://tp-prod-datalake@azpcineupraist01.dfs.core.windows.net/prod/iceberg"
)
# Fallback cutoff used only when a view spec omits `filter_expression`.
# Canonical edit point is `CUTOFF` in workflow_CELLS.md Cell 1 -- pass it
# through per spec for view validation, e.g.
#   {"view": "kg.x", "filter_expression": f"updated_at_ts='{CUTOFF}'"}
DEFAULT_FILTER_TS  = "2026-05-12 23:59:59.999"
DEFAULT_FILTER_SQL = (
    f"updated_at_ts='{DEFAULT_FILTER_TS}' AND kg_content_type='data'"
)
DEFAULT_JOIN_KEY = "p_id"
DEFAULT_CAST_TO_STRING = ["last_updated_attrs"]


# ---------------------------------------------------------------------------
# Spec resolution
# ---------------------------------------------------------------------------

def _resolve_spec(raw: dict) -> dict:
    """Normalise one view_specs entry. Raises on missing required fields."""
    src_full = (raw.get("view") or "").strip()
    if not src_full or "." not in src_full:
        raise ValueError(f"view row must have 'view'='<db>.<name>' (got {raw!r})")
    src_db, src_view = src_full.split(".", 1)
    tgt_db    = (raw.get("target_db") or src_db).strip()
    tgt_view  = (raw.get("target_view") or src_view).strip()
    base_full = (raw.get("base_table") or f"{src_db}.{src_view}{DEFAULT_BASE_TABLE_SUFFIX}").strip()
    tgt_loc   = (raw.get("target_location")
                 or f"{DEFAULT_PROD_LOCATION_ROOT}/{tgt_db}.db/{tgt_view}").strip()

    return {
        "view_full":      src_full,
        "view_suffix":    src_view,
        "src_db":         src_db,
        "src_view":       src_view,
        "tgt_db":         tgt_db,
        "tgt_view":       tgt_view,
        "base_table":     base_full,
        "target_location": tgt_loc,
        "qa_view":        f"{SRC_CATALOG}.{src_full}",
        "prod_view":      f"{TGT_CATALOG}.{tgt_db}.{tgt_view}",
        "filter_expression": (raw.get("filter_expression") or DEFAULT_FILTER_SQL).strip(),
        "join_key":       (raw.get("join_key") or DEFAULT_JOIN_KEY).strip(),
        "cast_to_string": (raw.get("cast_to_string") if isinstance(raw.get("cast_to_string"), list)
                           else list(DEFAULT_CAST_TO_STRING)),
    }


# ---------------------------------------------------------------------------
# View migration -- 6-step DDL rewrite
# ---------------------------------------------------------------------------

def _rewrite_view_ddl(qa_ddl: str, *, src_db: str, src_view: str,
                      tgt_db: str, tgt_view: str,
                      base_table: str, target_location: str) -> str:
    """Apply the canonical 6-step DDL rewrite from 03_view_migration.

    1. SHOW CREATE TABLE output already drops the catalog prefix from the
       view name (Spark behaviour), but we still defensively swap any
       `iceberg_catalog1.` -> `iceberg_catalog2.` in the body.
    2. Strip the `iceberg_catalog<n>.` prefix in front of any DB refs.
    3. Prefix the bare `FROM <src_db>.<src_view>` reference with the base
       table name so the view reads from the migrated __srdm_inv table.
    4. Rename the CREATE TABLE/VIEW <name> to the target view name.
    5. Set LOCATION to the prod target.
    6. Convert to CREATE OR REPLACE.
    """
    ddl = qa_ddl
    # Step 1: catalog swap on body (idempotent).
    ddl = ddl.replace(f"{SRC_CATALOG}.", "")
    ddl = ddl.replace(f"{TGT_CATALOG}.", "")

    # Step 3: rewrite the base-table reference.
    # Match `FROM <src_db>.<src_view>` (case-insensitive, not greedy).
    ddl = re.sub(
        rf"\bFROM\s+{re.escape(src_db)}\.{re.escape(src_view)}\b",
        f"FROM {base_table}",
        ddl,
        flags=re.IGNORECASE,
    )

    # Step 4: rename the created view. SHOW CREATE TABLE returns
    # `CREATE VIEW <db>.<name>` -- we rewrite both schema and name.
    ddl = re.sub(
        rf"CREATE\s+VIEW\s+(?:`)?{re.escape(src_db)}(?:`)?\."
        rf"(?:`)?{re.escape(src_view)}(?:`)?",
        f"CREATE VIEW {tgt_db}.{tgt_view}",
        ddl,
        count=1,
        flags=re.IGNORECASE,
    )

    # Step 5: set/replace location. Iceberg view SHOW CREATE TABLE output
    # carries it inside TBLPROPERTIES as `'location' = '...'`. Older / non-
    # Iceberg dialects expose it as a bare `LOCATION '...'` clause. Handle
    # both; whichever doesn't match is a silent no-op.
    matched = False
    new_ddl, n = re.subn(
        r"'location'\s*=\s*'[^']+'",
        f"'location' = '{target_location}'",
        ddl,
        count=1,
        flags=re.IGNORECASE,
    )
    if n:
        ddl = new_ddl
        matched = True
    new_ddl, n = re.subn(
        r"\bLOCATION\s+'[^']*'",
        f"LOCATION '{target_location}'",
        ddl,
        count=1,
        flags=re.IGNORECASE,
    )
    if n:
        ddl = new_ddl
        matched = True
    if not matched:
        # No location anywhere — append a TBLPROPERTIES-style line at the end.
        # (Most kg views won't hit this branch in practice.)
        ddl += f"\nLOCATION '{target_location}'"

    # Step 6: CREATE -> CREATE OR REPLACE.
    ddl = re.sub(
        r"^\s*CREATE\s+VIEW\b",
        "CREATE OR REPLACE VIEW",
        ddl,
        count=1,
        flags=re.IGNORECASE,
    )
    return ddl


def migrate_one_view(spark, spec: dict, *, verbose: bool = True,
                     dry_run: bool = False) -> tuple[bool, str, str | None]:
    """Migrate one view. Returns (ok, ddl_used, error_or_none).

    dry_run=True: rewrite the DDL but skip execution. Returns
    (True, ddl, None) on a successful rewrite. Use to preview the
    rewritten DDL safely before applying.
    """
    qa_view = spec["qa_view"]
    try:
        rows = spark.sql(f"SHOW CREATE TABLE {qa_view}").collect()
    except Exception as e:
        return False, "", f"SHOW CREATE TABLE failed for {qa_view}: {e}"
    qa_ddl = "\n".join(r[0] for r in rows)

    ddl = _rewrite_view_ddl(
        qa_ddl,
        src_db=spec["src_db"], src_view=spec["src_view"],
        tgt_db=spec["tgt_db"], tgt_view=spec["tgt_view"],
        base_table=spec["base_table"],
        target_location=spec["target_location"],
    )
    if verbose:
        prefix = "  [dry-run] rewrite for" if dry_run else "  rewriting ->"
        print(f"{prefix} {spec['prod_view']}", flush=True)

    if dry_run:
        return True, ddl, None

    try:
        spark.sql(f"USE {TGT_CATALOG}")    # ensure CREATE VIEW lands in prod catalog
        spark.sql(ddl)
        spark.sql(f"USE {SRC_CATALOG}")    # restore default
    except Exception as e:
        return False, ddl, f"spark.sql failed: {e}"
    return True, ddl, None


def migrate_views(spark, bundle_dir: str | Path, view_specs: list[dict], *,
                  skip_if_ok: bool = True, verbose: bool = True,
                  dry_run: bool = False) -> "pd.DataFrame":
    """Migrate every spec in `view_specs`, recording each outcome in
    <bundle_dir>/view_state.csv.

    `skip_if_ok=True` (default): rows whose current migrate_status='success'
    are not re-migrated. Edit view_state.csv (or pass skip_if_ok=False) to
    force a re-run.

    `dry_run=True`: rewrite each DDL and print it; do NOT execute and do NOT
    update view_state.csv. Use to preview the full batch before applying.
    """
    bundle_dir = Path(bundle_dir)
    state_df = load_state(bundle_dir, kind="view")

    print(f"src catalog : {SRC_CATALOG}")
    print(f"tgt catalog : {TGT_CATALOG}")
    print(f"specs       : {len(view_specs)}")
    print(f"state file  : {bundle_dir / VIEW_STATE_NAME}")
    print(f"skip_if_ok  : {skip_if_ok}")
    print(f"dry_run     : {dry_run}")
    print("-" * 60)

    for i, raw in enumerate(view_specs):
        try:
            spec = _resolve_spec(raw)
        except Exception as e:
            print(f"{i+1:>3} SKIP-PARSE {raw!r}: {e}", file=sys.stderr)
            continue

        view_full = spec["view_full"]
        if skip_if_ok and not is_pending(
            state_df, "view_suffix", spec["view_suffix"], "migrate_status",
        ):
            print(f"{i+1:>3} SKIP        {view_full}")
            continue

        label = "DRY-RUN    " if dry_run else "MIGRATE    "
        print(f"{i+1:>3} {label} {view_full}")
        ok, ddl, err = migrate_one_view(spark, spec, verbose=verbose, dry_run=dry_run)

        if dry_run:
            status = "OK" if ok else "FAIL"
            print(f"{i+1:>3} {status:<10} {view_full}"
                  + (f"   ({err})" if err else ""))
            if verbose and ok:
                print("---- rewritten DDL ----")
                print(ddl)
                print("-----------------------")
            continue  # no state writes in dry-run

        state_df = upsert_row(
            state_df, "view_suffix", spec["view_suffix"],
            {
                "qa_view":        spec["qa_view"],
                "prod_view":      spec["prod_view"],
                "migrate_status": "success" if ok else "failed",
                "migrate_at":     utc_now_iso(),
                "migrate_error":  "" if ok else (err or ""),
            },
        )
        save_state(state_df, bundle_dir, kind="view")
        print(f"{i+1:>3} {'DONE' if ok else 'FAIL'}     {view_full}"
              + (f"   ({err})" if err else ""))

    print("-" * 60)
    if dry_run:
        print(f"Dry-run complete: {len(view_specs)} spec(s); no state changes.")
        return state_df
    sub = state_df[state_df["migrate_status"] != ""]
    n_ok   = (sub["migrate_status"] == "success").sum()
    n_fail = (sub["migrate_status"] == "failed").sum()
    print(f"Summary: {n_ok} success / {n_fail} failed (of {len(sub)} touched).")
    return state_df


# ---------------------------------------------------------------------------
# View validation -- count + row-level diff
# ---------------------------------------------------------------------------

def validate_one_view(spark, spec: dict, *, verbose: bool = True,
                      per_column: bool = False) -> dict:
    """Run COUNT + row-level diff for one view. Returns result dict suitable
    for merging into view_state.csv.

    per_column=True: also compute mismatch count per compare column.
    Adds N extra COUNTs (one per compare column) — slower but useful for
    pinpointing which columns drift. Result dict gets a non-empty
    `per_column_mismatches` dict (col_name -> count) for columns with at
    least one mismatch; the field is always present (empty dict if disabled
    or no mismatches).
    """
    from pyspark.sql import functions as F

    out: dict = {
        "qa_count": "", "prod_count": "", "mismatched_rows": "",
        "validation_status": "", "validation_error": "",
        "per_column_mismatches": {},
    }
    qa_view   = spec["qa_view"]
    prod_view = spec["prod_view"]
    where_sql = spec["filter_expression"]
    join_key  = spec["join_key"]
    cast_to_string = set(spec["cast_to_string"])

    try:
        old_df = spark.table(qa_view).where(where_sql) if where_sql else spark.table(qa_view)
        new_df = spark.table(prod_view).where(where_sql) if where_sql else spark.table(prod_view)

        if join_key not in old_df.columns:
            raise RuntimeError(f"join key {join_key!r} not in QA view columns")
        if join_key not in new_df.columns:
            raise RuntimeError(f"join key {join_key!r} not in prod view columns")

        compare_cols = [c for c in old_df.columns if c != join_key]

        def _rename(side_prefix: str, df):
            return df.select(
                F.col(join_key),
                *[
                    (
                        F.col(c).cast("string").alias(f"{side_prefix}__{c}")
                        if c in cast_to_string
                        else F.col(c).alias(f"{side_prefix}__{c}")
                    )
                    for c in compare_cols
                ],
            )

        old_renamed = _rename("old", old_df)
        new_renamed = _rename("new", new_df)
        joined = old_renamed.join(new_renamed, on=join_key, how="full_outer")

        qa_count   = old_df.count()
        prod_count = new_df.count()
        out["qa_count"]   = qa_count
        out["prod_count"] = prod_count
        count_ok = (qa_count == prod_count)

        total_mismatched = 0
        if compare_cols:
            mismatch_conds = [
                ~F.col(f"old__{c}").eqNullSafe(F.col(f"new__{c}"))
                for c in compare_cols
            ]
            any_mismatch = mismatch_conds[0]
            for cond in mismatch_conds[1:]:
                any_mismatch = any_mismatch | cond
            total_mismatched = joined.filter(any_mismatch).count()

            if per_column:
                per_col: dict[str, int] = {}
                for c in compare_cols:
                    cnt = joined.filter(
                        ~F.col(f"old__{c}").eqNullSafe(F.col(f"new__{c}"))
                    ).count()
                    if cnt > 0:
                        per_col[c] = cnt
                out["per_column_mismatches"] = per_col
        out["mismatched_rows"] = total_mismatched

        ok = count_ok and (total_mismatched == 0)
        out["validation_status"] = "ok" if ok else "mismatch"
    except Exception as e:
        out["validation_status"] = "error"
        out["validation_error"]  = str(e)
        if verbose:
            traceback.print_exc()
    return out


def status_label(res: dict) -> str:
    """Single-line human-readable label for a validation result dict.

    Useful for printing per-view summaries in notebook cells without
    re-deriving the formatting at each callsite.
    """
    s = (res.get("validation_status") or "").lower()
    if s == "ok":
        return "OK"
    if s == "mismatch":
        qa   = res.get("qa_count")
        prod = res.get("prod_count")
        rows = res.get("mismatched_rows")
        return f"DIFF (qa={qa}, prod={prod}, mismatched_rows={rows})"
    if s == "error":
        msg = (res.get("validation_error") or "")[:120]
        return f"ERROR: {msg}"
    return "PENDING"


def validate_views(spark, bundle_dir: str | Path, view_specs: list[dict], *,
                   skip_if_ok: bool = True, verbose: bool = True,
                   per_column: bool = False) -> "pd.DataFrame":
    """Validate each spec, writing into <bundle_dir>/view_state.csv.

    `skip_if_ok=True` (default): rows whose current validation_status='ok'
    are not re-validated. Edit view_state.csv or pass skip_if_ok=False to
    force a re-run.

    `per_column=True`: forwarded to validate_one_view — adds per-column
    mismatch counts to each result. Printed inline for visibility but NOT
    persisted to view_state.csv (the CSV stays flat).
    """
    bundle_dir = Path(bundle_dir)
    state_df = load_state(bundle_dir, kind="view")

    print(f"src catalog : {SRC_CATALOG}")
    print(f"tgt catalog : {TGT_CATALOG}")
    print(f"specs       : {len(view_specs)}")
    print(f"state file  : {bundle_dir / VIEW_STATE_NAME}")
    print(f"skip_if_ok  : {skip_if_ok}")
    print(f"per_column  : {per_column}")
    print("-" * 60)
    print(f"{'#':>3}  {'status':<10}  view")

    for i, raw in enumerate(view_specs):
        try:
            spec = _resolve_spec(raw)
        except Exception as e:
            print(f"{i+1:>3}  SKIP-PARSE  {raw!r}: {e}", file=sys.stderr)
            continue

        if skip_if_ok and not is_pending(
            state_df, "view_suffix", spec["view_suffix"], "validation_status",
        ):
            print(f"{i+1:>3}  SKIP-OK     {spec['view_full']}")
            continue

        res = validate_one_view(spark, spec, verbose=verbose, per_column=per_column)
        state_df = upsert_row(
            state_df, "view_suffix", spec["view_suffix"],
            {
                "qa_view":          spec["qa_view"],
                "prod_view":        spec["prod_view"],
                "validation_at":    utc_now_iso(),
                "validation_status": res["validation_status"],
                "qa_count":          res["qa_count"],
                "prod_count":        res["prod_count"],
                "mismatched_rows":   res["mismatched_rows"],
                "validation_error":  res["validation_error"],
            },
        )
        save_state(state_df, bundle_dir, kind="view")
        label = res["validation_status"].upper() or "?"
        print(f"{i+1:>3}  {label:<10}  {spec['view_full']}")
        if per_column and res.get("per_column_mismatches"):
            for col, cnt in res["per_column_mismatches"].items():
                print(f"       {col}: {cnt:,}")

    print("-" * 60)
    sub = state_df[state_df["validation_status"] != ""]
    n_ok    = (sub["validation_status"] == "ok").sum()
    n_mis   = (sub["validation_status"] == "mismatch").sum()
    n_err   = (sub["validation_status"] == "error").sum()
    print(f"Summary: {n_ok} ok / {n_mis} mismatch / {n_err} error (of {len(sub)} touched).")
    return state_df


__all__ = [
    "SRC_CATALOG", "TGT_CATALOG", "DEFAULT_DB",
    "DEFAULT_BASE_TABLE_SUFFIX", "DEFAULT_PROD_LOCATION_ROOT",
    "DEFAULT_FILTER_TS", "DEFAULT_FILTER_SQL",
    "DEFAULT_JOIN_KEY", "DEFAULT_CAST_TO_STRING",
    "migrate_views", "validate_views",
    "migrate_one_view", "validate_one_view",
    "status_label",
]
