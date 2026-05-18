"""
Write a self-contained session bundle to `05_workflow/sessions/<name>/`:

    sessions/<name>/
    ├── migrate.py            (copy of migrate_template/migrate.py)
    ├── datasources.json      (immutable spec, generated)
    ├── template.json         (Spark CR template, generated)
    ├── table_state.csv       (progress; JupyterLab-editable CSV)
    ├── view_state.csv        (empty header; populated by view_workflow)
    └── README.md             (run instructions)

State is CSV (not JSON) so JupyterLab's CSV viewer can click-edit cells
(wipe a status to retry, mark a row done manually, etc.). migrate.py uses
only stdlib `csv` so it works on the ops VM without pandas.
"""

from __future__ import annotations

import csv
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from session_state import (
    TABLE_STATE_COLUMNS, TABLE_STATE_NAME,
    VIEW_STATE_COLUMNS,  VIEW_STATE_NAME,
)


HERE             = Path(__file__).resolve().parent
SESSIONS_ROOT    = HERE / "sessions"
MIGRATE_TEMPLATE = HERE / "migrate_template" / "migrate.py"


def write_session_bundle(
    session_name: str,
    *,
    datasources_rows: list[dict],
    template: dict,
    overwrite: bool = False,
) -> Path:
    """
    Write a complete session bundle (tables + view-state). Returns the
    session folder Path. Always seeds both `table_state.csv` and an empty
    `view_state.csv` so view migration can be added later without re-bundling.

    Args:
      session_name: subfolder name under `05_workflow/sessions/`. Must be
        a valid filesystem name (avoid spaces / slashes).
      datasources_rows: list-of-dicts in migrate.py format (output of
        `build_datasources_rows`).
      template: dict in SparkApplication CR format (output of
        `template_builder.build_template`).
      overwrite: if False (default), refuses to write into an existing
        non-empty session folder to protect prior state. Pass True to
        clobber.
    """
    if not session_name or "/" in session_name or ".." in session_name:
        raise ValueError(f"invalid session_name: {session_name!r}")
    if not MIGRATE_TEMPLATE.is_file():
        raise FileNotFoundError(
            f"migrate.py template missing: {MIGRATE_TEMPLATE} "
            "(check 05_workflow/migrate_template/)"
        )

    session_dir = SESSIONS_ROOT / session_name
    if session_dir.exists() and any(session_dir.iterdir()):
        if not overwrite:
            raise FileExistsError(
                f"session folder {session_dir} is non-empty. Pass overwrite=True "
                "to clobber, OR pick a different session_name."
            )
    session_dir.mkdir(parents=True, exist_ok=True)

    # 1. Copy the canonical migrate.py
    shutil.copyfile(MIGRATE_TEMPLATE, session_dir / "migrate.py")

    # 2. Write datasources.json (the migration spec; immutable)
    (session_dir / "datasources.json").write_text(
        json.dumps(datasources_rows, indent=2) + "\n", encoding="utf-8",
    )

    # 3. Write template.json (the Spark CR template)
    (session_dir / "template.json").write_text(
        json.dumps(template, indent=4) + "\n", encoding="utf-8",
    )

    # 4. Seed table_state.csv with one row per datasources entry
    _write_empty_state_csv(
        session_dir / TABLE_STATE_NAME,
        TABLE_STATE_COLUMNS,
        seed_rows=[
            {
                "table_key":  _table_key(r.get("table", "")),
                "qa_table":   r.get("table", ""),
                "prod_table": _prod_table_from_ds_row(r),
            }
            for r in datasources_rows
        ],
    )

    # 5. Always seed view_state.csv — view specs can be added later via Cell 13.
    _write_empty_state_csv(
        session_dir / VIEW_STATE_NAME,
        VIEW_STATE_COLUMNS,
        seed_rows=[],
    )

    # 6. README with run instructions specific to this bundle
    (session_dir / "README.md").write_text(
        _render_readme(session_name, datasources_rows),
        encoding="utf-8",
    )

    return session_dir


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _table_key(full: str) -> str:
    """Unique row key: <src_db>__<src_table>. Same shape as migrate.py's
    table_key() so both sides hash to the same value."""
    full = (full or "").strip()
    if not full:
        return ""
    if "." not in full:
        return full
    db, _, t = full.partition(".")
    return f"{db}__{t}"


def _prod_table_from_ds_row(row: dict) -> str:
    qa = row.get("table") or ""
    if "." not in qa:
        return ""
    qa_db, qa_t = qa.split(".", 1)
    prod_db = row.get("outputSchema") or qa_db
    prod_t  = row.get("outputTable")  or qa_t
    return f"{prod_db}.{prod_t}"


def _write_empty_state_csv(path: Path, columns: list[str],
                            seed_rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        for r in seed_rows:
            row = {c: "" for c in columns}
            row.update({k: v for k, v in r.items() if k in columns})
            w.writerow(row)


def show_bundle_links(session_dir: Path) -> None:
    """In a Jupyter cell, render clickable download links to each bundle file."""
    from IPython.display import FileLink, display
    for name in ("migrate.py", "datasources.json", "template.json",
                 "table_state.csv", "view_state.csv", "view_specs.json",
                 "README.md"):
        p = session_dir / name
        if p.is_file():
            display(FileLink(str(p)))


def _render_readme(session_name: str, rows: list[dict]) -> str:
    when = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    sample_qa = rows[0].get("table") if rows else "<no rows>"
    return f"""# Iceberg migration session: `{session_name}`

Generated by the 05_workflow notebook at {when}.

## What's in this folder

| File | Purpose |
|---|---|
| `migrate.py`         | State-aware driver: kubectl applies + polls SparkApplications. |
| `datasources.json`   | The migration spec ({len(rows)} table(s)). Immutable. |
| `template.json`      | The SparkApplication CR template filled in per row. |
| `table_state.csv`    | Table-migration + table-validation progress tracker. |
| `view_state.csv`     | View-migration + view-validation tracker (filled by Hub notebook, not migrate.py). |

First row sample: `{sample_qa}`.

## Run instructions (ops VM)

```bash
kubectl config current-context
kubectl get sparkapplication -n prod          # sanity-check visibility

python migrate.py --dry-run                   # render manifests, no apply
python migrate.py                              # apply pending rows (skip-success default)
python migrate.py --rerun-all                  # apply every row regardless of state
python migrate.py --table <suffix-or-fullname>
python migrate.py --list-pending
```

## How skip-success works

After each `kubectl apply`, migrate.py polls `.status.applicationState.state`
every 30s until COMPLETED or FAILED. On COMPLETED it sets
`apply_status=success` for that row in `table_state.csv`. On the next run,
success-state rows are dropped. To override:

- pass `--rerun-all`, OR
- open `table_state.csv` in JupyterLab and wipe the `apply_status` cell
  for the rows you want re-applied (no need to edit JSON).

## On failure

`apply_status=failed` (or `timeout`) aborts the batch by default. Use
`--continue-on-failure` to keep going. The failure reason is captured in
`apply_error` for inspection.
"""


__all__ = [
    "SESSIONS_ROOT",
    "MIGRATE_TEMPLATE",
    "write_session_bundle",
    "show_bundle_links",
]
