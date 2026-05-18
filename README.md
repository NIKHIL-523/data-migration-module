# Iceberg Migration Toolkit

> Notebook-driven source → target migration of Apache Iceberg tables and
> views across two Hive Metastore catalogs. Tables migrate via Spark
> Operator on Kubernetes; views and validation run inside JupyterHub.

[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![Spark 3.5](https://img.shields.io/badge/spark-3.5-orange.svg)](https://spark.apache.org/)
[![Iceberg 1.9](https://img.shields.io/badge/iceberg-1.9-1d63ed.svg)](https://iceberg.apache.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

---

> End-to-end flow diagram: [`data migration/WORKFLOW_DIAGRAM.md`](data%20migration/WORKFLOW_DIAGRAM.md).

## What it does

- **Tables** — generates a self-contained `SparkApplication` CR bundle
  per batch, drops on an ops VM, polls Kubernetes for completion
  (`COMPLETED` / `FAILED`).
- **Views** — rewrites view DDL (catalog swap, base-table prefix,
  `LOCATION`, `CREATE OR REPLACE`) and re-applies on the target catalog
  from JupyterHub, no kubectl needed.
- **Validation** — `COUNT(*)` parity + Iceberg partition spec
  comparison for tables; full-outer `eqNullSafe` join + per-column
  mismatch reporting for views.
- **Property sync** — copy specific `TBLPROPERTIES` keys source → target, key
  by key, with dry-run plan + safety-gated apply.
- **Backup rename** — schema-level move-aside before re-migration
  (`<schema>` → `<schema>_v2`), table-by-table since Hive doesn't
  support `RENAME SCHEMA`.

State for each session lives in **CSV files inside the bundle** —
`table_state.csv` and `view_state.csv`, edited in-place via
JupyterLab's CSV viewer. Wipe a status cell to force a re-run; no JSON
editor needed. The ops-VM driver uses only stdlib `csv` so it runs
without pandas.

## Quick start

```bash
git clone https://github.com/NIKHIL-523/data-migration-module.git
cd "data-migration-module/data migration"
# Open workflow.ipynb in JupyterHub (or any PySpark kernel)
```

In **Cell 2**, edit the four config blocks at the top:

```python
SOURCE_ENV = "qa"      # display label for the source side
TARGET_ENV = "prod"    # display label for the target side

CUTOFF_TS  = "2026-05-12 23:59:59.999"   # one edit; flows everywhere

connection = {
    "source_hms_uri":   "thrift://...",
    "target_hms_uri":   "thrift://...",
    "source_warehouse": "abfs://...",
    "target_warehouse": "abfs://...",
    ...
}
sparkapp   = { "driver_memory": "20g", "executor_instances": "30", ... }
```

`SOURCE_ENV` / `TARGET_ENV` are display-only labels — the code paths are
direction-agnostic, so the same notebook works for `dev → staging`,
`staging → prod`, or any other two-environment migration.

Restart the kernel, run cells in order. Each section opens with a
**What / Run when / Gotcha** markdown intro.

## Repo layout

```
data-migration-module/
├── README.md, LICENSE, CHANGELOG.md
└── data migration/
    ├── workflow.ipynb              # 39 cells, ready to open
    ├── workflow_CELLS.md           # diffable source for the notebook
    ├── CONTEXT.md                  # design notes
    ├── WORKFLOW_DIAGRAM.md         # the full mermaid diagram
    ├── spark_session.py            # dual-catalog SparkSession
    ├── catalog_traversal.py        # discovery + partition spec
    ├── build_datasources.py        # selections → datasources.json
    ├── template_builder.py         # SparkApplication CR template
    ├── bundle_writer.py            # session bundle writer
    ├── session_state.py            # CSV state helpers (notebook)
    ├── view_workflow.py            # view migrate + validate (in-Hub)
    ├── table_properties.py         # TBLPROPERTIES diff + sync
    └── migrate_template/
        └── migrate.py              # stdlib-only ops-VM driver
```

## Design decisions locked

- **CSV, not JSON, for state.** JupyterLab opens CSVs in a
  click-to-edit table viewer — wipe a status cell to retry, no JSON
  editor required.
- **Skip-success default ON.** Re-running a bundle picks up where it
  left off. Pass `--rerun-all` (CLI) or `skip_if_ok=False` (notebook)
  to override.
- **Polling, not sleep-guessing.** After each `kubectl apply`,
  `migrate.py` polls `.status.applicationState.state` every 30s until
  COMPLETED or FAILED.
- **One canonical migrate.py.** Lives at
  `migrate_template/migrate.py`; the bundle writer copies it per
  session. No per-session edits.
- **Safety gates on destructive cells.** Property apply (notebook Cell
  30) and backup rename (Cell 36) default to no-op; flip a flag to
  execute.
- **Single-source `CUTOFF_TS`.** Defined once in Cell 2, referenced by
  table migrate (Cell 8), table validate (Cell 12), view validate
  (Cell 20).

## Cell index

| #     | Section                                   | Type                |
|-------|-------------------------------------------|---------------------|
| 1–2   | Setup (SparkSession + imports + CUTOFF)   | one-shot            |
| 3–4   | List source-catalog schemas               | discovery           |
| 5–6   | Table summary + per-table `COUNT(*)`      | discovery           |
| 7–8   | Define selections + datasources rows      | plan                |
| 9–10  | Build template + write session bundle     | plan                |
| 11–12 | Table validation (count + partition)      | post-migrate        |
| 13–14 | Manual ad-hoc table validation            | optional, stateless |
| 15–16 | Define view specs                         | view plan           |
| 17–18 | Pilot single-view migrate (dry-run+apply) | view pilot          |
| 19–20 | Manual row-level diff for one view        | optional, stateless |
| 21–22 | Batch view migrate                        | view apply          |
| 23–24 | Batch view validate                       | view validate       |
| 25–26 | Property sync: single-table inspect       | property            |
| 27–28 | Property sync: plan                       | property            |
| 29–30 | Property sync: apply (gated)              | property            |
| 31–32 | Property sync: verify                     | property            |
| 33–34 | Backup rename: config                     | recovery            |
| 35–36 | Backup rename: apply (gated)              | recovery            |
| 37–38 | Backup rename: verify                     | recovery            |

(Each section is a `markdown` intro cell + a `code` cell — 19 sections
× 2 cells + 1 top-level intro = 39 cells in `workflow.ipynb`.)

## Status

Early — used in anger for one client migration, not yet battle-tested
across many environments. Issues and PRs welcome.

## License

MIT — see [LICENSE](LICENSE).
