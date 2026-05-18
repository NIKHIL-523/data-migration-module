# 05_workflow — session handover

End-to-end notebook for the table + view migration flow. User runs
everything in one notebook: enter connection details → traverse catalog
→ pick tables/views → build a self-contained bundle. Tables go to the
ops VM for `python migrate.py`; views run inside JupyterHub via Spark.
**State for both lives in CSV files** so JupyterLab's click-to-edit CSV
viewer can override cells without touching code.

## Layout

```
05_workflow/
├── spark_session.py         build_spark_session(connection, sparkapp, extra_conf)
├── catalog_traversal.py     list_schemas / summarize_tables / summarize_views / partition derivation (Py4J)
├── build_datasources.py     normalise selections -> datasources.json rows
├── template_builder.py      build_template(connection, sparkapp) -> SparkApplication CR
├── bundle_writer.py         write_session_bundle (tables + view-state seed)
├── session_state.py         pandas-flavoured CSV state helpers (notebook side)
├── view_workflow.py         migrate_views + validate_views (in-Hub, CSV-tracked)
├── table_properties.py      source<->target TBLPROPERTIES extract + diff (post-migration property sync)
├── migrate_template/
│   └── migrate.py           CANONICAL ops-VM driver -- stdlib csv only, no pandas
├── sessions/                per-session bundles (gitignore-able)
│   └── <session_name>/
│       ├── migrate.py       copy of the canonical driver
│       ├── datasources.json migration spec (immutable, generated)
│       ├── template.json    Spark CR template
│       ├── table_state.csv  apply + validation tracker (CSV, JupyterLab-editable)
│       ├── view_state.csv   view migrate + validation tracker (always seeded)
│       ├── view_specs.json  view spec (if views in this session)
│       └── README.md        per-bundle run instructions
├── workflow_CELLS.md        the canonical notebook
├── WORKFLOW_DIAGRAM.md      mermaid diagram of the end-to-end flow
├── adhoc_kg_publish_cutoff_delete_CELLS.md       ad-hoc: delete post-cutoff rows from prod.kg_publish
├── adhoc_rename_kg_publish_olap_to_v1_CELLS.md   ad-hoc: rename prod.kg_publish + prod.kg_olap to _v1
├── adhoc_extract_table_properties_CELLS.md       ad-hoc: extract Iceberg TBLPROPERTIES (source vs target diff) for the property-sync phase
└── CONTEXT.md               this file
```

## Decisions locked

- **State = CSV per session, not JSON.** JupyterLab opens CSVs in a
  click-to-edit table viewer; JSON requires a text editor. Two CSVs per
  bundle: `table_state.csv` (migration + table-validation) and
  `view_state.csv` (view migration + view-validation). Schema definitions
  live in `session_state.TABLE_STATE_COLUMNS` /
  `session_state.VIEW_STATE_COLUMNS`.
- **migrate.py uses stdlib `csv` only.** No pandas required on the ops
  VM. The notebook side uses pandas via `session_state.load_state` /
  `save_state` against the same file.
- **Skip-success default ON.** Both migrate.py (apply_status) and the
  view workflow (migrate_status / validation_status) skip rows where the
  relevant status column is `success`/`ok`. Force a retry by wiping the
  cell in JupyterLab, OR pass `--rerun-all` / `skip_if_ok=False`.
- **One canonical migrate.py** at `migrate_template/migrate.py`. Bundle
  writer copies it into each session folder; no per-session edits.
- **Views run in JupyterHub**, not via kubectl. `view_workflow.py`
  exposes `migrate_views(spark, bundle_dir, view_specs)` and
  `validate_views(spark, bundle_dir, view_specs)`. The 6-step DDL
  rewrite (catalog swap, base-table prefix, location, CREATE OR REPLACE)
  is in `_rewrite_view_ddl`.
- **Polling stays as the apply-wait strategy.** After each `kubectl
  apply`, migrate.py polls SparkApplication state every 30s until
  COMPLETED or FAILED. `--no-poll` was removed from the canonical
  driver; the source_count sleep estimator is kept for reference only.
- **Display style** = plain `display(df)` inside `pd.option_context`.
  No styled HTML, no glyphs.

## Notebook flow at a glance

| Cells | Step |
|---|---|
| 1-2  | Enter `connection` + `sparkapp` dicts → build `SparkSession` |
| 3-5  | List source schemas → pick → table summary with partition spec |
| 6-7  | Define `selections` (src/tgt mapping + overrides) → build datasources rows |
| 8-9  | Build `template.json` → write session bundle (4-5 files in `sessions/<name>/`) |
| 10-11 | After ops-VM run: read `table_state.csv`, run count validation, write back |
| 12-13 | Define `view_specs`, persist into the same bundle |
| 14-15 | Migrate views (in-Hub), validate views; both update `view_state.csv` |

## State CSV schemas

`table_state.csv` (driven by migrate.py + the table-validation cell):

| column | populated by | values |
|---|---|---|
| `table_key` (PK) | seeded at bundle write | `<src_db>__<src_table>` — unique across source schemas |
| `source_table`, `target_table` | seeded; refreshed by migrate.py | fully qualified `<db>.<t>` |
| `apply_status` | migrate.py | `""` \| `applying` \| `success` \| `failed` \| `timeout` |
| `apply_at` | migrate.py | ISO UTC |
| `k8s_state` | migrate.py | `COMPLETED` \| `FAILED` \| `SUBMITTED` \| … |
| `k8s_name` | migrate.py | RFC-1123 metadata.name |
| `apply_error` | migrate.py | string |
| `validation_status` | notebook | `""` \| `ok` \| `mismatch` \| `error` |
| `validation_at`, `source_count`, `target_count`, `target_count_total` | notebook | … |
| `partition_source`, `partition_target`, `partition_match` | notebook | derived via Py4J |
| `validation_error` | notebook | string |

`view_state.csv` (driven by view_workflow):

| column | populated by | values |
|---|---|---|
| `view_suffix` (PK) | seeded | string |
| `source_view`, `target_view` | view_workflow | fully qualified |
| `migrate_status` | migrate_views | `""` | `success` | `failed` |
| `migrate_at`, `migrate_error` | migrate_views | … |
| `validation_status` | validate_views | `""` | `ok` | `mismatch` | `error` |
| `validation_at`, `source_count`, `target_count`, `mismatched_rows`, `validation_error` | validate_views | … |

## migrate.py contract (ops VM)

Self-contained — copy the session folder anywhere with Python 3 + kubectl
and it works. Reads only `./template.json`, `./datasources.json`,
`./table_state.csv`. Writes `./rendered/<suffix>.json` and updates
`./table_state.csv` in place.

CLI:

```
python migrate.py                    # apply pending rows (skip-success default)
python migrate.py --dry-run          # render manifests only
python migrate.py --table <name>     # one row (suffix or db.t)
python migrate.py --rerun-all        # apply every row regardless of state
python migrate.py --list-pending     # show what would be applied
python migrate.py --continue-on-failure
python migrate.py --poll-interval 30 --poll-timeout 14400
```

## Forcing a re-run on a specific row

Open `table_state.csv` (or `view_state.csv`) in JupyterLab's CSV viewer,
double-click the relevant status cell, delete the value, save. Next
invocation will treat that row as pending and re-do it.

For batch overrides: edit the CSV in any editor or in a notebook cell:

```python
from session_state import load_state, save_state
df = load_state(bundle, kind="table")
df.loc[df["table_key"] == "kg_publish_final__sds_em__finding__publish", "apply_status"] = ""
save_state(df, bundle, kind="table")
```

## Cross-refs

- Legacy folders (`legacy/01_table_migration/`, `legacy/02_table_validation/`,
  `legacy/03_view_migration/`, `legacy/04_view_validation/`) are still in
  the workspace but **no longer authoritative**. New work uses 05_workflow
  exclusively.
- `../legacy/rough/spark_session_validation.py` — fuller K8s SparkSession
  builder with pod template / image / service account wiring; useful as a
  reference for `extra_conf` values to pass to `build_spark_session(...)`.

## Open notes / known gaps

- View DDL rewrite step 5 (LOCATION) has a fallback for views that don't
  have an existing LOCATION clause that appends one at the end. Untested
  on the kg corpus; if you hit a view where the rewrite produces invalid
  SQL, file the original DDL + the rewritten DDL and we can tune the
  regex.
- `view_specs.json` is the source of truth for view specs. `view_state.csv`
  records progress but doesn't carry the full spec (filter expression,
  cast_to_string, etc.) — keep the JSON to round-trip nested fields.
- The view DDL rewrite uses `iceberg_catalog1.` / `iceberg_catalog2.`
  literal strings; if your session uses different catalog names, edit
  `view_workflow.SRC_CATALOG` / `TGT_CATALOG`.
