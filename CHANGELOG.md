# Changelog

All notable changes to this project will be documented here.

## 0.1.0 — 2026-05-18

Initial release.

### Notebook
- 38-cell paste-driven workflow (`workflow_CELLS.md`) covering discovery
  → bundle → table migrate → table validate → view migrate → view
  validate → property sync → backup rename.
- Compiled `workflow.ipynb` (39 cells incl. intro) for direct
  JupyterHub use.
- Single edit point: `CUTOFF_TS` + `CUTOFF` in Cell 2, referenced
  downstream.
- Per-cell markdown intros (**What / Run when / Gotcha**) for every
  section.

### Helpers (8 Python modules)
- `spark_session.py` — dual-catalog SparkSession with kg-validation
  tuning (AQE, G1GC, Kryo, off-heap, shuffle.partitions=2000).
- `catalog_traversal.py` — `list_schemas` / `summarize_tables` /
  `summarize_views` + Py4J partition-spec derivation (singular →
  plural transform mapping for the migrate JAR).
- `build_datasources.py` — normalise user selections into
  `datasources.json` rows.
- `template_builder.py` — render SparkApplication CR template with
  IcebergMigrate sparkConf, K8s affinity, OIDC, Prometheus metrics.
- `bundle_writer.py` — self-contained `sessions/<name>/` bundle (5
  files seeded).
- `session_state.py` — pandas-flavoured CSV state helpers
  (`load_state`, `save_state`, `upsert_row`, `is_pending`).
- `view_workflow.py` — in-Hub view migrate + validate with the 6-step
  DDL rewrite. Supports `dry_run=True` (preview), `per_column=True`
  (per-column mismatch counts), `status_label()` formatter.
- `table_properties.py` — TBLPROPERTIES diff (SQL view vs Iceberg view)
  + QA → prod sync with reserved-key filtering.

### Ops VM driver
- `migrate_template/migrate.py` — stdlib-only (no pandas). Polls
  SparkApplication CR every 30s until COMPLETED or FAILED; skip-success
  default ON. CLI: `--dry-run`, `--table`, `--rerun-all`,
  `--list-pending`, `--continue-on-failure`, `--poll-interval`,
  `--poll-timeout`.

### Safety
- Property apply (Cell 30) — `apply = False` default; flip to execute.
- Backup rename (Cell 36) — `confirm = False` default; flip to execute.
- Single-quote escaping inline before ALTER TABLE literals.

### State
- CSV per session, click-editable from JupyterLab's CSV viewer.
- `table_state.csv` columns: `table_key` (PK), `qa_table`,
  `prod_table`, `apply_*` (5 fields), `validation_*` (4 fields),
  `qa_count`, `prod_count`, `prod_count_total`, `partition_qa`,
  `partition_prod`, `partition_match`.
- `view_state.csv` columns: `view_suffix` (PK), `qa_view`, `prod_view`,
  `migrate_*` (3 fields), `validation_*` (5 fields).
