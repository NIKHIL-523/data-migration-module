# Changelog

All notable changes to this project will be documented here.

## 0.2.0 — 2026-05-18

Generic environment naming. The codebase is no longer biased toward the
QA → prod migration example. **Breaking changes** below; old session
bundles are not compatible with this version.

### Added
- `SOURCE_ENV` / `TARGET_ENV` display-label constants in notebook Cell
  2. Defaults are `"qa"` / `"prod"` — set to any pair (e.g.
  `"dev"` / `"staging"`) and the notebook's print labels follow.

### Changed (breaking)
- `connection` dict keys: `qa_hms_uri` → `source_hms_uri`,
  `prod_hms_uri` → `target_hms_uri`, `qa_warehouse` →
  `source_warehouse`, `prod_warehouse` → `target_warehouse`.
- `table_state.csv` columns: `qa_table` → `source_table`, `prod_table`
  → `target_table`, `qa_count` → `source_count`, `prod_count` →
  `target_count`, `prod_count_total` → `target_count_total`,
  `partition_qa` → `partition_source`, `partition_prod` →
  `partition_target`.
- `view_state.csv` columns: `qa_view` / `prod_view` → `source_view_fqn`
  / `target_view_fqn` (suffixed to disambiguate from the user-input
  `target_view` spec key which is a view name, not an FQN).
- `table_properties.diff_qa_vs_prod()` → `diff_source_vs_target()`.
  Function args `qa_fqn` / `prod_fqn` → `source_fqn` / `target_fqn`.
- `bundle_writer._prod_table_from_ds_row` →
  `_target_table_from_ds_row`. `migrate.py` `prod_table_from_row` →
  `target_table_from_row`.
- Property-sync `df_plan` action label `"skip (qa missing)"` →
  `"skip (source missing)"`.

### Migration guide
1. In Cell 2, set `SOURCE_ENV` and `TARGET_ENV` to your env names.
2. Rename your `connection` keys per the list above.
3. Existing session bundles need a fresh re-bundle — the CSV column
   schema changed. (No automated upgrade path; the old QA→prod columns
   are removed cleanly.)

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
  + source → target sync with reserved-key filtering.

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
