# DATA MIGRATION MODULE

End-to-end toolkit for migrating Iceberg tables and views between two
Hive Metastore catalogs (QA → prod), driven from a Jupyter notebook.

The tooling is generic — point it at any two catalogs by editing the
`connection` dict in Cell 2 of the notebook. The 28 example
`selections` (Cell 8) and the kg-style view rewrites are TP-specific
defaults that are easy to swap out.

## What's inside

```
data migration/                     # everything lives here
├── workflow.ipynb                  # the canonical notebook (38 cells)
├── workflow_CELLS.md               # diffable source of truth for the notebook
├── CONTEXT.md                      # design notes
├── WORKFLOW_DIAGRAM.md             # mermaid diagram of the end-to-end flow
├── spark_session.py                # dual-catalog SparkSession builder
├── catalog_traversal.py            # schema/table/view discovery + partition spec
├── build_datasources.py            # user selections -> datasources.json
├── template_builder.py             # SparkApplication CR template renderer
├── bundle_writer.py                # self-contained session bundle writer
├── session_state.py                # CSV-based state helpers (notebook side)
├── view_workflow.py                # in-Hub view migrate + validate
├── table_properties.py             # TBLPROPERTIES diff + QA->prod sync
└── migrate_template/
    └── migrate.py                  # stdlib-only ops-VM driver
```

## How to use

1. **Clone**:
   ```bash
   git clone https://github.com/NIKHIL-523/data-migration-module.git
   cd data-migration-module
   ```

2. **Open the notebook** in JupyterHub (or anywhere with a PySpark
   kernel): `data migration/workflow.ipynb`.

3. **Edit Cell 2** (`connection`, `sparkapp`, `CUTOFF_TS`) for your
   environment.

4. **Restart the kernel**, then run cells in order. The flow is:
   - Discovery (Cells 4 / 6) — list schemas + table summary.
   - Plan + bundle (Cells 8 / 10) — pick tables, write a session bundle.
   - Tables (ops VM) — `python migrate.py` inside the bundle.
   - Validate tables (Cell 12) — count + partition spec, back in Hub.
   - Views (Cells 16–24) — define specs, pilot, batch migrate + validate.
   - Property sync (Cells 26–32) — copy specific TBLPROPERTIES.
   - Backup rename (Cells 34–38) — move a prod schema aside.

## Design notes

State for each session lives in CSV files (`table_state.csv`,
`view_state.csv`) inside `data migration/sessions/<name>/`.
JupyterLab's CSV viewer click-edits any cell — wipe `apply_status` to
force a re-run; no JSON editing needed. The ops-VM driver uses only
stdlib `csv` so it works without pandas.

See `data migration/CONTEXT.md` and `data migration/WORKFLOW_DIAGRAM.md`
for more.
