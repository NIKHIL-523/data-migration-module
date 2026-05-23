# Iceberg migration — notebook

Paste-driven canonical notebook for the source → target Iceberg table +
view migration workflow. One fenced code or markdown block per cell. Cells
alternate **markdown intro → code**, so a 19-step workflow lands as 38
cells in `workflow.ipynb`.

---

## Cell 1 — markdown

### Setup

**What it does.** Bootstraps the kernel: pip-installs pandas, adds the
helpers folder to `sys.path`, imports every module used downstream, then
builds the dual-catalog SparkSession.

**Run when.** Fresh kernel. Always restart before re-running — Spark's
`getOrCreate()` returns the existing session if alive, and the tuning
configs below are silently dropped on a warm kernel.

**Gotcha.** Edit `connection`, `sparkapp`, and `CUTOFF_TS` to match your
environment. Empty `extra_conf` = local-mode reads only; populate the
commented K8s entries for cluster mode.

---

## Cell 2 — code

```python
%pip install --quiet pandas

import sys
from pathlib import Path

# Add the helpers folder to sys.path. The notebook lives next to the .py
# modules in this repo's `data migration/` folder, so the current dir is
# normally already correct; the candidate list covers running from one
# level up (the repo root) or a `data migration/` subdir.
HERE = Path.cwd()
candidates = [HERE, HERE / "data migration", HERE.parent / "data migration"]
ROOT = next((p for p in candidates if (p / "spark_session.py").is_file()), HERE)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from IPython.display import display

from spark_session     import build_spark_session
from catalog_traversal import (
    list_schemas, summarize_tables, summarize_views,
    derive_partition_columns,
)
from build_datasources import build_datasources_rows, preview_rows
from template_builder  import build_template
from bundle_writer     import write_session_bundle, show_bundle_links
from session_state     import (
    load_state, save_state, upsert_row, is_pending, utc_now_iso,
)
from view_workflow     import migrate_views, validate_views, status_label
from table_properties  import (
    properties_via_sql, properties_via_iceberg,
    compare_properties, diff_source_vs_target,
    plan_property_sync, apply_property_plan, sync_batch, RESERVED_KEYS,
)

# ---------------------------------------------------------------------------
# Environment labels (display only). The code paths are direction-agnostic;
# these strings flow into print labels so the notebook reads naturally for
# your specific migration (e.g. "dev → staging" or "staging → prod").
# Catalogs stay as iceberg_catalog1 (source) and iceberg_catalog2 (target);
# those are arbitrary Spark identifiers, not env labels.
# ---------------------------------------------------------------------------
SOURCE_ENV = "qa"      # rename to "dev"/"staging"/etc. as needed
TARGET_ENV = "prod"

# ---------------------------------------------------------------------------
# Cutoff: single edit point for table migration filter (Cell 8), table
# validation (Cell 12), and view validation (Cell 20). Downstream cells
# reference these — edit only here.
# ---------------------------------------------------------------------------
CUTOFF_TS = "2026-05-12 23:59:59.999"
CUTOFF    = f"updated_at_ts<='{CUTOFF_TS}'"

# ---------------------------------------------------------------------------
# Connection details
# ---------------------------------------------------------------------------
connection = {
    "source_hms_uri":      "thrift://172.27.5.25:9083",
    "target_hms_uri":    "thrift://hivemetastore.prod.svc.cluster.local:9083",
    "source_warehouse":    "abfs://tp-qa-datalake@azpcinpneupraist02.dfs.core.windows.net/qa/iceberg/",
    "target_warehouse":  "abfs://tp-prod-datalake@azpcineupraist01.dfs.core.windows.net/prod/iceberg/",
    "default_fs":      "abfs://tp-prod-datalake@azpcineupraist01.dfs.core.windows.net/prod",
    "event_log_dir":   "abfs://tp-prod-logs@azpcineupraist01.dfs.core.windows.net/spark-history/",
    "azure_tenant":    "638fcbaf-ba4c-43e1-adae-5475c970fe10",
    "azure_client_id": "7b01d4d3-f76e-4215-b9cb-3f6814c7328d",
}

sparkapp = {
    "driver_cores":           "4",
    "driver_memory":          "20g",
    "driver_max_result_size": "4g",
    "executor_cores":         "8",
    "executor_memory":        "40g",
    "executor_instances":     "25",
    "executor_core_request":  "500m",
    "oidc_url": "https://qa.sds.ontp.app/realms/prod/protocol/openid-connect/token",
}

# K8s wiring for the notebook session — leave empty for local-mode reads.
extra_conf: dict[str, str] = {
    # "spark.master": "k8s://https://kubernetes.default.svc",
    # "spark.kubernetes.namespace": "<your-ns>",
    # "spark.kubernetes.authenticate.driver.serviceAccountName": "sds-jupyterhub-sa",
    # "spark.kubernetes.container.image": "<image>",
    # "spark.kubernetes.executor.podTemplateFile": "<abfs://...yaml>",
}

spark = build_spark_session(
    connection=connection,
    sparkapp=sparkapp,
    app_name="iceberg-migration-workflow",
    extra_conf=extra_conf or None,
)
print(f"Spark version : {spark.version}")
print(f"App ID        : {spark.sparkContext.applicationId}")
print(f"Master        : {spark.sparkContext.master}")
print(f"Envs          : {SOURCE_ENV.upper()} → {TARGET_ENV.upper()}")
print(f"CUTOFF_TS     : {CUTOFF_TS}")
spark
```

---

## Cell 3 — markdown

### Discovery — list source-catalog schemas

**What it does.** Prints every top-level namespace in the source Iceberg
catalog (`iceberg_catalog1`).

**Run when.** You want to confirm the schema name(s) you'll migrate
from. Read-only — safe to re-run anytime.

**Gotcha.** Schema visibility depends on HMS permissions; if a schema
you expect is missing, check with platform.

---

## Cell 4 — code

```python
source_schemas = list_schemas(spark, "iceberg_catalog1")
print(f"{len(source_schemas)} {SOURCE_ENV.upper()} schema(s):")
for s in source_schemas:
    print(f"  - {s}")
```

---

## Cell 5 — markdown

### Discovery — table summary + per-table COUNT(*)

**What it does.** Lists every table under `selected_schemas`, derives
the partition spec (Iceberg internal → migrate.py format), and runs a
`COUNT(*)` per table (optionally filtered by `where_sql`).

**Run when.** Cell 4 has shown you the namespaces. Edit
`selected_schemas` to whichever ones you want to inspect.

**Gotcha.** Unfiltered counts are free (Iceberg manifest stats); a
filtered count may scan partitions and be slow on huge tables. Set
`where_sql = CUTOFF` (from Cell 2) for cutoff-bounded counts.

---

## Cell 6 — code

```python
selected_schemas = [
    "lookup_v2",
    "kg",
    "kg_fragment",
    "kg_olap_v3",
    "kg_fragment_olap_v3",
    "kg_publish_final",
]

# Optional filter applied to the COUNT(*) query. Leave empty for unfiltered,
# or set to CUTOFF (from Cell 2) for cutoff-bounded counts.
where_sql = ""

tables_df = summarize_tables(spark, "iceberg_catalog1", schemas=selected_schemas)
print(f"{len(tables_df)} table(s) across {len(selected_schemas)} schema(s)")

# Add a source_count column. Iceberg answers unfiltered COUNT(*) from manifest
# stats (fast); a filtered count may scan partitions.
counts, errors = [], []
for _, r in tables_df.iterrows():
    fqn = f"iceberg_catalog1.{r['schema']}.{r['table']}"
    q = f"SELECT COUNT(*) AS c FROM {fqn}"
    if where_sql:
        q += f" WHERE {where_sql}"
    try:
        counts.append(int(spark.sql(q).collect()[0]["c"]))
        errors.append("")
    except Exception as e:
        counts.append(None)
        errors.append(str(e)[:120])
tables_df = tables_df.copy()
tables_df["source_count"]    = counts
tables_df["count_error"] = errors

with pd.option_context("display.max_rows", None, "display.max_colwidth", 120, "display.width", 240):
    display(tables_df)
```

---

## Cell 7 — markdown

### Define migration selections + build datasources rows

**What it does.** Declares the migration plan (28 example rows: 14 in
`kg_olap_v3 → kg_olap`, 14 in `kg_publish_final → kg_publish`), then
normalises into the `datasources.json` shape consumed by the ops-VM
driver and previews the result.

**Run when.** You've reviewed Cell 6's table summary and know exactly
which tables to migrate.

**Gotcha.** `k8s_name` must be unique per row (RFC-1123). Cross-batch
collisions — same bare table name in two source schemas going to two
different target schemas — require explicit suffixes (here `-olap` and
`-pub`). Partition columns are auto-derived via Py4J when omitted.

---

## Cell 8 — code

```python
selections = [
    # ----- kg_olap_v3 -> kg_olap (14 tables, suffix -olap) -----
    {"src_schema": "kg_olap_v3", "src_table": "sds_em__rel__finding_associated_with_host__publish",
     "tgt_schema": "kg_olap", "k8s_name": "sdsemrelfindingassociatedwithhostpublish-olap",
     "filter_expression": CUTOFF},
    {"src_schema": "kg_olap_v3", "src_table": "sds_em__rel__finding_associated_with_person__publish",
     "tgt_schema": "kg_olap", "k8s_name": "sdsemrelfindingassociatedwithpersonpublish-olap",
     "filter_expression": CUTOFF},
    {"src_schema": "kg_olap_v3", "src_table": "sds_ei__rel__vulnerability_finding_on_host__publish",
     "tgt_schema": "kg_olap", "k8s_name": "sdseirelvulnerabilityfindingonhostpublish-olap",
     "filter_expression": CUTOFF},
    {"src_schema": "kg_olap_v3", "src_table": "sds_ei__vulnerability__publish",
     "tgt_schema": "kg_olap", "k8s_name": "sdseivulnerabilitypublish-olap",
     "filter_expression": CUTOFF},
    {"src_schema": "kg_olap_v3", "src_table": "sds_em__rel__finding_associated_with_vulnerability__publish",
     "tgt_schema": "kg_olap", "k8s_name": "sdsemrelfindingassociatedwithvulnerabilitypublish-olap",
     "filter_expression": CUTOFF},
    {"src_schema": "kg_olap_v3", "src_table": "sds_ei__person__publish",
     "tgt_schema": "kg_olap", "k8s_name": "sdseipersonpublish-olap",
     "filter_expression": CUTOFF},
    {"src_schema": "kg_olap_v3", "src_table": "sds_ei__identity__publish",
     "tgt_schema": "kg_olap", "k8s_name": "sdseiidentitypublish-olap",
     "filter_expression": CUTOFF},
    {"src_schema": "kg_olap_v3", "src_table": "sds_em__assessment__publish",
     "tgt_schema": "kg_olap", "k8s_name": "sdsemassessmentpublish-olap",
     "filter_expression": CUTOFF},
    {"src_schema": "kg_olap_v3", "src_table": "sds_em__rel__finding_associated_with_identity__publish",
     "tgt_schema": "kg_olap", "k8s_name": "sdsemrelfindingassociatedwithidentitypublish-olap",
     "filter_expression": CUTOFF},
    {"src_schema": "kg_olap_v3", "src_table": "sds_ei__rel__person_has_identity__publish",
     "tgt_schema": "kg_olap", "k8s_name": "sdseirelpersonhasidentitypublish-olap",
     "filter_expression": CUTOFF},
    {"src_schema": "kg_olap_v3", "src_table": "sds_ei__rel__person_owns_host__publish",
     "tgt_schema": "kg_olap", "k8s_name": "sdseirelpersonownshostpublish-olap",
     "filter_expression": CUTOFF},
    {"src_schema": "kg_olap_v3", "src_table": "sds_em__finding__publish",
     "tgt_schema": "kg_olap", "k8s_name": "sdsemfindingpublish-olap",
     "filter_expression": CUTOFF},
    {"src_schema": "kg_olap_v3", "src_table": "sds_ei__host__publish",
     "tgt_schema": "kg_olap", "k8s_name": "sdseihostpublish-olap",
     "filter_expression": CUTOFF},
    {"src_schema": "kg_olap_v3", "src_table": "sds_em__rel__finding_associated_with_assessment__publish",
     "tgt_schema": "kg_olap", "k8s_name": "sdsemrelfindingassociatedwithassessmentpublish-olap",
     "filter_expression": CUTOFF},

    # ----- kg_publish_final -> kg_publish (14 tables, suffix -pub) -----
    {"src_schema": "kg_publish_final", "src_table": "sds_ei__rel__person_owns_host__publish",
     "tgt_schema": "kg_publish", "k8s_name": "sdseirelpersonownshostpublish-pub",
     "filter_expression": CUTOFF},
    {"src_schema": "kg_publish_final", "src_table": "sds_em__assessment__publish",
     "tgt_schema": "kg_publish", "k8s_name": "sdsemassessmentpublish-pub",
     "filter_expression": CUTOFF},
    {"src_schema": "kg_publish_final", "src_table": "sds_em__rel__finding_associated_with_person__publish",
     "tgt_schema": "kg_publish", "k8s_name": "sdsemrelfindingassociatedwithpersonpublish-pub",
     "filter_expression": CUTOFF},
    {"src_schema": "kg_publish_final", "src_table": "sds_ei__rel__person_has_identity__publish",
     "tgt_schema": "kg_publish", "k8s_name": "sdseirelpersonhasidentitypublish-pub",
     "filter_expression": CUTOFF},
    {"src_schema": "kg_publish_final", "src_table": "sds_em__rel__finding_associated_with_identity__publish",
     "tgt_schema": "kg_publish", "k8s_name": "sdsemrelfindingassociatedwithidentitypublish-pub",
     "filter_expression": CUTOFF},
    {"src_schema": "kg_publish_final", "src_table": "sds_ei__vulnerability__publish",
     "tgt_schema": "kg_publish", "k8s_name": "sdseivulnerabilitypublish-pub",
     "filter_expression": CUTOFF},
    {"src_schema": "kg_publish_final", "src_table": "sds_ei__person__publish",
     "tgt_schema": "kg_publish", "k8s_name": "sdseipersonpublish-pub",
     "filter_expression": CUTOFF},
    {"src_schema": "kg_publish_final", "src_table": "sds_ei__identity__publish",
     "tgt_schema": "kg_publish", "k8s_name": "sdseiidentitypublish-pub",
     "filter_expression": CUTOFF},
    {"src_schema": "kg_publish_final", "src_table": "sds_em__rel__finding_associated_with_vulnerability__publish",
     "tgt_schema": "kg_publish", "k8s_name": "sdsemrelfindingassociatedwithvulnerabilitypublish-pub",
     "filter_expression": CUTOFF},
    {"src_schema": "kg_publish_final", "src_table": "sds_ei__host__publish",
     "tgt_schema": "kg_publish", "k8s_name": "sdseihostpublish-pub",
     "filter_expression": CUTOFF},
    {"src_schema": "kg_publish_final", "src_table": "sds_em__rel__finding_associated_with_assessment__publish",
     "tgt_schema": "kg_publish", "k8s_name": "sdsemrelfindingassociatedwithassessmentpublish-pub",
     "filter_expression": CUTOFF},
    {"src_schema": "kg_publish_final", "src_table": "sds_em__rel__finding_associated_with_host__publish",
     "tgt_schema": "kg_publish", "k8s_name": "sdsemrelfindingassociatedwithhostpublish-pub",
     "filter_expression": CUTOFF},
    {"src_schema": "kg_publish_final", "src_table": "sds_ei__rel__vulnerability_finding_on_host__publish",
     "tgt_schema": "kg_publish", "k8s_name": "sdseirelvulnerabilityfindingonhostpublish-pub",
     "filter_expression": CUTOFF},
    {"src_schema": "kg_publish_final", "src_table": "sds_em__finding__publish",
     "tgt_schema": "kg_publish", "k8s_name": "sdsemfindingpublish-pub",
     "filter_expression": CUTOFF},
]

datasources_rows = build_datasources_rows(
    spark, qa_catalog="iceberg_catalog1",
    selections=selections, auto_derive_partitions=True,
)
print(f"\n{len(datasources_rows)} datasources row(s) built.\n")
preview_rows(datasources_rows)
```

---

## Cell 9 — markdown

### Build template + write session bundle

**What it does.** Renders the SparkApplication CR template and writes a
self-contained `sessions/<session_name>/` folder containing
`migrate.py`, `datasources.json`, `template.json`, seeded
`table_state.csv` + `view_state.csv`, and a per-bundle `README.md`.

**Run when.** Cell 8 has produced `datasources_rows`.

**Gotcha.** `overwrite=False` raises `FileExistsError` if the bundle
folder already exists — set to True to clobber. Download the folder
from JupyterLab's file browser and run `python migrate.py` on the ops
VM.

---

## Cell 10 — code

```python
session_name = "kg_olap_publish__final"

main_application_file = (
    "abfs://tp-prod-apps@azpcineupraist01.dfs.core.windows.net/"
    "prod/sds/data-analytics/lib/latest/sds-pe-utility-jobs-3.5.5_2.13.jar"
)

template = build_template(
    connection=connection,           # from Cell 2
    sparkapp=sparkapp,               # from Cell 2
    main_application_file=main_application_file,
    namespace="prod",
)

bundle = write_session_bundle(
    session_name,
    datasources_rows=datasources_rows,   # from Cell 8
    template=template,
    overwrite=False,                     # set True to clobber an existing folder
)
print(f"Bundle: {bundle}")
for f in ("migrate.py", "datasources.json", "template.json",
          "table_state.csv", "view_state.csv", "README.md"):
    print(f"  - {bundle / f}")

show_bundle_links(bundle)
```

---

## Cell 11 — markdown

### Table validation — counts + partition specs

**What it does.** Post-migration sanity check. Loads `table_state.csv`,
for each table runs filtered + unfiltered `COUNT(*)` on the target side,
derives partition spec on both sides, writes verdict back to the CSV.

**Run when.** The ops VM has finished `python migrate.py` for the
bundle's tables.

**Gotcha.** `reset_validation = True` wipes prior validation columns on
every run. Set to False to keep skip-success behaviour. The per-row
filter is loaded from the bundle's `datasources.json` (each row's
`filterExpression`) so chunked / per-schema filters validate correctly;
`fallback_where` is only used for rows missing a filter or when
`datasources.json` is absent. The `target_count_total` column is the
UN-filtered target count — a sanity check that the target isn't carrying
rows beyond the cutoff.

---

## Cell 12 — code

```python
import json
from bundle_writer import k8s_name_for_datasource_row

bundle = Path("sessions/kg_olap_publish__final")    # adjust if path differs

# Per-row filter is read from datasources.json (each row's filterExpression),
# keyed by k8s_name. Use fallback_where for rows missing a filter, or set
# fallback_where = "" to leave them unfiltered.
fallback_where = CUTOFF

# Wipe prior validation state so the loop re-checks everything.
# False = keep skip-success behaviour.
reset_validation = True

state_df = load_state(bundle, kind="table")

# Load per-row filter map from datasources.json (keyed by the same k8s_name
# the state CSV stores). Missing file -> empty map -> all rows use fallback.
ds_path = bundle / "datasources.json"
filter_by_k8s: dict[str, str] = {}
if ds_path.exists():
    ds_rows = json.loads(ds_path.read_text())
    for row in ds_rows:
        filter_by_k8s[k8s_name_for_datasource_row(row)] = (row.get("filterExpression") or "")
    print(f"loaded {len(filter_by_k8s)} per-row filter(s) from {ds_path.name}")
else:
    print(f"WARN no {ds_path.name} found -- every row will use fallback_where")

if reset_validation:
    for col in ("validation_status", "validation_at",
                "source_count", "target_count", "target_count_total",
                "partition_source", "partition_target", "partition_match",
                "validation_error"):
        if col in state_df.columns:
            state_df[col] = ""
    save_state(state_df, bundle, kind="table")
    print(f"reset: cleared validation state on {len(state_df)} row(s)")

print(f"state : {bundle / 'table_state.csv'}")
print(f"rows  : {len(state_df)}")
print(f"fallback_where : {fallback_where or '<unfiltered>'}")
print("-" * 60)

for _, r in state_df.iterrows():
    key = r["table_key"]
    source, target = r["source_table"], r["target_table"]
    k8s = (r.get("k8s_name") or "").strip()

    if not is_pending(state_df, "table_key", key, "validation_status"):
        print(f"SKIP   {key}  (validation_status already ok)")
        continue

    # Per-row filter: prefer datasources.json's filterExpression for this
    # row's k8s_name; fall back to fallback_where (or '' if also missing).
    where_sql = filter_by_k8s.get(k8s, fallback_where)

    s_fqn = f"iceberg_catalog1.{source}"
    t_fqn = f"iceberg_catalog2.{target}"
    fields = {"validation_at": utc_now_iso()}
    try:
        # Filtered counts (BOTH sides under the same per-row filter)
        q_source   = f"SELECT COUNT(*) c FROM {s_fqn}" + (f" WHERE {where_sql}" if where_sql else "")
        q_target = f"SELECT COUNT(*) c FROM {t_fqn}" + (f" WHERE {where_sql}" if where_sql else "")
        source_c   = int(spark.sql(q_source).collect()[0]["c"])
        target_c = int(spark.sql(q_target).collect()[0]["c"])

        # Unfiltered target total — sanity check that the target isn't
        # carrying rows beyond the cutoff. Free on Iceberg (manifest stats).
        target_total = int(spark.sql(f"SELECT COUNT(*) c FROM {t_fqn}").collect()[0]["c"])

        # Partition specs
        p_source   = derive_partition_columns(spark, s_fqn)
        p_target = derive_partition_columns(spark, t_fqn)
        p_match = (p_source == p_target)

        all_ok = (source_c == target_c) and p_match
        fields.update({
            "source_count":          source_c,
            "target_count":        target_c,
            "target_count_total":  target_total,
            "partition_source":      p_source,
            "partition_target":    p_target,
            "partition_match":   "true" if p_match else "false",
            "validation_status": "ok" if all_ok else "mismatch",
            "validation_error":  "",
        })
        label = "OK   " if all_ok else "DIFF "
        extra = ""
        if target_total != target_c:
            extra = f"  (target_total={target_total:,}, +{target_total - target_c:,} beyond filter)"
        print(f"{label} {key}  {SOURCE_ENV}={source_c:,}  {TARGET_ENV}={target_c:,}  partition={'match' if p_match else 'DIFF'}{extra}")
        if not p_match:
            print(f"       {SOURCE_ENV:<6} spec: {p_source}")
            print(f"       {TARGET_ENV:<6} spec: {p_target}")
    except Exception as e:
        fields.update({"validation_status": "error", "validation_error": str(e)[:200]})
        print(f"ERR   {key}: {e}")

    state_df = upsert_row(state_df, "table_key", key, fields)
    save_state(state_df, bundle, kind="table")

# Display: validation-relevant columns first; hide migration columns from view.
DISPLAY_COLS = [
    "table_key", "source_table", "target_table",
    "source_count", "target_count", "target_count_total",
    "partition_source", "partition_target", "partition_match",
    "validation_status", "validation_at", "validation_error",
]
view = state_df[[c for c in DISPLAY_COLS if c in state_df.columns]].copy()
# Big numbers in nullable Int64 so they don't render in scientific notation.
for c in ("source_count", "target_count", "target_count_total"):
    if c in view.columns:
        view[c] = pd.to_numeric(view[c], errors="coerce").astype("Int64")
with pd.option_context("display.max_rows", None, "display.max_colwidth", 120, "display.width", 240):
    display(view)

print(f"\nsaved -> {bundle / 'table_state.csv'}")
print("(apply_* columns hidden from view; still in the CSV.)")
```

---

## Cell 13 — markdown

### Manual ad-hoc table validation (optional, stateless)

**What it does.** Stateless `COUNT(*)` + partition spec diff for
arbitrary `(source, target)` pairs. No writeback to `table_state.csv`.

**Run when.** Cross-checking a table outside the current bundle, or
sanity-checking one row.

**Gotcha.** Results only live in this cell's output — nothing is
persisted. Use Cell 12 if you want the verdict in the state CSV.

---

## Cell 14 — code

```python
# Pairs of (source_full, prod_full). Edit freely; no state is written.
pairs = [
    ("kg_publish_final.sds_em__finding__publish", "kg_publish.sds_em__finding__publish"),
    # ("kg.sds_ei__host__bms_client_extract__pid__srdm_inv", "kg.sds_ei__host__bms_client_extract__pid__srdm_inv"),
]

where_sql = ""   # "" = unfiltered, or e.g. CUTOFF (from Cell 2)

rows = []
for source, target in pairs:
    s_fqn = f"iceberg_catalog1.{source}"
    t_fqn = f"iceberg_catalog2.{target}"
    row = {"source_table": source, "target_table": target,
           "source_count": None, "target_count": None, "count_match": None,
           "partition_source": None, "partition_target": None, "partition_match": None,
           "error": ""}
    try:
        q_source   = f"SELECT COUNT(*) c FROM {s_fqn}" + (f" WHERE {where_sql}" if where_sql else "")
        q_target = f"SELECT COUNT(*) c FROM {t_fqn}" + (f" WHERE {where_sql}" if where_sql else "")
        row["source_count"]    = int(spark.sql(q_source).collect()[0]["c"])
        row["target_count"]  = int(spark.sql(q_target).collect()[0]["c"])
        row["count_match"] = row["source_count"] == row["target_count"]
        row["partition_source"]    = derive_partition_columns(spark, s_fqn)
        row["partition_target"]  = derive_partition_columns(spark, t_fqn)
        row["partition_match"] = row["partition_source"] == row["partition_target"]
    except Exception as e:
        row["error"] = str(e)[:200]
    print(f"{source}  vs  {target}  ->  count={row['count_match']}  partition={row['partition_match']}  {row['error'] or ''}")
    rows.append(row)

df = pd.DataFrame(rows)
# Nullable Int64 so big counts don't render in scientific notation.
for c in ("source_count", "target_count"):
    df[c] = df[c].astype("Int64")
with pd.option_context("display.max_rows", None, "display.max_colwidth", 120, "display.width", 240):
    display(df)
```

---

## Cell 15 — markdown

### Define view specs + persist to bundle

**What it does.** Declares which views to migrate and writes
`view_specs.json` next to the table-state CSV in the same bundle.

**Run when.** Tables have migrated successfully and you want to layer
views on top.

**Gotcha.** Only `view` is required per spec. Optional overrides
(`target_db`, `base_table`, `target_location`, `filter_expression`,
`join_key`, `cast_to_string`) default via
`view_workflow._resolve_spec` — override only when the default is
wrong for that view.

---

## Cell 16 — code

```python
import json

bundle = Path("sessions/kg_olap_publish__final")     # adjust if path differs

view_specs = [
    {"view": "kg.sds_ei__host__bitsight__company_assets__asset_id"},
    # add more here:
    # {"view": "kg.sds_ei__identity__ms_azure_ad_users__user_principal_name"},
    # {"view": "kg.sds_ei__host__bms_client_extract__pid"},
]

(bundle / "view_specs.json").write_text(json.dumps(view_specs, indent=2) + "\n")
print(f"{len(view_specs)} view spec(s) -> {bundle / 'view_specs.json'}")
```

---

## Cell 17 — markdown

### Pilot single-view migrate (dry-run → optional apply)

**What it does.** Picks one spec, prints the rewritten DDL via
`dry_run=True`, optionally applies on second run.

**Run when.** Piloting view migration before running the full batch.
Always run with `apply = False` first to read the DDL.

**Gotcha.** When `apply = True` it writes one row to `view_state.csv`.
The batch cell (Cell 22) will then skip that view on its next run
(skip-success). Edit the cell or pass `skip_if_ok=False` to redo.

---

## Cell 18 — code

```python
# Pick one spec to pilot (first match by substring; tweak the predicate).
pilot = next(s for s in view_specs if "bitsight" in s["view"])
print(f"Pilot view: {pilot['view']}\n")

# Step 1: dry-run — print the rewritten DDL, do NOT execute, no state writes.
print("=== DRY-RUN ===")
migrate_views(spark, bundle, [pilot], dry_run=True, skip_if_ok=False, verbose=True)

# Step 2: flip apply=True once the DDL looks right, then re-run this cell.
# Applies the pilot only; view_state.csv gets one row.
apply = False
if apply:
    print("\n=== APPLY ===")
    migrate_views(spark, bundle, [pilot], skip_if_ok=False)
else:
    print("\nApply skipped. Set `apply = True` and re-run to execute.")
```

---

## Cell 19 — markdown

### Manual single-view row-level diff (optional, stateless)

**What it does.** Full-outer joins source vs target for one view on
`JOIN_KEY`, shows mismatched rows with which columns differ, and emits
per-column mismatch counts. No state writeback.

**Run when.** Debugging WHY a view doesn't match (after Cell 24 has
flagged it as `mismatch`).

**Gotcha.** Slow on large views. The cell hardcodes `db`/`src_view`/
`filter_ts` — edit them inline. `CAST_TO_STRING` is the set of columns
to cast to string before comparing (works around precision differences
on long-precision attribute fields).

---

## Cell 20 — code

```python
from pyspark.sql import functions as F

# Pick a view to inspect. Defaults match the Cell 18 pilot.
db        = "kg"
src_view  = "sds_ei__host__bitsight__company_assets__asset_id"
tgt_view  = src_view
filter_ts = CUTOFF_TS                                # from Cell 2
where_sql = f"updated_at_ts='{filter_ts}' AND kg_content_type='data'"

JOIN_KEY       = "p_id"
CAST_TO_STRING = {"last_updated_attrs"}

source_fqn   = f"iceberg_catalog1.{db}.{src_view}"
target_fqn = f"iceberg_catalog2.{db}.{tgt_view}"

old_df = spark.table(source_fqn).where(where_sql)
new_df = spark.table(target_fqn).where(where_sql)
compare_cols = [c for c in old_df.columns if c != JOIN_KEY]

def _rename(prefix, df):
    return df.select(
        F.col(JOIN_KEY),
        *[
            (F.col(c).cast("string").alias(f"{prefix}__{c}")
             if c in CAST_TO_STRING
             else F.col(c).alias(f"{prefix}__{c}"))
            for c in compare_cols
        ],
    )

joined = _rename("old", old_df).join(_rename("new", new_df), on=JOIN_KEY, how="full_outer")

mismatch_conds = [~F.col(f"old__{c}").eqNullSafe(F.col(f"new__{c}")) for c in compare_cols]
any_mismatch = mismatch_conds[0]
for cond in mismatch_conds[1:]:
    any_mismatch = any_mismatch | cond

mismatched = joined.filter(any_mismatch).withColumn(
    "mismatched_columns",
    F.concat_ws(", ", *[
        F.when(~F.col(f"old__{c}").eqNullSafe(F.col(f"new__{c}")), F.lit(c))
        for c in compare_cols
    ]),
)
pair_cols = [col for c in compare_cols for col in (f"old__{c}", f"new__{c}")]
result = mismatched.select(JOIN_KEY, "mismatched_columns", *pair_cols)

print(f"Source ({SOURCE_ENV.upper()}): {source_fqn}")
print(f"Target ({TARGET_ENV.upper()}): {target_fqn}")
print(f"Filter : {where_sql}")
print(f"{SOURCE_ENV.upper():<5} row count : {old_df.count():,}")
print(f"{TARGET_ENV.upper():<5} row count : {new_df.count():,}")
print(f"Mismatched rows : {mismatched.count():,}")
result.show(truncate=False)

print("\n--- Mismatch count per column ---")
for c in compare_cols:
    cnt = joined.filter(~F.col(f"old__{c}").eqNullSafe(F.col(f"new__{c}"))).count()
    if cnt > 0:
        print(f"  {c}: {cnt:,}")
```

---

## Cell 21 — markdown

### Batch view migrate

**What it does.** Applies every spec in `view_specs` to the target via
the 6-step DDL rewrite. Writes each outcome to `view_state.csv`.

**Run when.** You've piloted at least one view (Cell 18) and trust the
rewrite.

**Gotcha.** `skip_if_ok=True` (default) skips rows already
`migrate_status=success`. To force a re-migrate on a row, wipe its
status cell in JupyterLab's CSV viewer or pass `skip_if_ok=False`.

---

## Cell 22 — code

```python
state_df = migrate_views(spark, bundle, view_specs, skip_if_ok=True)

# Display the migrate-relevant columns only.
keep = ("view_suffix", "source_view_fqn", "target_view_fqn",
        "migrate_status", "migrate_at", "migrate_error")
with pd.option_context("display.max_rows", None, "display.max_colwidth", 120, "display.width", 240):
    display(state_df[[c for c in keep if c in state_df.columns]])
```

---

## Cell 23 — markdown

### Batch view validate

**What it does.** For every spec, counts both sides under the spec's
`filter_expression`, full-outer joins on `join_key`, computes total
mismatched rows. Verdict (`ok` / `mismatch` / `error`) lands in
`view_state.csv`.

**Run when.** Cell 22 has finished.

**Gotcha.** `per_column=True` is much slower (N extra COUNTs per view)
but prints which columns drift, inline. Per-column counts are NOT
persisted — only `mismatched_rows` (total) is stored.

---

## Cell 24 — code

```python
state_df = validate_views(spark, bundle, view_specs,
                          skip_if_ok=True, per_column=False)

# Hide migrate_* columns from the displayed slice for readability.
keep = ("view_suffix", "source_view_fqn", "target_view_fqn",
        "validation_status", "validation_at",
        "source_count", "target_count", "mismatched_rows", "validation_error")
# Cast count columns to nullable Int64 so big numbers don't go to scientific.
view = state_df[[c for c in keep if c in state_df.columns]].copy()
for c in ("source_count", "target_count", "mismatched_rows"):
    if c in view.columns:
        view[c] = pd.to_numeric(view[c], errors="coerce").astype("Int64")
with pd.option_context("display.max_rows", None, "display.max_colwidth", 120, "display.width", 240):
    display(view)
```

---

## Cell 25 — markdown

### Property sync — single-table inspect

**What it does.** For one source table + its target counterpart, prints
TBLPROPERTIES via the SQL view, via the Iceberg view, and a side-by-side
diff (`status ∈ {equal, value_changed, missing_on_prod, extra_on_prod}`).

**Run when.** Deciding which property keys are worth copying source → target.

**Gotcha.** Iceberg-managed reserved keys (`current-snapshot-id`,
`snapshot-count`, etc.) appear in SQL output but can't be SET via
`ALTER`. `table_properties.RESERVED_KEYS` enumerates them; the planner
in Cell 28 skips them automatically.

---

## Cell 26 — code

```python
SRC_CATALOG = "iceberg_catalog1"
TGT_CATALOG = "iceberg_catalog2"

TABLE_SUFFIX = "sds_em__assessment__publish"
SOURCE_FQN   = f"{SRC_CATALOG}.kg_olap_v3.{TABLE_SUFFIX}"
TARGET_FQN = f"{TGT_CATALOG}.kg_olap.{TABLE_SUFFIX}"

print(f"{SOURCE_ENV.upper():<6}: {SOURCE_FQN}")
print(f"{TARGET_ENV.upper():<6}: {TARGET_FQN}")

df_source = compare_properties(spark, SOURCE_FQN)
print(f"\n{SOURCE_ENV.upper()} properties: {len(df_source)} keys")
with pd.option_context("display.max_rows", None, "display.max_colwidth", 200):
    display(df_source)

df_target = compare_properties(spark, TARGET_FQN)
print(f"\n{TARGET_ENV.upper()} properties: {len(df_target)} keys")
with pd.option_context("display.max_rows", None, "display.max_colwidth", 200):
    display(df_target)

df_diff = diff_source_vs_target(spark, SOURCE_FQN, TARGET_FQN)
print(f"\nstatus counts: {df_diff['status'].value_counts().to_dict()}")
with pd.option_context("display.max_rows", None, "display.max_colwidth", 200):
    display(df_diff)
```

---

## Cell 27 — markdown

### Property sync — plan

**What it does.** Builds `(source_fqn, target_fqn)` pairs for every
target schema in `SOURCE_SCHEMA_FOR`, then for `KEY` computes per-pair
`action ∈ {skip (source missing), noop, set}`.

**Run when.** After Cell 26 has identified a key worth syncing.

**Gotcha.** Pairs are discovered **from the target side** (`SHOW TABLES
IN iceberg_catalog2.<target_schema>`) — only tables that already exist
in the target are paired. Uncomment additional `SOURCE_SCHEMA_FOR` rows
to cover more batches in the same run.

---

## Cell 28 — code

```python
KEY = "write.parquet.page-size-bytes"

# Map target schema -> source schema (handles the source-side schema rename).
# Uncomment additional batches as needed.
SOURCE_SCHEMA_FOR = {
    "kg_olap":    "kg_olap_v3",
    # "kg_publish": "kg_publish_final",
}
TARGET_SCHEMAS = list(SOURCE_SCHEMA_FOR.keys())

# Discover pairs from the TARGET side — only sync properties for tables
# that actually exist in the target post-migration.
pairs: list[tuple[str, str]] = []
for target_schema in TARGET_SCHEMAS:
    source_schema = SOURCE_SCHEMA_FOR[target_schema]
    for r in spark.sql(f"SHOW TABLES IN iceberg_catalog2.{target_schema}").collect():
        t = r[1]
        pairs.append((
            f"iceberg_catalog1.{source_schema}.{t}",
            f"iceberg_catalog2.{target_schema}.{t}",
        ))
print(f"{len(pairs)} pair(s) across {len(TARGET_SCHEMAS)} target schema(s)")

# Per-pair plan for KEY only.
#   action = "skip (source missing)"  -> nothing to copy from
#            "noop"                    -> already in sync
#            "set"                     -> ALTER TABLE will run in the apply cell
rows = []
for source_fqn, target_fqn in pairs:
    source_val   = properties_via_iceberg(spark, source_fqn).get(KEY)
    prod_val = properties_via_iceberg(spark, target_fqn).get(KEY)
    rows.append({
        "target_fqn":   target_fqn,
        "source_value":   source_val,
        "target_value": prod_val,
        "action":     "skip (source missing)" if source_val is None
                      else "noop"          if source_val == prod_val
                      else "set",
    })

df_plan = pd.DataFrame(rows)
print(f"\nKEY    : {KEY}")
print(f"action counts: {df_plan['action'].value_counts().to_dict()}")
with pd.option_context("display.max_rows", None, "display.max_colwidth", 200):
    display(df_plan)
```

---

## Cell 29 — markdown

### Property sync — apply (safety-gated)

**What it does.** Iterates `df_plan`, runs `ALTER TABLE SET
TBLPROPERTIES` for every row with `action='set'`. Captures per-row
`applied` / `error` and a timestamp.

**Run when.** You've reviewed `df_plan` from Cell 28.

**Gotcha.** `apply = False` default — no-op until you flip it. Single
quotes in `source_value` are escaped inline before the SQL literal is
built; this guards against accidental SQL syntax breaks on property
values with apostrophes.

---

## Cell 30 — code

```python
from datetime import datetime, timezone

# ============================================================
#   STOP. Read df_plan above before flipping `apply = True`.
#   - Only rows with action='set' will be touched.
#   - ALTER TABLE SET TBLPROPERTIES is non-destructive but real.
# ============================================================
apply = False

if not apply:
    print("This cell is a no-op while apply=False. Set apply=True and re-run to execute.")
else:
    results = []
    for _, row in df_plan.iterrows():
        rec = {**row.to_dict(), "applied": False, "error": "",
               "at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        if row["action"] == "set":
            try:
                # Escape single quotes in the value to keep the SQL literal valid.
                qa_v = str(row["source_value"]).replace("'", "''")
                spark.sql(
                    f"ALTER TABLE {row['target_fqn']} "
                    f"SET TBLPROPERTIES ('{KEY}'='{qa_v}')"
                )
                rec["applied"] = True
            except Exception as e:
                rec["error"] = str(e)
        results.append(rec)

    df_apply = pd.DataFrame(results)
    n_set    = int((df_apply["action"] == "set").sum())
    n_done   = int(df_apply["applied"].sum())
    n_errors = int((df_apply["error"] != "").sum())
    print(f"applied: {n_done} / {n_set}")
    print(f"errors : {n_errors}")
    with pd.option_context("display.max_rows", None, "display.max_colwidth", 200):
        display(df_apply[["target_fqn", "source_value", "target_value", "action", "applied", "error"]])
```

---

## Cell 31 — markdown

### Property sync — verify

**What it does.** Re-reads `KEY` from every target table in `pairs`
after apply.

**Run when.** Immediately after Cell 30 with `apply=True`.

**Gotcha.** Only checks `KEY`. If you are syncing multiple keys, run
this cell once per key (or extend it to iterate over a list of keys).

---

## Cell 32 — code

```python
# Re-read KEY on every target table to confirm the sync took effect.
check = []
for _, target_fqn in pairs:
    v = properties_via_iceberg(spark, target_fqn).get(KEY)
    check.append({"target_fqn": target_fqn, KEY: v})

with pd.option_context("display.max_rows", None, "display.max_colwidth", 200):
    display(pd.DataFrame(check))
```

---

## Cell 33 — markdown

### Backup rename — config

**What it does.** Declares which target schemas to back up (table-by-table
rename into `<name><SUFFIX>`) and the version suffix for this run.

**Run when.** You need to preserve the current target state before a
destructive op — e.g. re-migrating from scratch over the existing
tables.

**Gotcha.** Bump `SUFFIX` per run (`_v1` → `_v2` → `_v3`) so old
backups aren't overwritten. The cell prints the new names so you can
verify before running the apply cell.

---

## Cell 34 — code

```python
# Backup-and-destroy: rename each target schema in TARGETS to <name><SUFFIX>.
# Bump SUFFIX per run if you want multiple historical backups
# (e.g. "_v1" -> "_v2" -> "_v3" ...).
CATALOG = "iceberg_catalog2"
TARGETS = ["kg_publish", "kg_olap"]
SUFFIX  = "_v2"

new_names = [f"{t}{SUFFIX}" for t in TARGETS]

print(f"catalog  : {CATALOG}")
print(f"targets  : {TARGETS}")
print(f"suffix   : {SUFFIX}")
print(f"new names: {new_names}")
```

---

## Cell 35 — markdown

### Backup rename — apply (safety-gated)

**What it does.** For each target schema: lists tables + views, drops
the views, creates the new `<name><SUFFIX>` schema, renames every table
into it, drops the now-empty old schema.

**Run when.** You've reviewed Cell 34's printout and confirmed the
target schemas + suffix.

**Gotcha.** `confirm = False` default — flip to True to execute. Views
are DROPPED (not renamed) — re-create them via Cells 16/22 after the
rename if needed. Fails fast on the first error: a half-renamed schema
needs manual recovery.

---

## Cell 36 — code

```python
# ============================================================
#   STOP. Read the Cell 34 printout before flipping
#   `confirm = True`.
#   - ALTER TABLE ... RENAME TO moves every table from <OLD> to <OLD><SUFFIX>.
#   - DROP VIEW removes view DDLs (re-create later with Cells 16/22 if needed).
#   - DROP SCHEMA removes the empty original namespace.
#   - Fails fast: if one ALTER TABLE fails, the loop aborts mid-batch and the
#     schema ends up half-renamed. Recovery is manual.
# ============================================================
confirm = False

if not confirm:
    print("This cell is a no-op while confirm=False. Set confirm=True and re-run to rename.")
else:
    for OLD in TARGETS:
        NEW = f"{OLD}{SUFFIX}"
        print(f"\n=== {OLD} -> {NEW} ===")

        try:
            rels = [r[1] for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{OLD}").collect()]
        except Exception as e:
            print(f"  SKIP: {OLD} not present in {CATALOG} ({e})")
            continue

        try:
            vws = [r[1] for r in spark.sql(f"SHOW VIEWS IN {CATALOG}.{OLD}").collect()]
        except Exception:
            vws = []
        tbls = [r for r in rels if r not in vws]
        print(f"  found: {len(tbls)} table(s), {len(vws)} view(s)")

        for v in vws:
            spark.sql(f"DROP VIEW IF EXISTS {CATALOG}.{OLD}.{v}")
            print(f"  dropped view {v}")

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{NEW}")
        print(f"  created schema {NEW}")

        for t in tbls:
            spark.sql(f"ALTER TABLE {CATALOG}.{OLD}.{t} RENAME TO {CATALOG}.{NEW}.{t}")
            print(f"  renamed {t}")

        spark.sql(f"DROP SCHEMA {CATALOG}.{OLD}")
        print(f"  dropped schema {OLD}")

    print("\nDONE. Run the verify cell to confirm.")
```

---

## Cell 37 — markdown

### Backup rename — verify

**What it does.** Lists namespaces in the target catalog and counts
tables in each new backup schema.

**Run when.** After Cell 36 with `confirm=True`.

**Gotcha.** Read-only.

---

## Cell 38 — code

```python
ns = [r[0] for r in spark.sql(f"SHOW NAMESPACES IN {CATALOG}").collect()]
print(f"Namespaces in {CATALOG}:")
for s in sorted(ns):
    print(f"  - {s}")

print()
for OLD in TARGETS:
    NEW = f"{OLD}{SUFFIX}"
    try:
        cnt = len(spark.sql(f"SHOW TABLES IN {CATALOG}.{NEW}").collect())
        print(f"{CATALOG}.{NEW}: {cnt} table(s)")
    except Exception as e:
        print(f"{CATALOG}.{NEW}: ERROR {e}")
```
