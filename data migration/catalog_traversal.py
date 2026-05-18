"""
Iceberg catalog traversal helpers for the consolidated migration workflow.

Wraps:
  - SHOW NAMESPACES / SHOW TABLES / SHOW VIEWS over the dual catalogs.
  - Per-table partition-spec derivation via Py4J Spark3Util.loadIcebergTable
    (same approach as validate_iceberg_counts.format_partition_spec, but
    emitted in the migrate.py `partitionColumns` flag format:
    `<source_col>[:<transform>]` comma-separated).

Designed to be called from a Jupyter notebook cell-by-cell:

    from catalog_traversal import (
        list_schemas, list_tables, list_views,
        derive_partition_columns,
        summarize_tables, summarize_views,
    )

    schemas = list_schemas(spark, "iceberg_catalog1")
    df = summarize_tables(spark, "iceberg_catalog1", schemas=["kg", "kg_mini"])
    display(df)
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


SRC_CATALOG_DEFAULT = "iceberg_catalog1"   # QA
TGT_CATALOG_DEFAULT = "iceberg_catalog2"   # prod


# ---------------------------------------------------------------------------
# Catalog walking
# ---------------------------------------------------------------------------

def list_schemas(spark, catalog: str = SRC_CATALOG_DEFAULT) -> list[str]:
    """Return all top-level namespaces in `catalog`."""
    rows = spark.sql(f"SHOW NAMESPACES IN {catalog}").collect()
    return [r[0] for r in rows]


def list_tables(spark, catalog: str, schema: str) -> list[str]:
    """
    Return all relation names in `<catalog>.<schema>`.

    Spark's SHOW TABLES returns both tables and views (no isTemporary
    discrimination across HMS). For the kg layout we treat anything ending
    in a known view suffix as a view in summarize_views; otherwise this
    returns everything.
    """
    rows = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
    # SHOW TABLES schema is (namespace, tableName, isTemporary); use position
    # to avoid quirks in column case across Spark versions.
    return [r[1] for r in rows]


def list_views(spark, catalog: str, schema: str) -> list[str]:
    """
    Return all view names in `<catalog>.<schema>`.

    Uses SHOW VIEWS where supported; falls back to SHOW TABLES + filtering
    by `DESCRIBE EXTENDED ... WHERE col_name='Type'` if SHOW VIEWS isn't
    available in the connected HMS.
    """
    try:
        rows = spark.sql(f"SHOW VIEWS IN {catalog}.{schema}").collect()
        return [r[1] for r in rows]
    except Exception:
        # Fallback: SHOW TABLES + per-relation Type check. Slow for big
        # namespaces; only used if SHOW VIEWS fails.
        names = list_tables(spark, catalog, schema)
        out: list[str] = []
        for n in names:
            try:
                desc = spark.sql(
                    f"DESCRIBE EXTENDED {catalog}.{schema}.{n}"
                ).collect()
                kind = next(
                    (r[1] for r in desc if (r[0] or "").lower() == "type"),
                    "",
                ) or ""
                if "view" in kind.lower():
                    out.append(n)
            except Exception:
                continue
        return out


# ---------------------------------------------------------------------------
# Partition-spec derivation (Py4J)
# ---------------------------------------------------------------------------

def _load_iceberg_table(spark, fqn: str) -> Any:
    """Load org.apache.iceberg.Table via Py4J (matches validate_iceberg_counts)."""
    jvm = spark._jvm
    jsession = spark._jsparkSession
    return jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(jsession, fqn)


# Iceberg's Transform.toString() returns singular forms ("day", "hour",
# "month", "year"), but the IcebergMigrate JAR's --listOfPartitionColumns
# flag expects the plural forms. Map them here so the rendered manifest
# matches what the JAR parses. bucket[N] / truncate[N] / identity stay
# as Iceberg emits them.
_ICEBERG_TO_MIGRATE_TRANSFORM = {
    "day":   "days",
    "hour":  "hours",
    "month": "months",
    "year":  "years",
}


def derive_partition_columns(spark, fqn: str) -> str:
    """
    Return the `partitionColumns` string for `fqn` in `migrate.py` format:

        "<source_col>[:<transform>], ..."

    Examples:
        "updated_at_ts:days"
        "updated_at_ts:days,kg_content_type"
        ""                          # not partitioned

    Identity transforms drop the `:transform` suffix (matches the convention
    used by `01_table_migration/*/datasources.json`). Time-bucket transforms
    are pluralised (day -> days, hour -> hours, month -> months, year ->
    years) to match the IcebergMigrate JAR's expected flag format. Other
    derived transforms (`bucket[N]`, `truncate[N]`, ...) keep Iceberg's
    string form.

    Raises if the table can't be loaded via Spark3Util (e.g. non-Iceberg or
    HMS issue).
    """
    table = _load_iceberg_table(spark, fqn)
    schema = table.schema()
    parts: list[str] = []
    for f in table.spec().fields():
        source_field = schema.findField(f.sourceId())
        source_col = str(source_field.name()) if source_field is not None else str(f.name())
        tr_str = str(f.transform()).strip()
        tr_str = _ICEBERG_TO_MIGRATE_TRANSFORM.get(tr_str, tr_str)
        if tr_str == "identity":
            parts.append(source_col)
        else:
            parts.append(f"{source_col}:{tr_str}")
    return ",".join(parts)


def format_partition_spec_display(spark, fqn: str) -> str:
    """
    Audit-style display string: `"<fieldId>: <name>: <transform>(<sourceId>); ..."`
    or `"Not partitioned"`.

    Mirrors validate_iceberg_counts.format_partition_spec for parity with the
    existing partition-audit notebook; use this for *display*, use
    derive_partition_columns for *migration config*.
    """
    table = _load_iceberg_table(spark, fqn)
    fields = list(table.spec().fields())
    if not fields:
        return "Not partitioned"
    return "; ".join(
        f"{f.fieldId()}: {f.name()}: {f.transform()}({f.sourceId()})"
        for f in fields
    )


# ---------------------------------------------------------------------------
# Tabular summaries (for display in the notebook)
# ---------------------------------------------------------------------------

def summarize_tables(spark, catalog: str, schemas: list[str]):
    """
    For every table under each schema in `<catalog>`, derive its partition
    columns (migrate.py format) and the audit display. Returns a pandas
    DataFrame with columns:

        catalog, schema, table, partition_columns, partition_spec_display, error

    Tables that fail to load (e.g. non-Iceberg in the same HMS) get the
    error stringified into the `error` column and empty values elsewhere.
    """
    import pandas as pd

    rows: list[dict] = []
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    for ns in schemas:
        names = list_tables(spark, catalog, ns)
        for tbl in names:
            fqn = f"{catalog}.{ns}.{tbl}"
            row: dict = {
                "catalog": catalog,
                "schema": ns,
                "table": tbl,
                "partition_columns": "",
                "partition_spec_display": "",
                "checked_at": now,
                "error": None,
            }
            try:
                row["partition_columns"] = derive_partition_columns(spark, fqn)
                row["partition_spec_display"] = format_partition_spec_display(spark, fqn)
            except Exception as e:
                row["error"] = str(e)
            rows.append(row)
    return pd.DataFrame(rows, columns=[
        "catalog", "schema", "table",
        "partition_columns", "partition_spec_display",
        "checked_at", "error",
    ])


def summarize_views(spark, catalog: str, schemas: list[str]):
    """
    For every view under each schema in `<catalog>`, return its name and
    the SHOW CREATE statement (useful for QA -> prod definition copy). The
    DDL itself isn't rewritten here -- that's `migrate_views.migrate_view`'s
    job. Returns a pandas DataFrame with columns:

        catalog, schema, view, ddl_preview, error
    """
    import pandas as pd

    rows: list[dict] = []
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    for ns in schemas:
        names = list_views(spark, catalog, ns)
        for view in names:
            fqn = f"{catalog}.{ns}.{view}"
            row: dict = {
                "catalog": catalog,
                "schema": ns,
                "view": view,
                "ddl_preview": "",
                "checked_at": now,
                "error": None,
            }
            try:
                ddl_rows = spark.sql(f"SHOW CREATE TABLE {fqn}").collect()
                ddl = "\n".join(r[0] for r in ddl_rows)
                # Truncate for display; full DDL still available via spark.sql later.
                row["ddl_preview"] = ddl[:400] + (" ..." if len(ddl) > 400 else "")
            except Exception as e:
                row["error"] = str(e)
            rows.append(row)
    return pd.DataFrame(rows, columns=[
        "catalog", "schema", "view", "ddl_preview", "checked_at", "error",
    ])


__all__ = [
    "SRC_CATALOG_DEFAULT",
    "TGT_CATALOG_DEFAULT",
    "list_schemas",
    "list_tables",
    "list_views",
    "derive_partition_columns",
    "format_partition_spec_display",
    "summarize_tables",
    "summarize_views",
]
