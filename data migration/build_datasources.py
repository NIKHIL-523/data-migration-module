"""
Build `datasources.json` (for `migrate.py`) from a list of user-defined
table mapping rows.

Per the workflow Cell 3 contract, each input row specifies:
    {
        "src_schema":         "<db on QA>",                (required)
        "src_table":          "<table name on QA>",        (required)
        "tgt_schema":         "<db on prod>",              (optional; default = src_schema)
        "tgt_table":          "<table name on prod>",      (optional; default = src_table)
        "partition_columns":  "<col[:transform],...>",     (optional; derived via Py4J if missing)
        "filter_expression":  "<sql predicate>",           (optional)
        "k8s_name":           "<rfc-1123 metadata.name>",  (optional; collision override)
        "qa_count":           <int>,                       (optional; informational)
    }

Emits per-row datasources.json shape consumed by migrate.py:
    {
        "table":             "<src_schema>.<src_table>",
        "partitionColumns":  "...",
        "filterExpression":  "...",      (when set)
        "outputSchema":      "<tgt_schema>",  (when != src_schema)
        "outputTable":       "<tgt_table>",   (when != src_table)
        "k8sName":           "...",      (when set)
        "qa_count":          <int>       (when set)
    }
"""

from __future__ import annotations

from catalog_traversal import derive_partition_columns


def build_datasources_rows(
    spark,
    *,
    qa_catalog: str = "iceberg_catalog1",
    selections: list[dict],
    auto_derive_partitions: bool = True,
) -> list[dict]:
    """
    Validate + normalise the user's selection list, derive missing
    `partition_columns` via Py4J, and emit datasources.json rows.

    Args:
      selections: list of dicts in the Cell-3 shape (see module docstring).
      auto_derive_partitions: if True (default), rows that omit
        `partition_columns` get it filled via `derive_partition_columns`
        on the QA-side table. Set to False if you've supplied them all
        manually (or to skip Py4J calls for non-partitioned tables).
    """
    out: list[dict] = []
    errors: list[tuple[str, str]] = []

    for raw in selections:
        if not isinstance(raw, dict):
            errors.append((str(raw), "row is not a dict"))
            continue

        src_schema = (raw.get("src_schema") or "").strip()
        src_table  = (raw.get("src_table") or "").strip()
        if not src_schema or not src_table:
            errors.append((str(raw), "missing src_schema or src_table"))
            continue
        qa_full = f"{src_schema}.{src_table}"

        tgt_schema = (raw.get("tgt_schema") or src_schema).strip()
        tgt_table  = (raw.get("tgt_table")  or src_table ).strip()

        # Partition columns: explicit, else derived from QA table metadata.
        part_cols = (raw.get("partition_columns") or "").strip()
        if not part_cols and auto_derive_partitions:
            try:
                part_cols = derive_partition_columns(spark, f"{qa_catalog}.{qa_full}")
            except Exception as e:
                errors.append((qa_full, f"partition derivation failed: {e}"))
                part_cols = ""

        row: dict = {
            "table": qa_full,
            "partitionColumns": part_cols,
        }
        filter_expr = (raw.get("filter_expression") or "").strip()
        if filter_expr:
            row["filterExpression"] = filter_expr
        if tgt_schema != src_schema:
            row["outputSchema"] = tgt_schema
        if tgt_table != src_table:
            row["outputTable"] = tgt_table
        k8s = (raw.get("k8s_name") or "").strip()
        if k8s:
            row["k8sName"] = k8s
        qa_count = raw.get("qa_count")
        if qa_count is not None:
            try:
                row["qa_count"] = int(qa_count)
            except (TypeError, ValueError):
                errors.append((qa_full, f"qa_count not int: {qa_count!r}"))
        out.append(row)

    if errors:
        print(f"WARN: {len(errors)} row(s) had derivation issues:")
        for t, msg in errors:
            print(f"  - {t}: {msg}")

    return out


def preview_rows(rows: list[dict]) -> None:
    """Display rows as a pandas DataFrame inside pd.option_context.
    Returns None to avoid Jupyter's auto-display duplicating the table."""
    import pandas as pd
    from IPython.display import display

    df = pd.DataFrame(rows)
    with pd.option_context(
        "display.max_rows", None,
        "display.max_colwidth", None,
        "display.width", 240,
    ):
        display(df)


__all__ = [
    "build_datasources_rows",
    "preview_rows",
]
