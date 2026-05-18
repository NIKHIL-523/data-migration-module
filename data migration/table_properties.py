"""
Iceberg TBLPROPERTIES extraction for the QA -> prod property-sync phase
that follows table + view migration.

Two extraction paths are exposed so we can see what each surfaces:

  properties_via_sql(spark, fqn)     -- SHOW TBLPROPERTIES (Spark SQL).
                                        User-visible Iceberg props, returned
                                        as dict[str, str].
  properties_via_iceberg(spark, fqn) -- TableMetadata.properties() via Py4J.
                                        Raw Iceberg view, same shape.

compare_properties(spark, fqn) runs both and returns a pandas DataFrame:

    key | sql_value | iceberg_value | only_in    ({both, sql, iceberg})

Designed for ad-hoc notebook use, mirroring the catalog_traversal idiom
(`_load_iceberg_table` via Spark3Util.loadIcebergTable).
"""

from __future__ import annotations

from typing import Any


def _load_iceberg_table(spark, fqn: str) -> Any:
    jvm = spark._jvm
    jsession = spark._jsparkSession
    return jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(jsession, fqn)


def properties_via_sql(spark, fqn: str) -> dict[str, str]:
    """SHOW TBLPROPERTIES <fqn> as a dict[str, str]."""
    rows = spark.sql(f"SHOW TBLPROPERTIES {fqn}").collect()
    return {str(r[0]): str(r[1]) for r in rows}


def properties_via_iceberg(spark, fqn: str) -> dict[str, str]:
    """TableMetadata.properties() via Py4J as a dict[str, str]."""
    table = _load_iceberg_table(spark, fqn)
    props = table.properties()
    keys = list(props.keySet().toArray())
    return {str(k): str(props.get(k)) for k in keys}


def compare_properties(spark, fqn: str):
    """
    Run both extractors against `fqn` and return a side-by-side pandas
    DataFrame sorted by key, with columns:

        key, sql_value, iceberg_value, only_in

    `only_in` is 'both' if the key appears in both views, otherwise 'sql'
    or 'iceberg' to flag which side hides it.
    """
    import pandas as pd

    sql_props = properties_via_sql(spark, fqn)
    ice_props = properties_via_iceberg(spark, fqn)

    rows: list[dict] = []
    for key in sorted(set(sql_props) | set(ice_props)):
        in_sql = key in sql_props
        in_ice = key in ice_props
        only_in = "both" if (in_sql and in_ice) else ("sql" if in_sql else "iceberg")
        rows.append({
            "key":           key,
            "sql_value":     sql_props.get(key, ""),
            "iceberg_value": ice_props.get(key, ""),
            "only_in":       only_in,
        })
    return pd.DataFrame(rows, columns=["key", "sql_value", "iceberg_value", "only_in"])


def diff_qa_vs_prod(spark, qa_fqn: str, prod_fqn: str):
    """
    Compare TBLPROPERTIES across two FQNs using SHOW TBLPROPERTIES as the
    canonical user-visible set. Returns a pandas DataFrame:

        key, qa_value, prod_value, status   ({equal, missing_on_prod,
                                              extra_on_prod, value_changed})

    Use this after compare_properties has shown that the SQL view captures
    everything you care about; otherwise build the diff from
    properties_via_iceberg dicts directly.
    """
    import pandas as pd

    qa = properties_via_sql(spark, qa_fqn)
    pd_ = properties_via_sql(spark, prod_fqn)

    rows: list[dict] = []
    for key in sorted(set(qa) | set(pd_)):
        in_qa = key in qa
        in_pd = key in pd_
        if in_qa and in_pd:
            status = "equal" if qa[key] == pd_[key] else "value_changed"
        elif in_qa:
            status = "missing_on_prod"
        else:
            status = "extra_on_prod"
        rows.append({
            "key":        key,
            "qa_value":   qa.get(key, ""),
            "prod_value": pd_.get(key, ""),
            "status":     status,
        })
    return pd.DataFrame(rows, columns=["key", "qa_value", "prod_value", "status"])


# ---------------------------------------------------------------------------
# Property sync (QA -> prod) for the post-migration property phase
# ---------------------------------------------------------------------------

# Iceberg-managed keys synthesized from TableMetadata fields rather than
# stored in the user-settable `properties` Map. SHOW TBLPROPERTIES exposes
# them, but ALTER TABLE SET TBLPROPERTIES rejects them. Always skip.
RESERVED_KEYS: frozenset[str] = frozenset({
    "current-snapshot-id",
    "format",
    "format-version",
    "snapshot-count",
    "last-updated-ms",
    "current-schema-id",
    "default-partition-spec",
    "default-sort-order-id",
})


def _sql_quote(value: str) -> str:
    """Escape single quotes for embedding inside a SQL string literal."""
    return str(value).replace("'", "''")


def plan_property_sync(
    spark,
    qa_fqn: str,
    prod_fqn: str,
    *,
    source: str = "iceberg",
    skip_keys: frozenset[str] = RESERVED_KEYS,
) -> dict[str, str]:
    """
    Compute the SET TBLPROPERTIES plan for `prod_fqn`: every key on QA
    whose value is missing or different on prod, minus reserved keys.

    source = 'iceberg' (default) uses Py4J table.properties() — the
    user-set view. source = 'sql' uses SHOW TBLPROPERTIES.
    """
    read = properties_via_iceberg if source == "iceberg" else properties_via_sql
    qa = read(spark, qa_fqn)
    pd_ = read(spark, prod_fqn)
    plan: dict[str, str] = {}
    for key, value in qa.items():
        if key in skip_keys:
            continue
        if pd_.get(key) != value:
            plan[key] = value
    return plan


def apply_property_plan(spark, prod_fqn: str, plan: dict[str, str]) -> None:
    """Run a single ALTER TABLE SET TBLPROPERTIES on `prod_fqn`."""
    if not plan:
        return
    pairs = ", ".join(
        f"'{_sql_quote(k)}'='{_sql_quote(v)}'" for k, v in plan.items()
    )
    spark.sql(f"ALTER TABLE {prod_fqn} SET TBLPROPERTIES ({pairs})")


def sync_batch(
    spark,
    pairs,
    *,
    dry_run: bool = True,
    source: str = "iceberg",
    skip_keys: frozenset[str] = RESERVED_KEYS,
):
    """
    Plan (and optionally apply) the property sync across many tables.

    `pairs` is an iterable of (qa_fqn, prod_fqn). Returns a pandas
    DataFrame: qa_fqn, prod_fqn, key_count, keys, applied, error, at.
    Dry-run by default — pass dry_run=False to actually run ALTERs.
    """
    import pandas as pd
    from datetime import datetime, timezone

    rows: list[dict] = []
    for qa_fqn, prod_fqn in pairs:
        row = {
            "qa_fqn":    qa_fqn,
            "prod_fqn":  prod_fqn,
            "key_count": 0,
            "keys":      "",
            "applied":   False,
            "error":     "",
            "at":        datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        try:
            plan = plan_property_sync(
                spark, qa_fqn, prod_fqn,
                source=source, skip_keys=skip_keys,
            )
            row["key_count"] = len(plan)
            row["keys"] = ",".join(sorted(plan))
            if plan and not dry_run:
                apply_property_plan(spark, prod_fqn, plan)
                row["applied"] = True
        except Exception as exc:
            row["error"] = str(exc)
        rows.append(row)
    return pd.DataFrame(
        rows,
        columns=["qa_fqn", "prod_fqn", "key_count", "keys",
                 "applied", "error", "at"],
    )


__all__ = [
    "properties_via_sql",
    "properties_via_iceberg",
    "compare_properties",
    "diff_qa_vs_prod",
    "RESERVED_KEYS",
    "plan_property_sync",
    "apply_property_plan",
    "sync_batch",
]
