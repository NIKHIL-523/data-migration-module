"""
Smoke tests for the per-row filter lookup used by Cell 12 (table validation).

The validation cell loads `datasources.json`, builds
`{k8s_name -> filterExpression}`, and looks up each state row's filter by
its `k8s_name`. The key must match what `bundle_writer` writes into
`table_state.csv` for every kind of row:
  - explicit k8sName override (e.g. chunked tables: "...feb", "...mar")
  - auto-derived (table name, underscores/hyphens stripped, truncated to 52)

Run:
    cd "data migration" && python3 test_validation_filter_lookup.py
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

from bundle_writer import k8s_name_for_datasource_row


def build_filter_map(ds_rows: list[dict]) -> dict[str, str]:
    """Mirror of the Cell 12 lookup-map construction."""
    return {
        k8s_name_for_datasource_row(row): (row.get("filterExpression") or "")
        for row in ds_rows
    }


def assert_eq(label: str, got, want) -> None:
    if got != want:
        print(f"FAIL {label}: got {got!r}, want {want!r}")
        sys.exit(1)
    print(f"  ok  {label}")


def test_explicit_k8s_overrides_for_chunked_rows() -> None:
    """A chunked table appears N times with distinct k8sName overrides."""
    ds_rows = [
        {"table": "kg.sds_ei__rel__tanium__vulnerability_finding_on_host",
         "k8sName": "sdseireltaniumvulnerabilityfindingonhost-kg-feb",
         "filterExpression": "updated_at_ts >= '2026-02-02' AND updated_at_ts <= '2026-02-28'"},
        {"table": "kg.sds_ei__rel__tanium__vulnerability_finding_on_host",
         "k8sName": "sdseireltaniumvulnerabilityfindingonhost-kg-mar",
         "filterExpression": "updated_at_ts >= '2026-03-01' AND updated_at_ts <= '2026-03-31'"},
    ]
    fm = build_filter_map(ds_rows)
    assert_eq("two entries", len(fm), 2)
    assert_eq("feb chunk", fm["sdseireltaniumvulnerabilityfindingonhost-kg-feb"],
              "updated_at_ts >= '2026-02-02' AND updated_at_ts <= '2026-02-28'")
    assert_eq("mar chunk", fm["sdseireltaniumvulnerabilityfindingonhost-kg-mar"],
              "updated_at_ts >= '2026-03-01' AND updated_at_ts <= '2026-03-31'")


def test_no_override_uses_auto_derived_key() -> None:
    """A row without k8sName matches by auto-derived key (last segment, stripped, <=52)."""
    ds_rows = [
        {"table": "lookup_v2.country_lookup_iso",
         "filterExpression": "updated_at_ts <= '2026-05-22'"},
    ]
    fm = build_filter_map(ds_rows)
    # bundle_writer rule: split on "." -> "country_lookup_iso"
    #                    .replace("_","").replace("-","").lower-NA, no lower
    #                    [:52] -> "countrylookupiso"
    assert_eq("auto key", list(fm.keys())[0], "countrylookupiso")
    assert_eq("filter", fm["countrylookupiso"], "updated_at_ts <= '2026-05-22'")


def test_empty_filter_expression_preserved() -> None:
    """Missing/empty filterExpression yields '' (means: no WHERE clause)."""
    ds_rows = [
        {"table": "lookup_v2.country_lookup", "k8sName": "countrylookup-lkp"},  # no filterExpression
    ]
    fm = build_filter_map(ds_rows)
    assert_eq("empty filter present", fm.get("countrylookup-lkp"), "")


def test_round_trip_with_real_datasources_json() -> None:
    """Round-trip: write a JSON file mirroring bundle output, read + look up."""
    rows = [
        {"table": "kg.t1", "k8sName": "t1-kg-feb", "filterExpression": "a >= 1"},
        {"table": "kg.t1", "k8sName": "t1-kg-mar", "filterExpression": "a >= 2"},
        {"table": "lookup_v2.t2",                  "filterExpression": "b <= 5"},  # auto-derived key
    ]
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "datasources.json"
        p.write_text(json.dumps(rows))
        loaded = json.loads(p.read_text())
        fm = build_filter_map(loaded)
    assert_eq("3 entries", len(fm), 3)
    assert_eq("override key feb", fm["t1-kg-feb"], "a >= 1")
    assert_eq("override key mar", fm["t1-kg-mar"], "a >= 2")
    assert_eq("auto key t2",      fm["t2"],        "b <= 5")


def main() -> None:
    print("test_explicit_k8s_overrides_for_chunked_rows")
    test_explicit_k8s_overrides_for_chunked_rows()
    print("test_no_override_uses_auto_derived_key")
    test_no_override_uses_auto_derived_key()
    print("test_empty_filter_expression_preserved")
    test_empty_filter_expression_preserved()
    print("test_round_trip_with_real_datasources_json")
    test_round_trip_with_real_datasources_json()
    print("\nall tests passed")


if __name__ == "__main__":
    main()
