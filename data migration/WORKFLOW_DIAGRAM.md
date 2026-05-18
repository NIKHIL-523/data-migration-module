# 05_workflow — end-to-end flow

```mermaid
flowchart LR
    classDef setup    fill:#DBEAFE,stroke:#1E40AF,color:#1E3A8A
    classDef discover fill:#EDE9FE,stroke:#6D28D9,color:#4C1D95
    classDef bundle   fill:#FCE7F3,stroke:#BE185D,color:#831843
    classDef tables   fill:#DCFCE7,stroke:#15803D,color:#14532D
    classDef views    fill:#FFEDD5,stroke:#C2410C,color:#7C2D12
    classDef state    fill:#CCFBF1,stroke:#0F766E,color:#134E4A
    classDef validate fill:#E0F2FE,stroke:#0369A1,color:#0C4A6E
    classDef fork     fill:#FEF3C7,stroke:#A16207,color:#713F12

    A["1. Configure<br/>connection + sparkapp"]:::setup
    B["2. Discover<br/>schemas, tables, views"]:::discover
    C["3. Bundle<br/>datasources + template + CSVs"]:::bundle
    F{"4. Run where?"}:::fork

    A --> B
    B --> C
    C --> F

    subgraph TBL["Tables - ops VM"]
        T1["python migrate.py<br/>poll SparkApplication"]:::tables
        TS[("table_state.csv")]:::state
        T1 --> TS
    end

    subgraph VW["Views - JupyterHub"]
        V1["view_workflow.migrate_views<br/>6-step DDL rewrite"]:::views
        VS[("view_state.csv")]:::state
        V1 --> VS
    end

    F -->|tables| T1
    F -->|views| V1

    VAL["5. Validate<br/>COUNT + row-level diff"]:::validate
    TS --> VAL
    VS --> VAL
    VAL -.-> TS
    VAL -.-> VS
```

### Reading the diagram

1. **Configure** — Cell 1–2. Fill `connection` + `sparkapp`; `build_spark_session(...)` returns a dual-catalog Spark session (QA + prod).
2. **Discover** — Cell 3–5. Walk QA schemas, list tables, derive partition specs.
3. **Bundle** — Cell 6–9. Pick what to migrate, normalise into `datasources.json`, render `template.json`, write `sessions/<name>/` with two seeded CSV state trackers.
4. **Run** — bundle is self-contained. Tables go to the ops VM (`python migrate.py` polls the `SparkApplication` CR every 30s). Views run in-place from JupyterHub (`view_workflow.migrate_views` rewrites DDL and applies on the prod catalog).
5. **Validate** — `COUNT(*)` + row-level eqNullSafe diff. Verdict (`ok` / `mismatch` / `error`) is written back to the same CSVs.

### Conventions

- **Skip-success is the default** on both lanes. Wipe a status cell in JupyterLab (or pass `--rerun-all`) to retry.
- **CSV — not JSON.** JupyterLab's table viewer means no JSON editor needed for overrides.
- **One canonical migrate.py** at `migrate_template/migrate.py`; the bundle writer copies it per session.
