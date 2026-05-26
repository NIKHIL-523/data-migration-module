# `jar/` — multi-cloud fork of IcebergMigrate

Cloud-agnostic fork of the `IcebergMigrate` JAR that today lives in
[`prevalent-ai/sds-platform-apps/utility-jobs`][upstream].

**This module is staged here in `NIKHIL-523/data-migration-module` until
it's battle-tested across all three clouds. It does *not* push to
`sds-platform-apps`.** Graduation back to platform-apps is a manual,
explicit step (see below).

[upstream]: https://github.com/prevalent-ai/sds-platform-apps/tree/main/utility-jobs/src/main/scala/ai/prevalent/icebergmigrate

## What changes vs the upstream JAR

| | upstream `utility-jobs` | this `jar/` |
|---|---|---|
| Package & main class | `ai.prevalent.icebergmigrate.IcebergMigrate` | **same** (so the SparkApplication CR's `mainClass` is unchanged when you swap the artifact) |
| Migration logic (`execute()`) | Azure-only at the orchestration layer (sparkConf hardcoded for ABFS WI) | **same business logic** — verbatim copy of the original body |
| CLI args | `--tableName --listOfPartitionColumns --filterExpression --outputSchema` | adds `--cloudProvider` plus per-provider knobs |
| Hadoop FS config | hardcoded in the SparkApplication CR's `sparkConf` block (Azure-only) | applied at runtime by `CloudFsConfig.applyTo(spark)` from CLI args |
| Artifact name | `sds-pe-utility-jobs-3.5_2.13-1.2.0.jar` | `data-migration-iceberg-3.5_2.13-<version>.jar` |

The Python orchestration in `../data migration/` (`template_builder.py`,
`spark_session.py`) has been parametrized in parallel — see those files.

## Layout

```
jar/
├── build.sbt                          # depends on sds-pe-core via SSH-NIKHIL-523 alias
├── project/{build.properties, plugins.sbt}
├── src/
│   ├── main/scala/ai/prevalent/icebergmigrate/
│   │   ├── IcebergMigrate.scala        # forked + 3-line cloud bootstrap
│   │   ├── IcebergMigrateArgs.scala    # original 4 flags + 10 cloud flags
│   │   ├── IcebergMigrateConfig.scala  # verbatim
│   │   └── cloudfs/CloudFsConfig.scala # sealed trait: Azure | Aws | Gcp
│   └── test/scala/.../CloudFsConfigSpec.scala
└── it/
    ├── docker-compose.yml              # MinIO + Azurite + fake-gcs + spark
    ├── scripts/seed_source_table.py
    ├── scripts/run_roundtrip.sh
    └── README.md                       # how to run a round-trip
```

## CLI contract

```
spark-submit \
  --class ai.prevalent.icebergmigrate.IcebergMigrate \
  data-migration-iceberg-3.5_2.13-<v>.jar \
  --spark-service spark \
  --tableName <db>.<table> \
  --listOfPartitionColumns <col>[,<col>:days] \
  [--filterExpression <where-clause>] \
  [--outputSchema <db>] \
  --cloudProvider {azure|aws|gcp} \
  # Azure:
  [--azureTenant <uuid> --azureClientId <uuid> --azureStorageAccount <acct>] \
  # AWS:
  [--awsRegion <region> --awsRoleArn <arn> --awsEndpoint <url> --awsPathStyle <true|false>] \
  # GCP:
  [--gcpProject <id> --gcpKeyFile <path>]
```

The first thing `execute()` does is build a `CloudFsConfig` from the
CLI args and call `applyTo(spark)`, which sets the appropriate
`spark.hadoop.fs.*` keys on the live session. Hadoop reads them lazily
on first FS access, so this happens before any Iceberg read/write.

## Build

```bash
cd jar/
sbt clean assembly
# artifact lands at target/scala-2.13/data-migration-iceberg-3.5_2.13-<version>.jar
```

SBT pulls `sds-pe-core@release-4.1.2` over SSH using the
`github.com-nikhil523` host alias (see
`~/.ssh/config` and the memory note on prevalent-ai auth split). Verify:

```bash
ssh -T git@github.com-nikhil523       # expect: Hi NIKHIL-523!
```

## Test

Unit tests for the pure-config layer (no Spark):

```bash
sbt test
```

End-to-end round-trip against cloud emulators — MinIO + Azurite +
fake-gcs in Docker — see [`it/README.md`](it/README.md).

## Graduation checklist (Stage 2 → sds-platform-apps)

Do **not** run any of this until all three clouds are green locally and
you explicitly say "ship it":

1. Open a PR against `prevalent-ai/sds-platform-apps` that copies
   `jar/src/main/scala/ai/prevalent/icebergmigrate/{IcebergMigrate, IcebergMigrateArgs, cloudfs/CloudFsConfig}.scala`
   into `utility-jobs/src/main/scala/ai/prevalent/icebergmigrate/`.
   (`IcebergMigrateConfig.scala` is identical — no copy needed.)
2. Update `utility-jobs/build.sbt`: no dependency changes required —
   `CloudFsConfig` has no new third-party deps; the AWS auth provider
   classes resolve from `aws-java-sdk-bundle` which Spark's hadoop-aws
   already pulls.
3. Move the unit tests under `utility-jobs/src/test/scala/isolated/…`
   to fit the existing Shared/Isolated test scaffolding.
4. Bump `utility-jobs/branch.version` to `1.3.0` (or whatever's next).
5. Run the existing Jenkins pipeline; the assembled JAR ships to JFrog
   as `sds-pe-utility-jobs-3.5_2.13-1.3.0.jar`.
6. In production templates, swap to that new JAR + add the
   `--cloudProvider azure …` args. Behaviour is identical to today's
   Azure-only path (the cloud bootstrap is a no-op against an already-
   configured Spark session except for setting the same keys twice).
7. The data-migration-module fork can then either be deleted or kept as
   a downstream playground; either way it should no longer be the
   source of truth.

## Why fork instead of patching utility-jobs directly

- Lets us iterate on multi-cloud surface area (CLI flags, image
  selection, executor tolerations) without touching shared CI.
- Local docker-compose validation against MinIO/Azurite/fake-gcs is
  cheap and avoids burning real-cloud quota.
- Once green, graduation is a near-zero-diff PR — same package, same
  main class, same `mainApplicationFile` path in the CR.
