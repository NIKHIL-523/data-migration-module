# Local cloud-emulator integration tests

Round-trips the multi-cloud `IcebergMigrate` JAR against three cloud
emulators running in Docker. No real AWS / Azure / GCP creds required.

## Stack

| Service | Purpose | Default endpoint |
|---|---|---|
| `minio` | AWS S3 emulator (real `s3a://` driver works) | `http://minio:9000` |
| `azurite` | Azure Storage emulator (real `wasb://` / `abfs://`) | `http://azurite:10000` |
| `fake-gcs` | GCS emulator | `http://fake-gcs:4443` |
| `spark` | Bitnami Spark 3.5.5 runner; mounts the `sbt assembly` output | — |

Catalogs use Iceberg's `hadoop` type (metadata alongside data in the
object store) — no HMS container needed for local round-trips. Production
still uses HMS; the JAR is catalog-impl-agnostic.

## Run a round-trip

```bash
# from data-migration-module/jar/
sbt assembly

docker compose -f it/docker-compose.yml up -d

it/scripts/run_roundtrip.sh aws       # round-trip on MinIO
it/scripts/run_roundtrip.sh azure     # round-trip on Azurite
it/scripts/run_roundtrip.sh gcp       # round-trip on fake-gcs

docker compose -f it/docker-compose.yml down -v
```

Each round-trip:

1. `seed_source_table.py` — creates `src.test.input_table` on the
   cloud's source bucket with 4 sample rows partitioned by `date`.
2. `IcebergMigrate` JAR — runs the migration to the target bucket.
3. Asserts row count parity (TODO: validation cell wired into the
   round-trip driver).

## Caveat

The CLI auth flags passed to the JAR in local mode are mostly *no-ops*
because the seed script's plain-key auth is what actually authenticates
to the emulators — but the JAR's `CloudFsConfig.applyTo` still runs and
the `provider` selection still flows through. The intent of the
emulator stack is to validate **code path** (the JAR's argument parsing,
provider dispatch, FS scheme handling, Iceberg read/write across two
warehouses) — *not* to validate real-cloud identity federation. Real
IRSA / Workload-Identity validation needs the cluster, by definition.

## Output location for the JAR

`build.sbt` writes the assembly to:

```
data-migration-module/jar/target/scala-2.13/data-migration-iceberg-<sparkMajor>_2.13-<version>.jar
```

That directory is mounted into the `spark` container at
`/opt/spark/jars-extra/` so `spark-submit` finds it without copying.
