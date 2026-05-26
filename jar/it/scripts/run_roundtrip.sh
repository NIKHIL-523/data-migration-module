#!/usr/bin/env bash
# Drive one full round-trip for the chosen cloud: seed -> migrate -> assert.
#
# Usage (from data-migration-module/jar/):
#   sbt assembly && it/scripts/run_roundtrip.sh aws
#
# Requires the docker-compose stack to be up.
set -euo pipefail

CLOUD="${1:-aws}"
COMPOSE="docker compose -f $(dirname "$0")/../docker-compose.yml"

case "$CLOUD" in
  aws|azure|gcp) ;;
  *) echo "usage: $0 {aws|azure|gcp}" >&2; exit 2 ;;
esac

echo "==> seeding source on $CLOUD"
$COMPOSE exec -T spark \
  spark-submit \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.5.2 \
    /opt/it/scripts/seed_source_table.py "$CLOUD"

echo "==> running IcebergMigrate JAR on $CLOUD"
case "$CLOUD" in
  aws)
    CLOUD_ARGS=(
      --cloudProvider aws
      --awsRegion us-east-1
      --awsEndpoint http://minio:9000
      --awsPathStyle true
    )
    SRC_WH="s3a://iceberg-source/wh/"
    TGT_WH="s3a://iceberg-target/wh/"
    ;;
  azure)
    CLOUD_ARGS=(
      --cloudProvider azure
      --azureTenant   00000000-0000-0000-0000-000000000000
      --azureClientId 00000000-0000-0000-0000-000000000000
    )
    SRC_WH="wasb://iceberg-source@devstoreaccount1/wh/"
    TGT_WH="wasb://iceberg-target@devstoreaccount1/wh/"
    ;;
  gcp)
    CLOUD_ARGS=(
      --cloudProvider gcp
      --gcpProject    local-dev
    )
    SRC_WH="gs://iceberg-source/wh/"
    TGT_WH="gs://iceberg-target/wh/"
    ;;
esac

JAR=$(ls "$(dirname "$0")/../../target/scala-2.13/"data-migration-iceberg-*.jar | head -1)
echo "    using jar: $JAR"

# Note: the smoke-test invocation below assumes the SparkApplication CR
# would also pass --spark-service spark plus the source/target catalog
# configs through extra spark.conf. For local round-trip these come from
# the docker container env; in prod template_builder.py emits them into
# the CR spec.sparkConf block.
$COMPOSE exec -T spark \
  spark-submit \
    --class ai.prevalent.icebergmigrate.IcebergMigrate \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.5.2 \
    --conf spark.sql.catalog.iceberg_catalog1=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.iceberg_catalog1.type=hadoop \
    --conf "spark.sql.catalog.iceberg_catalog1.warehouse=$SRC_WH" \
    --conf spark.sql.catalog.iceberg_catalog2=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.iceberg_catalog2.type=hadoop \
    --conf "spark.sql.catalog.iceberg_catalog2.warehouse=$TGT_WH" \
    --conf spark.sds.hive.read.catalog=iceberg_catalog1 \
    --conf spark.sds.hive.write.catalog=iceberg_catalog2 \
    "/opt/spark/jars-extra/$(basename "$JAR")" \
    --spark-service spark \
    --tableName test.input_table \
    --listOfPartitionColumns date \
    "${CLOUD_ARGS[@]}"

echo "==> $CLOUD round-trip complete"
