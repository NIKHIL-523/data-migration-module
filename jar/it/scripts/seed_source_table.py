"""
Seed one Iceberg table on the "source" emulator for the cloud being tested.

Usage (inside the spark container):
  spark-submit /opt/it/scripts/seed_source_table.py <cloud>

<cloud> ∈ {aws, azure, gcp}. The catalog uses Iceberg's `hadoop` type
(metadata stored alongside data in the object store), so no HMS is
required for the local round-trip. Production still uses HMS — the JAR
is catalog-impl agnostic.
"""
from __future__ import annotations

import sys

from pyspark.sql import SparkSession


def build_conf(cloud: str) -> dict[str, str]:
    if cloud == "aws":
        return {
            # MinIO via s3a.
            "spark.hadoop.fs.s3a.endpoint":          "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key":        "minioadmin",
            "spark.hadoop.fs.s3a.secret.key":        "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.aws.credentials.provider":
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.sql.catalog.src":           "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.src.type":      "hadoop",
            "spark.sql.catalog.src.warehouse": "s3a://iceberg-source/wh/",
        }
    if cloud == "azure":
        # Azurite well-known dev key.
        conn_str = (
            "DefaultEndpointsProtocol=http;"
            "AccountName=devstoreaccount1;"
            "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
            "BlobEndpoint=http://azurite:10000/devstoreaccount1;"
        )
        return {
            "spark.hadoop.fs.azure.storage.emulator.account.name":
                "devstoreaccount1.blob.core.windows.net",
            "spark.hadoop.fs.azure.account.key.devstoreaccount1.blob.core.windows.net":
                "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
            "spark.hadoop.fs.azure.connection.string":           conn_str,
            "spark.sql.catalog.src":           "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.src.type":      "hadoop",
            "spark.sql.catalog.src.warehouse":
                "wasb://iceberg-source@devstoreaccount1/wh/",
        }
    if cloud == "gcp":
        return {
            "spark.hadoop.fs.gs.impl":
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "spark.hadoop.fs.gs.storage.root.url": "http://fake-gcs:4443",
            "spark.hadoop.google.cloud.auth.type": "UNAUTHENTICATED",
            "spark.sql.catalog.src":           "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.src.type":      "hadoop",
            "spark.sql.catalog.src.warehouse": "gs://iceberg-source/wh/",
        }
    raise SystemExit(f"unknown cloud: {cloud}")


def main(cloud: str) -> None:
    conf = build_conf(cloud)
    builder = SparkSession.builder.appName(f"seed-{cloud}")
    for k, v in conf.items():
        builder = builder.config(k, v)
    builder = builder.config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    spark = builder.getOrCreate()

    spark.sql("CREATE SCHEMA IF NOT EXISTS src.test")
    spark.sql("DROP TABLE IF EXISTS src.test.input_table")
    spark.sql("""
        CREATE TABLE src.test.input_table (
          client     STRING,
          date       STRING,
          class      STRING,
          data_type  STRING,
          date_new   STRING
        ) USING iceberg
        PARTITIONED BY (date)
    """)
    spark.sql("""
        INSERT INTO src.test.input_table VALUES
          ('xxxxx','2022-11-12','direct',  'hosting_banners','2022-12-10'),
          ('xxxxx','2022-11-12','indirect','hosting_banners','2022-12-10'),
          ('xxxxx','2022-11-12','direct',  'hosting_banners','2022-12-11'),
          ('yyyyy','2022-11-12','indirect','hosting_banners','2022-12-12')
    """)

    n = spark.sql("SELECT COUNT(*) c FROM src.test.input_table").collect()[0]["c"]
    print(f"[seed:{cloud}] rows written = {n}")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "aws")
