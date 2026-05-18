"""
Dual-catalog SparkSession builder for the 05_workflow notebook.

Takes the same `connection` dict you pass into `template_builder.build_template`
plus an optional `sparkapp` dict for sizing knobs. Builds a SparkSession with
both Iceberg catalogs registered and the kg-view-validation JVM tuning
(memory, AQE, G1GC, Kryo, off-heap, shuffle.partitions) baked in.

Restart the kernel before calling this -- `SparkSession.builder.getOrCreate()`
returns the existing session if one exists, so the tuning configs below are
silently dropped on a warm kernel.
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def _catalog_conf(connection: dict) -> dict[str, str]:
    return {
        "spark.hadoop.fs.defaultFS": connection["default_fs"],
        "spark.sds.hive.read.catalog": "iceberg_catalog1",
        "spark.sds.hive.write.catalog": "iceberg_catalog2",
        "spark.hadoop.hive.metastore.execute.setugi": "false",
        "spark.sql.catalog.iceberg_catalog1": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg_catalog1.type": "hive",
        "spark.sql.catalog.iceberg_catalog1.uri": connection["source_hms_uri"],
        "spark.hadoop.hive.metastore.uris": connection["source_hms_uri"],
        "spark.sql.catalog.iceberg_catalog1.warehouse": connection["source_warehouse"],
        "spark.sql.catalog.iceberg_catalog2": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg_catalog2.type": "hive",
        "spark.sql.catalog.iceberg_catalog2.uri": connection["target_hms_uri"],
        "spark.sql.catalog.iceberg_catalog2.warehouse": connection["target_warehouse"],
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.caseSensitive": "true",
        # Azure ABFS via workload identity
        "spark.hadoop.fs.azure.account.auth.type": "OAuth",
        "spark.hadoop.fs.azure.account.oauth.provider.type": (
            "org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider"
        ),
        "spark.hadoop.fs.azure.account.oauth2.msi.tenant": connection["azure_tenant"],
        "spark.hadoop.fs.azure.account.oauth2.client.id": connection["azure_client_id"],
    }


def _jvm_tuning_conf(sparkapp: dict | None) -> dict[str, str]:
    """JVM/Spark sizing + AQE/GC/Kryo tuning derived from sparkapp dict.
    Falls back to safe-but-not-tiny defaults if sparkapp is omitted (e.g.
    when you're only doing catalog traversal + count queries, not heavy
    full-outer joins)."""
    sparkapp = sparkapp or {}
    return {
        "spark.driver.memory":     str(sparkapp.get("driver_memory", "10g")),
        "spark.driver.cores":      str(sparkapp.get("driver_cores", "3")),
        "spark.driver.maxResultSize": str(sparkapp.get("driver_max_result_size", "4g")),
        "spark.executor.memory":   str(sparkapp.get("executor_memory", "24g")),
        "spark.executor.cores":    str(sparkapp.get("executor_cores", "4")),
        "spark.executor.instances": str(sparkapp.get("executor_instances", "4")),
        "spark.memory.offHeap.enabled": "true",
        "spark.memory.offHeap.size":    str(sparkapp.get("off_heap_size", "4g")),
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryoserializer.buffer.max": "1024m",
        "spark.executor.extraJavaOptions": (
            "-Dcom.amazonaws.services.s3.enableV4 "
            "-XX:+UseG1GC "
            "-XX:+ParallelRefProcEnabled "
            "-XX:MaxGCPauseMillis=200"
        ),
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128mb",
        "spark.sql.shuffle.partitions": "2000",
    }


def build_spark_session(
    *,
    connection: dict,
    sparkapp: dict | None = None,
    app_name: str = "iceberg-migration-workflow",
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    """
    Build (or return existing) SparkSession with the dual catalogs from
    `connection` and the JVM/AQE/Kryo tuning derived from `sparkapp`
    (optional; sensible defaults).

    For K8s cluster mode, pass via `extra_conf`:
        {
            "spark.master": "k8s://https://kubernetes.default.svc",
            "spark.kubernetes.container.image": <image>,
            "spark.kubernetes.executor.podTemplateFile": <abfs://.../...yaml>,
            "spark.kubernetes.namespace": <ns>,
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
            ...
        }
    Without those, Spark uses whatever the kernel was launched with (often
    `local[*]` for Hub kernels without K8s wiring -- watch for that).

    IMPORTANT: restart the kernel before calling this if a session already
    exists. `getOrCreate()` will return the live session and silently drop
    everything below.
    """
    builder = SparkSession.builder.appName(app_name)
    for k, v in _catalog_conf(connection).items():
        builder = builder.config(k, v)
    for k, v in _jvm_tuning_conf(sparkapp).items():
        builder = builder.config(k, v)
    if extra_conf:
        for k, v in extra_conf.items():
            builder = builder.config(k, v)
    return builder.getOrCreate()


__all__ = ["build_spark_session"]
