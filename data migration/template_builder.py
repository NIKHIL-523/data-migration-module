"""
Build the SparkApplication CR template.json from a user-supplied connection
config + Spark sizing/k8s config. This is the file the migrate.py driver
fills in per row (--tableName, --listOfPartitionColumns, per-row outputSchema)
before `kubectl apply`.

Two-stage flow in the notebook:
  Cell 1 -> connection_config (catalogs, HMS, ABFS, Azure WI)  ----+
  Cell 4 -> sparkapp_config   (driver/executor sizing, k8s)   ----+--> template.json

The default IcebergMigrate sparkConf knobs (snappy compression, distribution
mode none, Prometheus metrics, decommission storage) are baked into this
function; override or extend via `extra_spark_conf`.
"""

from __future__ import annotations

import json
from pathlib import Path


# Defaults reflect the canonical TP template currently in
# 01_table_migration/kg_publish/template.json. Override per session as needed.
DEFAULT_MAIN_CLASS = "ai.prevalent.icebergmigrate.IcebergMigrate"
DEFAULT_IMAGE = (
    "docker.io/prevalentai/spark:"
    "4-1-0-3.5.5-2.13-iceberg-v1-9-bookworm-12.10-20250428-slim"
)
DEFAULT_NAMESPACE          = "prod"
DEFAULT_APP_NAME_META      = "tpicebergmigrator"
DEFAULT_SERVICE_ACCOUNT    = "spark"
DEFAULT_IMAGE_PULL_SECRET  = "docker-secret"
DEFAULT_SPARK_VERSION      = "3.5.1"
DEFAULT_DRIVER_JOBTYPE     = "spark-driver"
DEFAULT_EXECUTOR_JOBTYPE   = "medium"
DEFAULT_OUTPUT_SCHEMA      = ""           # filled per-row by migrate.py


def build_template(
    *,
    connection: dict,
    sparkapp: dict,
    main_application_file: str,
    main_class: str = DEFAULT_MAIN_CLASS,
    image: str = DEFAULT_IMAGE,
    namespace: str = DEFAULT_NAMESPACE,
    app_name_meta: str = DEFAULT_APP_NAME_META,
    service_account: str = DEFAULT_SERVICE_ACCOUNT,
    image_pull_secret: str = DEFAULT_IMAGE_PULL_SECRET,
    spark_version: str = DEFAULT_SPARK_VERSION,
    driver_jobtype: str = DEFAULT_DRIVER_JOBTYPE,
    executor_jobtype: str = DEFAULT_EXECUTOR_JOBTYPE,
    output_schema: str = DEFAULT_OUTPUT_SCHEMA,
    extra_spark_conf: dict[str, str] | None = None,
) -> dict:
    """
    Build the SparkApplication CR. Returns a Python dict ready to be
    json.dumped.

    `connection` keys (all required):
        source_hms_uri, target_hms_uri,
        source_warehouse, target_warehouse,
        default_fs,
        azure_tenant, azure_client_id

    `sparkapp` keys (all required):
        driver_cores, driver_memory,
        executor_cores, executor_memory, executor_instances, executor_core_request,
        oidc_url
    """
    sparkapp_keys = {
        "driver_cores", "driver_memory",
        "executor_cores", "executor_memory", "executor_instances",
        "executor_core_request", "oidc_url",
    }
    connection_keys = {
        "source_hms_uri", "target_hms_uri",
        "source_warehouse", "target_warehouse",
        "default_fs",
        "event_log_dir",
        "azure_tenant", "azure_client_id",
    }
    missing_c = connection_keys - set(connection)
    missing_s = sparkapp_keys   - set(sparkapp)
    if missing_c:
        raise ValueError(f"connection missing keys: {sorted(missing_c)}")
    if missing_s:
        raise ValueError(f"sparkapp missing keys: {sorted(missing_s)}")

    # Event log path is explicit in `connection` — historically it lives in a
    # separate logs storage account (e.g. tp-prod-logs), NOT under the data
    # warehouse default_fs, so don't derive it from default_fs.
    event_log_dir = connection["event_log_dir"].rstrip("/") + "/"

    spark_conf: dict[str, str] = {
        # Event log + auth
        "spark.eventLog.dir": event_log_dir,
        "spark.eventLog.enabled": "true",
        "spark.kubernetes.authenticate.driver.serviceAccountName": service_account,
        "spark.kubernetes.authenticate.executor.serviceAccountName": service_account,
        # Filesystem + catalogs
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
        # IcebergMigrate-specific knobs
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.iceberg.merge-schema": "true",
        "spark.sql.iceberg.set-all-nullable-field": "true",
        "spark.sql.iceberg.check-ordering": "false",
        "spark.sds.iceberg.table.write.parquet.compression-codec": "snappy",
        "spark.sds.iceberg.table.write.spark.accept-any-schema": "true",
        "spark.sds.iceberg.table.write.distribution-mode": "none",
        "spark.sds.iceberg.read.schema-evolution.enabled": "true",
        "spark.sds.iceberg.read.schema-evolution.conflict-strategy": "BACKUP_EXISTING_COLUMN",
        # Decommission storage (spot tolerant)
        "spark.storage.decommission.fallbackStorage.path": (
            f"{connection['default_fs'].rstrip('/')}/spark-decomission-storage/"
        ),
        "spark.storage.decommission.rddBlocks.enabled": "true",
        "spark.storage.decommission.shuffleBlocks.enabled": "true",
        "spark.storage.decommission.enabled": "true",
        "spark.storage.decommission.fallbackStorage.cleanUp": "true",
        # SDS REST OIDC
        "spark.sds.restapi.oidcAuthEnabled": "true",
        "spark.kubernetes.driver.secretKeyRef.OIDC_CLIENT_ID": "external-secret-vault-prod:clientId",
        "spark.kubernetes.driver.secretKeyRef.OIDC_CLIENT_SECRET": "external-secret-vault-prod:clientSecret",
        "spark.sds.restapi.oidcUrl": sparkapp["oidc_url"],
        # Azure ABFS via workload identity
        "spark.hadoop.fs.azure.account.auth.type": "OAuth",
        "spark.hadoop.fs.azure.account.oauth.provider.type": (
            "org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider"
        ),
        "spark.hadoop.fs.azure.account.oauth2.msi.tenant": connection["azure_tenant"],
        "spark.hadoop.fs.azure.account.oauth2.client.id": connection["azure_client_id"],
        # Prometheus metrics
        "spark.ui.prometheus.enabled": "true",
        "spark.eventLog.logStageExecutorMetrics": "true",
        "spark.executor.processTreeMetrics.enabled": "true",
        "spark.kubernetes.driver.annotation.prometheus.io/scrape": "true",
        "spark.kubernetes.driver.annotation.prometheus.io/path": "/metrics/executors/prometheus/",
        "spark.kubernetes.driver.label.monitored-by": "prometheus",
        "spark.kubernetes.driver.annotation.prometheus.io/port": "4040",
        "spark.metrics.namespace": namespace,
        "spark.metrics.conf.*.sink.prometheusServlet.class":
            "org.apache.spark.metrics.sink.PrometheusServlet",
        "spark.metrics.conf.*.sink.prometheusServlet.path": "/metrics/prometheus",
        "spark.metrics.conf.master.sink.prometheusServlet.path": "/metrics/master/prometheus",
        "spark.metrics.conf.applications.sink.prometheusServlet.path": "/metrics/applications/prometheus",
        # Misc
        "spark.sql.caseSensitive": "true",
        "spark.sql.files.maxPartitionBytes": "128MB",
        "spark.default.parallelism": "4",
        "spark.app.name": "iceberg-table-migrator-job",
    }
    if extra_spark_conf:
        spark_conf.update(extra_spark_conf)

    spec: dict = {
        "driver": {
            "annotations": {
                "sidecar.istio.io/inject": "true",
                "cluster-autoscaler.kubernetes.io/safe-to-evict": "false",
            },
            "env": [],
            "affinity": _job_affinity(driver_jobtype),
            "tolerations": [
                {"effect": "NoSchedule", "key": "job-resource",
                 "operator": "Equal", "value": driver_jobtype},
            ],
            "labels": _job_labels(component="driver"),
            "serviceAccount": service_account,
            "cores": int(sparkapp["driver_cores"]),
            "memory": str(sparkapp["driver_memory"]),
        },
        "executor": {
            "annotations": {
                "sidecar.istio.io/inject": "true",
                "cluster-autoscaler.kubernetes.io/safe-to-evict": "false",
            },
            "affinity": _job_affinity(executor_jobtype),
            "tolerations": [
                {"effect": "NoSchedule", "key": "job-resource",
                 "operator": "Equal", "value": executor_jobtype},
                {"key": "kubernetes.azure.com/scalesetpriority", "value": "spot",
                 "operator": "Equal", "effect": "NoSchedule"},
            ],
            "env": [],
            "labels": _job_labels(component="executor"),
            "cores": int(sparkapp["executor_cores"]),
            "memory": str(sparkapp["executor_memory"]),
            "instances": int(sparkapp["executor_instances"]),
            "coreRequest": str(sparkapp["executor_core_request"]),
        },
        "sparkConf": spark_conf,
        "image": image,
        "imagePullPolicy": "Always",
        "imagePullSecrets": [image_pull_secret],
        "mode": "cluster",
        "restartPolicy": {"type": "Never"},
        "sparkVersion": spark_version,
        "type": "Scala",
        "arguments": [
            "--spark-service", "spark",
            "--tableName", "",
            "--listOfPartitionColumns", "",
            "--outputSchema", output_schema,
        ],
        "mainApplicationFile": main_application_file,
        "mainClass": main_class,
    }

    return {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "namespace": namespace,
            "name": app_name_meta,
        },
        "spec": spec,
    }


def write_template(template: dict, path: str | Path) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(template, indent=4) + "\n", encoding="utf-8")
    return p


def _job_affinity(jobtype_value: str) -> dict:
    return {
        "nodeAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": {
                "nodeSelectorTerms": [
                    {
                        "matchExpressions": [
                            {
                                "key": "jobtype",
                                "operator": "In",
                                "values": [jobtype_value],
                            }
                        ]
                    }
                ]
            }
        }
    }


def _job_labels(*, component: str) -> dict[str, str]:
    return {
        "application_component_name": f"spark_job_{component}",
        "app.kubernetes.io/name": f"pai-spark-{component}",
        "version": "3.2.2",
        "application_name": "spark_job",
        "sds_app_type": "application",
        "azure.workload.identity/use": "true",
        "job_name": "iceberg_table_migrator",
    }


__all__ = [
    "DEFAULT_MAIN_CLASS",
    "DEFAULT_IMAGE",
    "DEFAULT_NAMESPACE",
    "build_template",
    "write_template",
]
