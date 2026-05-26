"""
Build the SparkApplication CR template.json from a user-supplied connection
config + Spark sizing/k8s config. This is the file the migrate.py driver
fills in per row (--tableName, --listOfPartitionColumns, per-row outputSchema)
before `kubectl apply`.

Two-stage flow in the notebook:
  Cell 1 -> connection_config (catalogs, HMS, FS, cloud auth)  ----+
  Cell 4 -> sparkapp_config   (driver/executor sizing, k8s)   ----+--> template.json

Multi-cloud: `connection` carries a `cloud` block:

    cloud = {
        "provider": "azure" | "aws" | "gcp",
        # azure: tenant + client_id (workload identity); optional storage_account
        "azure_tenant":    "...", "azure_client_id": "...",
        "azure_storage_account": "tpprodlake",                # optional, scopes auth
        # aws: region, optional role to assume, optional custom endpoint
        "aws_region": "us-east-1", "aws_role_arn": "...", "aws_endpoint": "",
        "aws_path_style": False,
        # gcp: project, optional key file (default = ADC / Workload Identity)
        "gcp_project": "...", "gcp_key_file": "",
    }

The default IcebergMigrate sparkConf knobs (snappy compression, distribution
mode none, Prometheus metrics, decommission storage) are baked in. Cloud
auth / image / executor-toleration blocks are emitted from `cloud.provider`.

Backward-compat: if `connection` has top-level `azure_tenant` / `azure_client_id`
and no `cloud` block, the builder treats it as `provider=azure` automatically.
"""

from __future__ import annotations

import json
from pathlib import Path


# --- defaults --------------------------------------------------------------

DEFAULT_MAIN_CLASS = "ai.prevalent.icebergmigrate.IcebergMigrate"

# Per-provider default container images. All three carry the same Spark +
# Iceberg runtime; what differs is which Hadoop FS connector jars are
# baked in (hadoop-azure, hadoop-aws, gcs-connector). Override per call
# with `image=` if you have a non-default image for that cloud.
DEFAULT_IMAGES: dict[str, str] = {
    "azure": (
        "docker.io/prevalentai/spark:"
        "4-1-0-3.5.5-2.13-iceberg-v1-9-bookworm-12.10-20250428-slim"
    ),
    # Placeholders until the AWS/GCP-flavored images are published. Update
    # these once the corresponding image tags exist in the registry.
    "aws":   "docker.io/prevalentai/spark:aws-3.5.5-2.13-iceberg-v1-9",
    "gcp":   "docker.io/prevalentai/spark:gcp-3.5.5-2.13-iceberg-v1-9",
}

DEFAULT_NAMESPACE          = "prod"
DEFAULT_APP_NAME_META      = "tpicebergmigrator"
DEFAULT_SERVICE_ACCOUNT    = "spark"
DEFAULT_IMAGE_PULL_SECRET  = "docker-secret"
DEFAULT_SPARK_VERSION      = "3.5.1"
DEFAULT_DRIVER_JOBTYPE     = "spark-driver"
DEFAULT_EXECUTOR_JOBTYPE   = "medium"
DEFAULT_OUTPUT_SCHEMA      = ""           # filled per-row by migrate.py

_VALID_PROVIDERS = {"azure", "aws", "gcp"}


# --- public API ------------------------------------------------------------


def build_template(
    *,
    connection: dict,
    sparkapp: dict,
    main_application_file: str,
    main_class: str = DEFAULT_MAIN_CLASS,
    image: str | None = None,             # default: provider-specific
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

    `connection` keys (all required, plus a `cloud` block):
        source_hms_uri, target_hms_uri,
        source_warehouse, target_warehouse,
        default_fs, event_log_dir,
        cloud = {"provider": "...", ...}        # see module docstring

    `sparkapp` keys (all required):
        driver_cores, driver_memory,
        executor_cores, executor_memory, executor_instances, executor_core_request,
        oidc_url
    """
    cloud = _normalize_cloud(connection)
    provider = cloud["provider"]

    sparkapp_keys = {
        "driver_cores", "driver_memory",
        "executor_cores", "executor_memory", "executor_instances",
        "executor_core_request", "oidc_url",
    }
    connection_keys = {
        "source_hms_uri", "target_hms_uri",
        "source_warehouse", "target_warehouse",
        "default_fs", "event_log_dir",
    }
    missing_c = connection_keys - set(connection)
    missing_s = sparkapp_keys   - set(sparkapp)
    if missing_c:
        raise ValueError(f"connection missing keys: {sorted(missing_c)}")
    if missing_s:
        raise ValueError(f"sparkapp missing keys: {sorted(missing_s)}")

    image = image or DEFAULT_IMAGES[provider]

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
        "spark.driver.maxResultSize": str(sparkapp.get("driver_max_result_size", "4g")),
        "spark.app.name": "iceberg-table-migrator-job",
    }
    # Provider-specific FS auth block. The JAR also applies the same keys
    # at runtime via CloudFsConfig — but emitting them here too lets the
    # SparkApplication CR be self-describing for debugging.
    spark_conf.update(_cloud_auth_conf(cloud))
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
            "labels": _job_labels(provider=provider, component="driver"),
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
                *_spot_tolerations(provider),
            ],
            "env": [],
            "labels": _job_labels(provider=provider, component="executor"),
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
            *_cloud_cli_args(cloud),
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


# --- cloud-specific helpers ------------------------------------------------


def _normalize_cloud(connection: dict) -> dict:
    """
    Return a `cloud` dict with a validated `provider` key.

    Accepts either a nested `connection["cloud"]` block (new) or the
    legacy top-level Azure keys (`azure_tenant` / `azure_client_id`).
    The legacy path is detected when no `cloud` key is present and at
    least `azure_tenant` is set.
    """
    cloud = connection.get("cloud")
    if cloud is None:
        if "azure_tenant" in connection and "azure_client_id" in connection:
            cloud = {
                "provider":         "azure",
                "azure_tenant":     connection["azure_tenant"],
                "azure_client_id":  connection["azure_client_id"],
                "azure_storage_account": connection.get("azure_storage_account", ""),
            }
        else:
            raise ValueError(
                "connection missing 'cloud' block (or legacy azure_tenant/azure_client_id)"
            )
    provider = cloud.get("provider", "").lower()
    if provider not in _VALID_PROVIDERS:
        raise ValueError(
            f"cloud.provider must be one of {sorted(_VALID_PROVIDERS)}, got {provider!r}"
        )
    cloud["provider"] = provider
    return cloud


def _cloud_auth_conf(cloud: dict) -> dict[str, str]:
    """Hadoop FS auth keys for the SparkApplication CR's sparkConf block."""
    p = cloud["provider"]
    if p == "azure":
        if "azure_tenant" not in cloud or "azure_client_id" not in cloud:
            raise ValueError("cloud.provider=azure requires azure_tenant + azure_client_id")
        base = {
            "spark.hadoop.fs.azure.account.auth.type": "OAuth",
            "spark.hadoop.fs.azure.account.oauth.provider.type":
                "org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider",
            "spark.hadoop.fs.azure.account.oauth2.msi.tenant":  cloud["azure_tenant"],
            "spark.hadoop.fs.azure.account.oauth2.client.id":   cloud["azure_client_id"],
        }
        acct = cloud.get("azure_storage_account") or ""
        if acct:
            scoped = {
                k.replace("fs.azure.account.",
                          f"fs.azure.account.{acct}.dfs.core.windows.net.", 1): v
                for k, v in base.items()
            }
            return {**base, **scoped}
        return base

    if p == "aws":
        region = cloud.get("aws_region") or ""
        if not region:
            raise ValueError("cloud.provider=aws requires aws_region")
        role_arn = cloud.get("aws_role_arn") or ""
        providers = (
            "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider"
            if role_arn else
            "com.amazonaws.auth.WebIdentityTokenCredentialsProvider,"
            "com.amazonaws.auth.ContainerCredentialsProvider,"
            "com.amazonaws.auth.InstanceProfileCredentialsProvider,"
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        )
        out = {
            "spark.hadoop.fs.s3a.aws.credentials.provider": providers,
            "spark.hadoop.fs.s3a.endpoint.region":          region,
            "spark.hadoop.fs.s3a.connection.ssl.enabled":   "true",
            "spark.hadoop.fs.s3a.fast.upload":              "true",
            "spark.hadoop.fs.s3a.path.style.access":
                str(bool(cloud.get("aws_path_style", False))).lower(),
        }
        if role_arn:
            out["spark.hadoop.fs.s3a.assumed.role.arn"] = role_arn
        endpoint = cloud.get("aws_endpoint") or ""
        if endpoint:
            out["spark.hadoop.fs.s3a.endpoint"] = endpoint
        return out

    if p == "gcp":
        project = cloud.get("gcp_project") or ""
        if not project:
            raise ValueError("cloud.provider=gcp requires gcp_project")
        out = {
            "spark.hadoop.fs.gs.impl":
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "spark.hadoop.fs.AbstractFileSystem.gs.impl":
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            "spark.hadoop.fs.gs.project.id":             project,
            "spark.hadoop.fs.gs.auth.service.account.enable": "true",
            "spark.hadoop.google.cloud.auth.type":       "APPLICATION_DEFAULT",
        }
        key_file = cloud.get("gcp_key_file") or ""
        if key_file:
            out["spark.hadoop.google.cloud.auth.type"] = "SERVICE_ACCOUNT_JSON_KEYFILE"
            out["spark.hadoop.fs.gs.auth.service.account.json.keyfile"] = key_file
        return out

    raise AssertionError(f"unreachable: provider={p!r}")


def _cloud_cli_args(cloud: dict) -> list[str]:
    """CLI args appended to the SparkApplication CR's spec.arguments for the JAR."""
    p = cloud["provider"]
    out = ["--cloudProvider", p]
    if p == "azure":
        out += [
            "--azureTenant",          cloud["azure_tenant"],
            "--azureClientId",        cloud["azure_client_id"],
        ]
        if cloud.get("azure_storage_account"):
            out += ["--azureStorageAccount", cloud["azure_storage_account"]]
    elif p == "aws":
        out += ["--awsRegion", cloud["aws_region"]]
        if cloud.get("aws_role_arn"):
            out += ["--awsRoleArn", cloud["aws_role_arn"]]
        if cloud.get("aws_endpoint"):
            out += ["--awsEndpoint", cloud["aws_endpoint"]]
        if cloud.get("aws_path_style"):
            out += ["--awsPathStyle", "true"]
    elif p == "gcp":
        out += ["--gcpProject", cloud["gcp_project"]]
        if cloud.get("gcp_key_file"):
            out += ["--gcpKeyFile", cloud["gcp_key_file"]]
    return out


def _spot_tolerations(provider: str) -> list[dict]:
    """Per-cloud spot/preemptible-node tolerations."""
    if provider == "azure":
        return [{
            "key": "kubernetes.azure.com/scalesetpriority", "value": "spot",
            "operator": "Equal", "effect": "NoSchedule",
        }]
    if provider == "aws":
        # Karpenter / capacity-type=spot is the common pattern on EKS.
        return [{
            "key": "karpenter.sh/capacity-type", "value": "spot",
            "operator": "Equal", "effect": "NoSchedule",
        }]
    if provider == "gcp":
        return [{
            "key": "cloud.google.com/gke-spot", "value": "true",
            "operator": "Equal", "effect": "NoSchedule",
        }]
    return []


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


def _job_labels(*, provider: str, component: str) -> dict[str, str]:
    base = {
        "application_component_name": f"spark_job_{component}",
        "app.kubernetes.io/name": f"pai-spark-{component}",
        "version": "3.2.2",
        "application_name": "spark_job",
        "sds_app_type": "application",
        "job_name": "iceberg_table_migrator",
    }
    if provider == "azure":
        # Required by AKS for the federated WorkloadIdentity webhook.
        base["azure.workload.identity/use"] = "true"
    return base


__all__ = [
    "DEFAULT_MAIN_CLASS",
    "DEFAULT_IMAGES",
    "DEFAULT_NAMESPACE",
    "build_template",
    "write_template",
]
