package ai.prevalent.icebergmigrate.cloudfs

import org.apache.spark.sql.SparkSession

/**
 * Cloud-specific Hadoop FileSystem configuration applied to a live SparkSession.
 *
 * Each concrete case emits the right `spark.hadoop.fs.*` keys for its
 * provider; Hadoop reads them lazily on first FS access, so calling
 * [[CloudFsConfig.applyTo]] at the top of `execute()` is sufficient.
 *
 * The IcebergMigrate Scala layer is itself cloud-agnostic (no SDK imports);
 * this trait is the single point where cloud-specific configuration enters
 * the runtime.
 */
sealed trait CloudFsConfig {
  def provider: String
  def hadoopConf: Map[String, String]

  // Keys are stored with the spark.hadoop. prefix (the form a SparkApplication
  // CR uses in sparkConf). At runtime, propagate the unprefixed form into the
  // live Hadoop configuration so the FileSystem layer actually picks them up —
  // spark.conf.set after the SparkContext is already running does not bridge
  // back to Hadoop conf.
  final def applyTo(spark: SparkSession): Unit = {
    val hc = spark.sparkContext.hadoopConfiguration
    hadoopConf.foreach { case (k, v) =>
      spark.conf.set(k, v)
      if (k.startsWith("spark.hadoop.")) hc.set(k.stripPrefix("spark.hadoop."), v)
    }
  }
}

object CloudFsConfig {

  /** Azure ABFS with Workload Identity (AKS + federated identity). */
  final case class Azure(
      tenantId: String,
      clientId: String,
      storageAccount: Option[String] = None,
  ) extends CloudFsConfig {
    val provider = "azure"
    val hadoopConf: Map[String, String] = {
      val base = Map(
        "spark.hadoop.fs.azure.account.auth.type" -> "OAuth",
        "spark.hadoop.fs.azure.account.oauth.provider.type" ->
          "org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider",
        "spark.hadoop.fs.azure.account.oauth2.msi.tenant"   -> tenantId,
        "spark.hadoop.fs.azure.account.oauth2.client.id"    -> clientId,
      )
      // ABFS allows scoping the auth block per storage account, which is
      // what production does for tp-prod-datalake / tp-prod-logs. When the
      // caller knows the account name, scope it to avoid cross-account
      // leakage of the same identity.
      storageAccount.fold(base) { acct =>
        val scoped = base.map { case (k, v) =>
          k.replaceFirst("fs\\.azure\\.account\\.", s"fs.azure.account.$acct.dfs.core.windows.net.") -> v
        }
        base ++ scoped
      }
    }
  }

  /**
   * AWS S3A. With IRSA on EKS / GKE Workload-Identity-style pod identity,
   * the AWS SDK picks credentials up from env + projected service-account
   * tokens automatically — `WebIdentityTokenCredentialsProvider` in the
   * chain handles it. Pass `roleArn` only if you need to assume a
   * different role.
   */
  final case class Aws(
      region: String,
      roleArn: Option[String] = None,
      endpoint: Option[String] = None,
      pathStyleAccess: Boolean = false,
  ) extends CloudFsConfig {
    val provider = "aws"
    val hadoopConf: Map[String, String] = {
      val providers = roleArn match {
        case Some(_) =>
          // AssumedRoleCredentialProvider chain: IRSA token → STS AssumeRole
          "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider"
        case None =>
          // DefaultAWSCredentialsProviderChain covers env vars, system
          // properties, WebIdentity (IRSA), shared profile, and EC2 metadata
          // in that order. Using it alone avoids the NPE the standalone
          // WebIdentity provider throws when AWS_ROLE_ARN is unset.
          "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
      }
      // SSL follows the endpoint scheme: explicit http:// (e.g. MinIO over
      // plain HTTP) disables SSL; everything else (https:// or no override)
      // keeps it on, matching production S3 / IRSA behavior.
      val sslEnabled = endpoint match {
        case Some(ep) if ep.startsWith("http://") => "false"
        case _                                     => "true"
      }
      val base = Map(
        "spark.hadoop.fs.s3a.aws.credentials.provider" -> providers,
        "spark.hadoop.fs.s3a.endpoint.region"          -> region,
        "spark.hadoop.fs.s3a.path.style.access"        -> pathStyleAccess.toString,
        // Sensible defaults shared across IRSA + assumed-role setups.
        "spark.hadoop.fs.s3a.connection.ssl.enabled"   -> sslEnabled,
        "spark.hadoop.fs.s3a.fast.upload"              -> "true",
      )
      val withRole = roleArn.fold(base)(arn =>
        base + ("spark.hadoop.fs.s3a.assumed.role.arn" -> arn)
      )
      endpoint.fold(withRole)(ep =>
        withRole + ("spark.hadoop.fs.s3a.endpoint" -> ep)
      )
    }
  }

  /**
   * GCS via the gcs-connector. Workload Identity on GKE injects the
   * service-account token via env + metadata server; the connector picks
   * it up when `auth.service.account.enable=true` and no key file is set.
   */
  final case class Gcp(
      projectId: String,
      serviceAccountKeyFile: Option[String] = None,
  ) extends CloudFsConfig {
    val provider = "gcp"
    val hadoopConf: Map[String, String] = {
      val base = Map(
        "spark.hadoop.fs.gs.impl" ->
          "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        "spark.hadoop.fs.AbstractFileSystem.gs.impl" ->
          "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        "spark.hadoop.fs.gs.project.id"             -> projectId,
        "spark.hadoop.fs.gs.auth.service.account.enable" -> "true",
        "spark.hadoop.google.cloud.auth.type"       -> "APPLICATION_DEFAULT",
      )
      serviceAccountKeyFile.fold(base) { path =>
        // Explicit key-file path overrides Workload Identity / ADC.
        base ++ Map(
          "spark.hadoop.google.cloud.auth.type"                -> "SERVICE_ACCOUNT_JSON_KEYFILE",
          "spark.hadoop.fs.gs.auth.service.account.json.keyfile" -> path,
        )
      }
    }
  }

  /**
   * Build a CloudFsConfig from the parsed [[ai.prevalent.icebergmigrate.IcebergMigrateArgs]] bag.
   * Kept as a thin factory here so the args class doesn't take a Spark dep
   * just for this glue.
   */
  def fromArgs(
      provider: String,
      azureTenant: String,
      azureClientId: String,
      azureStorageAccount: String,
      awsRegion: String,
      awsRoleArn: String,
      awsEndpoint: String,
      awsPathStyle: Boolean,
      gcpProject: String,
      gcpKeyFile: String,
  ): CloudFsConfig = provider.toLowerCase match {
    case "azure" =>
      require(azureTenant.nonEmpty,   "--azureTenant required for cloudProvider=azure")
      require(azureClientId.nonEmpty, "--azureClientId required for cloudProvider=azure")
      Azure(
        tenantId       = azureTenant,
        clientId       = azureClientId,
        storageAccount = Option(azureStorageAccount).filter(_.nonEmpty),
      )

    case "aws" =>
      require(awsRegion.nonEmpty, "--awsRegion required for cloudProvider=aws")
      Aws(
        region          = awsRegion,
        roleArn         = Option(awsRoleArn).filter(_.nonEmpty),
        endpoint        = Option(awsEndpoint).filter(_.nonEmpty),
        pathStyleAccess = awsPathStyle,
      )

    case "gcp" =>
      require(gcpProject.nonEmpty, "--gcpProject required for cloudProvider=gcp")
      Gcp(
        projectId             = gcpProject,
        serviceAccountKeyFile = Option(gcpKeyFile).filter(_.nonEmpty),
      )

    case other =>
      throw new IllegalArgumentException(
        s"Unknown --cloudProvider '$other'. Expected one of: azure, aws, gcp."
      )
  }
}
