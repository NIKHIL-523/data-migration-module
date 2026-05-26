package ai.prevalent.icebergmigrate.cloudfs

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Pure-config tests for CloudFsConfig. No Spark dependency — exercises
 * the key emission and factory validation surface only. The applyTo()
 * side-effect on a live SparkSession is covered by the docker-compose
 * integration tests in jar/it/.
 */
class CloudFsConfigSpec extends AnyFlatSpec with Matchers {

  // --- Azure -------------------------------------------------------------

  "Azure" should "emit OAuth + WorkloadIdentityTokenProvider keys" in {
    val cfg = CloudFsConfig.Azure(tenantId = "t-1", clientId = "c-1")
    cfg.provider shouldBe "azure"
    cfg.hadoopConf("spark.hadoop.fs.azure.account.auth.type") shouldBe "OAuth"
    cfg.hadoopConf("spark.hadoop.fs.azure.account.oauth.provider.type") should include(
      "WorkloadIdentityTokenProvider"
    )
    cfg.hadoopConf("spark.hadoop.fs.azure.account.oauth2.msi.tenant")   shouldBe "t-1"
    cfg.hadoopConf("spark.hadoop.fs.azure.account.oauth2.client.id")    shouldBe "c-1"
  }

  it should "additionally scope keys to the storage account when given" in {
    val cfg = CloudFsConfig.Azure(
      tenantId       = "t-1",
      clientId       = "c-1",
      storageAccount = Some("tpprodlake"),
    )
    cfg.hadoopConf(
      "spark.hadoop.fs.azure.account.tpprodlake.dfs.core.windows.net.oauth2.msi.tenant"
    ) shouldBe "t-1"
    cfg.hadoopConf(
      "spark.hadoop.fs.azure.account.tpprodlake.dfs.core.windows.net.oauth2.client.id"
    ) shouldBe "c-1"
  }

  // --- AWS ---------------------------------------------------------------

  "AWS" should "default to a WebIdentity-first provider chain" in {
    val cfg = CloudFsConfig.Aws(region = "us-east-1")
    cfg.provider shouldBe "aws"
    cfg.hadoopConf("spark.hadoop.fs.s3a.aws.credentials.provider") should
      startWith("com.amazonaws.auth.WebIdentityTokenCredentialsProvider")
    cfg.hadoopConf("spark.hadoop.fs.s3a.endpoint.region")   shouldBe "us-east-1"
    cfg.hadoopConf("spark.hadoop.fs.s3a.path.style.access") shouldBe "false"
  }

  it should "switch to AssumedRoleCredentialProvider when a role ARN is given" in {
    val cfg = CloudFsConfig.Aws(
      region  = "us-east-1",
      roleArn = Some("arn:aws:iam::123:role/migrate"),
    )
    cfg.hadoopConf("spark.hadoop.fs.s3a.aws.credentials.provider") shouldBe
      "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider"
    cfg.hadoopConf("spark.hadoop.fs.s3a.assumed.role.arn") shouldBe
      "arn:aws:iam::123:role/migrate"
  }

  it should "set custom endpoint + path style for MinIO-like targets" in {
    val cfg = CloudFsConfig.Aws(
      region          = "us-east-1",
      endpoint        = Some("http://minio:9000"),
      pathStyleAccess = true,
    )
    cfg.hadoopConf("spark.hadoop.fs.s3a.endpoint")          shouldBe "http://minio:9000"
    cfg.hadoopConf("spark.hadoop.fs.s3a.path.style.access") shouldBe "true"
  }

  // --- GCP ---------------------------------------------------------------

  "GCP" should "emit GoogleHadoopFileSystem + ADC keys by default" in {
    val cfg = CloudFsConfig.Gcp(projectId = "proj-1")
    cfg.provider shouldBe "gcp"
    cfg.hadoopConf("spark.hadoop.fs.gs.impl") should include("GoogleHadoopFileSystem")
    cfg.hadoopConf("spark.hadoop.google.cloud.auth.type") shouldBe "APPLICATION_DEFAULT"
    cfg.hadoopConf("spark.hadoop.fs.gs.project.id")       shouldBe "proj-1"
  }

  it should "switch to JSON keyfile auth when given" in {
    val cfg = CloudFsConfig.Gcp(
      projectId             = "proj-1",
      serviceAccountKeyFile = Some("/secrets/sa.json"),
    )
    cfg.hadoopConf("spark.hadoop.google.cloud.auth.type") shouldBe "SERVICE_ACCOUNT_JSON_KEYFILE"
    cfg.hadoopConf("spark.hadoop.fs.gs.auth.service.account.json.keyfile") shouldBe
      "/secrets/sa.json"
  }

  // --- factory -----------------------------------------------------------

  "fromArgs" should "dispatch to the right provider case" in {
    val az = CloudFsConfig.fromArgs(
      provider="azure", azureTenant="t", azureClientId="c", azureStorageAccount="",
      awsRegion="", awsRoleArn="", awsEndpoint="", awsPathStyle=false,
      gcpProject="", gcpKeyFile="",
    )
    az shouldBe a [CloudFsConfig.Azure]

    val aws = CloudFsConfig.fromArgs(
      provider="aws", azureTenant="", azureClientId="", azureStorageAccount="",
      awsRegion="us-east-1", awsRoleArn="", awsEndpoint="", awsPathStyle=false,
      gcpProject="", gcpKeyFile="",
    )
    aws shouldBe a [CloudFsConfig.Aws]

    val gcp = CloudFsConfig.fromArgs(
      provider="gcp", azureTenant="", azureClientId="", azureStorageAccount="",
      awsRegion="", awsRoleArn="", awsEndpoint="", awsPathStyle=false,
      gcpProject="proj-1", gcpKeyFile="",
    )
    gcp shouldBe a [CloudFsConfig.Gcp]
  }

  it should "reject unknown providers" in {
    a [IllegalArgumentException] should be thrownBy CloudFsConfig.fromArgs(
      provider="oracle", azureTenant="", azureClientId="", azureStorageAccount="",
      awsRegion="", awsRoleArn="", awsEndpoint="", awsPathStyle=false,
      gcpProject="", gcpKeyFile="",
    )
  }

  it should "reject Azure args missing tenant/client" in {
    an [IllegalArgumentException] should be thrownBy CloudFsConfig.fromArgs(
      provider="azure", azureTenant="", azureClientId="c", azureStorageAccount="",
      awsRegion="", awsRoleArn="", awsEndpoint="", awsPathStyle=false,
      gcpProject="", gcpKeyFile="",
    )
    an [IllegalArgumentException] should be thrownBy CloudFsConfig.fromArgs(
      provider="azure", azureTenant="t", azureClientId="", azureStorageAccount="",
      awsRegion="", awsRoleArn="", awsEndpoint="", awsPathStyle=false,
      gcpProject="", gcpKeyFile="",
    )
  }

  it should "reject AWS args missing region and GCP args missing project" in {
    an [IllegalArgumentException] should be thrownBy CloudFsConfig.fromArgs(
      provider="aws", azureTenant="", azureClientId="", azureStorageAccount="",
      awsRegion="", awsRoleArn="", awsEndpoint="", awsPathStyle=false,
      gcpProject="", gcpKeyFile="",
    )
    an [IllegalArgumentException] should be thrownBy CloudFsConfig.fromArgs(
      provider="gcp", azureTenant="", azureClientId="", azureStorageAccount="",
      awsRegion="", awsRoleArn="", awsEndpoint="", awsPathStyle=false,
      gcpProject="", gcpKeyFile="",
    )
  }
}
