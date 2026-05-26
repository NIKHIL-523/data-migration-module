package ai.prevalent.icebergmigrate

import ai.prevalent.sdspecore.jobbase.DefaultJobArgs
import picocli.CommandLine.{Option => commandlineOption}

/**
 * Args bag for the multi-cloud fork. Keeps the original four flags
 * (tableName / partition / filter / outputSchema) and adds a
 * --cloudProvider switch plus per-provider knobs.
 *
 * The Azure flags are kept to match the names that Prevalent's existing
 * SparkApplication CRs already pass in production; new AWS/GCP flags
 * follow the same `--<prefix><Knob>` convention.
 */
class IcebergMigrateArgs extends DefaultJobArgs {

  // --- original flags (unchanged contract) ---

  @commandlineOption(names = Array("--tableName"),
    description = Array("tables to be migrated"), required = false)
  var tableName: String = _

  @commandlineOption(names = Array("--listOfPartitionColumns"),
    description = Array("array of partition columns"), required = false)
  var partitionColumns: String = ""

  @commandlineOption(names = Array("--filterExpression"),
    description = Array("filter expression for read"), required = false)
  var filterExpression: String = ""

  @commandlineOption(names = Array("--outputSchema"),
    description = Array("output schema to write"), required = false)
  var outputSchema: String = ""

  // --- new cloud-agnostic flags ---

  @commandlineOption(names = Array("--cloudProvider"),
    description = Array("azure | aws | gcp (required)"), required = false)
  var cloudProvider: String = ""

  // Azure
  @commandlineOption(names = Array("--azureTenant"),
    description = Array("Entra tenant ID (required when --cloudProvider=azure)"), required = false)
  var azureTenant: String = ""

  @commandlineOption(names = Array("--azureClientId"),
    description = Array("Workload-identity client ID (required when --cloudProvider=azure)"), required = false)
  var azureClientId: String = ""

  @commandlineOption(names = Array("--azureStorageAccount"),
    description = Array("Storage account name to scope the WI auth block to"), required = false)
  var azureStorageAccount: String = ""

  // AWS
  @commandlineOption(names = Array("--awsRegion"),
    description = Array("AWS region (required when --cloudProvider=aws)"), required = false)
  var awsRegion: String = ""

  @commandlineOption(names = Array("--awsRoleArn"),
    description = Array("Optional role ARN to assume on top of the IRSA token"), required = false)
  var awsRoleArn: String = ""

  @commandlineOption(names = Array("--awsEndpoint"),
    description = Array("Custom S3 endpoint URL (MinIO, on-prem). Empty = real S3"), required = false)
  var awsEndpoint: String = ""

  @commandlineOption(names = Array("--awsPathStyle"),
    description = Array("Force path-style S3 access (true for MinIO)"), required = false)
  var awsPathStyle: Boolean = false

  // GCP
  @commandlineOption(names = Array("--gcpProject"),
    description = Array("GCP project ID (required when --cloudProvider=gcp)"), required = false)
  var gcpProject: String = ""

  @commandlineOption(names = Array("--gcpKeyFile"),
    description = Array("Optional service-account JSON keyfile path; default = ADC / Workload Identity"), required = false)
  var gcpKeyFile: String = ""
}
