package ai.prevalent.icebergmigrate

import ai.prevalent.icebergmigrate.cloudfs.CloudFsConfig
import ai.prevalent.sdspecore.sparkbase.SDSSparkBase
import ai.prevalent.sdspecore.sparkbase.table.iceberg.SDSIcebergConnect
import ai.prevalent.sdspecore.sparkbase.table.{SDSTableReaderFactory, SDSTableWriterFactory}
import org.apache.spark.sql.functions.{col, days, hours}
import org.apache.spark.sql.{Column, SparkSession}

/**
 * Multi-cloud fork of sds-platform-apps/utility-jobs IcebergMigrate.
 *
 * Body is byte-identical to the original execute() logic; only the first
 * three lines are new — they apply the right Hadoop FS config from CLI
 * args so the same JAR runs against Azure ABFS / AWS S3A / GCS without
 * any changes to the SparkApplication CR's sparkConf block.
 */
object IcebergMigrate
    extends SDSIcebergConnect
    with SDSSparkBase[IcebergMigrateArgs] {

  override def execute(params: IcebergMigrateArgs): Unit = {
    // --- cloud-agnostic bootstrap: one new block, then identical to source ---
    val cloudFs = CloudFsConfig.fromArgs(
      provider             = params.cloudProvider,
      azureTenant          = params.azureTenant,
      azureClientId        = params.azureClientId,
      azureStorageAccount  = params.azureStorageAccount,
      awsRegion            = params.awsRegion,
      awsRoleArn           = params.awsRoleArn,
      awsEndpoint          = params.awsEndpoint,
      awsPathStyle         = params.awsPathStyle,
      gcpProject           = params.gcpProject,
      gcpKeyFile           = params.gcpKeyFile,
    )
    cloudFs.applyTo(spark)
    LOGGER.info(s"CloudFsConfig applied: provider=${cloudFs.provider}")

    // --- original IcebergMigrate logic, verbatim ---

    val tableName = params.tableName
    val partitionColumnArg =
      if (!params.partitionColumns.isEmpty) params.partitionColumns.split(",")
      else Array.empty[String]
    val partitionColumns =
      if (!partitionColumnArg.isEmpty) {
        partitionColumnArg.map(x =>
          if (x.contains(':')) {
            val Array(partColumn, granularity) = x.split(':')
            getPartitionColumn(partColumn, granularity)
          } else col(x)
        )
      } else Array.empty[Column]

    var dataDF = SDSTableReaderFactory.getDefault(spark).read(tableName)
    LOGGER.info("READ FINISHED")
    LOGGER.info("Starting to fetch table properties")
    val readCatalog = spark.conf.get("spark.sds.hive.read.catalog", "iceberg_catalog")
    val tableProperties = spark
      .sql(s"SHOW TBLPROPERTIES $readCatalog.$tableName")
      .collect()
      .map(row => row.getString(0) -> row.getString(1))
      .filter { case (key, _) => key.startsWith("graph") }
      .toMap
    if (!params.filterExpression.isEmpty) {
      dataDF = dataDF.filter(params.filterExpression)
    }
    val tableWriter = SDSTableWriterFactory.getDefault(spark)
    val outputTableName =
      if (!params.outputSchema.isEmpty)
        params.outputSchema + "." + tableName.split("\\.")(1)
      else tableName
    LOGGER.info(s"WRITE STARTING TO TABLE $outputTableName")
    tableWriter.overwritePartition(dataDF, outputTableName, partitionColumns)
    LOGGER.info(s"$outputTableName WRITE FINISHED")
    LOGGER.info(s"Starting to write Table Properties to $tableName")
    val writeCatalog = spark.conf.get("spark.sds.hive.write.catalog", "iceberg_catalog")
    tableProperties.foreach { case (key, value) =>
      spark.sql(
        s"ALTER TABLE $writeCatalog.$outputTableName SET TBLPROPERTIES ('$key'='$value')"
      )
    }
  }

  def getPartitionColumn(colName: String, granularity: String): Column = {
    if (granularity == "days")       days(col(colName))
    else if (granularity == "hours") hours(col(colName))
    else col(colName)
  }

  override def getInitParams: IcebergMigrateArgs = new IcebergMigrateArgs
}
