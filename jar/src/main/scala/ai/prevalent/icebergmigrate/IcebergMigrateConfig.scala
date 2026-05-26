package ai.prevalent.icebergmigrate

import scala.collection.mutable

case class IcebergMigrateConfig(
    addField: Option[Map[String, String]],
    dropField: Option[Array[String]],
    partitionField: Option[mutable.LinkedHashMap[String, String]],
)
