package utils

import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTablePartition, ExternalCatalog}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.api.FieldSchema

import scala.collection.JavaConverters._
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.log4j.Logger

class HiveUtils(val cat: ExternalCatalog, val hiveMetaStoreClient: HiveMetaStoreClient) {
	val logger = Logger.getLogger("sparkETL.HiveUtils")

	def createHivePartition(schema: String, table: String, partitionField: String, partitionValut: String, location: String, ignoreIfExists: Boolean): Unit ={
		val locationPath = new Path(location)
		val sf = CatalogStorageFormat(
			scala.Option.apply(locationPath.toUri)
			, scala.Option.apply("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
			, scala.Option.apply("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat")
			, scala.Option.apply("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
			, true
			, Map()
		)
		val part = CatalogTablePartition(Map(partitionField->partitionValut), sf, Map())
		cat.createPartitions(schema, table, Seq(part), ignoreIfExists)
	}

	def getTablePath(schemaName: String, tableName: String): String = {
		logger.info("getting path of table : " + schemaName + "." + tableName)
		val table = cat.getTable(schemaName, tableName)
		table.location.toString
	}

	def getTableFields(schemaName: String, tableName: String): List[FieldSchema] = hiveMetaStoreClient.getFields(schemaName, tableName).asScala.toList
}
