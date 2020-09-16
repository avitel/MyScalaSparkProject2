package logic.replicaReader

import scala.collection.mutable.ListBuffer
import entity.ColumnDescr
import logic.Configuration
import logic.columnWrapper.ColumnWrapper
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, concat, expr}
import org.apache.spark.sql._
import utils.HiveUtils

class PartitionedReplicaReader2(val conf: Configuration, val spark: SparkSession, val hiveUtils: HiveUtils, val wrapper: ColumnWrapper) extends ReplicaReader {
  var logger = Logger.getLogger("sparkETL.PartitionedReplicaReader2")

  def getHiveWrappedColumns(columns: ListBuffer[ColumnDescr], wrapperType: String): Array[Column]={
    columns.map(row => wrapper.wrapWithPattern(row.column_name, row.data_type, wrapperType)).map(str => expr(str)).toArray
  }

  def getPartitionsPaths(replicaPath: String, source_df: Dataset[Row]): Array[String] = {
    val ctl_utrnno_part_list  = source_df.select(concat(
      expr("'ctl_node_id_part='"),
      expr("CAST(CEIL(NODE_ID) as string)"),
      expr("'/'"),
      expr("'ctl_utrnno_part='"),
      expr("CAST(CEIL(NODE_ID) as string)"),
      expr("'-'"),
      expr("CAST(CEIL(UTRNNO/16777216) as string)")))
      .distinct.collect().map{_.getString(0)}

    val partList = ctl_utrnno_part_list.map(s => replicaPath + "/" + s)
    logger.info("count of partition : " + partList.size)
    logger.info("list of partitions : " )
    partList.foreach(logger.info)
    partList
  }

  def getReplicaDF(columns: ListBuffer[ColumnDescr], source_df: Dataset[Row]): Dataset[Row] = {
    val replicaPath  = hiveUtils.getTablePath(conf.replicaHiveSchema, conf.replicaHiveTable)
    val partitionsPaths: Array[String] = getPartitionsPaths(replicaPath, source_df)
    val df = spark.read.parquet(partitionsPaths: _*)
    val wrappedColumns = getHiveWrappedColumns(columns, "hive")
    printReplicaWrappedColumn(wrappedColumns)
    df.select(wrappedColumns: _*)
  }

  def printReplicaWrappedColumn(wrappedColumns: Array[Column]) = {
    logger.info("Replica wrapped columns : ")
    wrappedColumns.foreach(logger.info)
  }
}