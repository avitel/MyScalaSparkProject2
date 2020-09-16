package logic.replicaReader

import entity.ColumnDescr
import logic.Configuration
import logic.columnWrapper.ColumnWrapper
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql._
import utils.HiveUtils

import scala.collection.mutable.ListBuffer

class SimpleReplicaReader(val conf: Configuration, val spark: SparkSession, val hiveUtils: HiveUtils, val wrapper: ColumnWrapper) extends ReplicaReader {
  var logger = Logger.getLogger("sparkETL.SimpleReplicaReader")

  def getHiveWrappedColumns(columns: ListBuffer[ColumnDescr], wrapperType: String): Array[Column]={
    columns.map(row => wrapper.wrapWithPattern(row.column_name, row.data_type, wrapperType)).map(str => expr(str)).toArray
  }

  def getReplicaDF(columns: ListBuffer[ColumnDescr], source_df: Dataset[Row]): Dataset[Row] = {
    val replicaPath  = hiveUtils.getTablePath(conf.replicaHiveSchema, conf.replicaHiveTable)
    val df1 = spark.read.parquet(replicaPath)
    val wrappedColumns = getHiveWrappedColumns(columns, "hive")
    printReplicaWrappedColumn(wrappedColumns)
    df1.select(wrappedColumns: _*)
  }

  def printReplicaWrappedColumn(wrappedColumns: Array[Column]) = {
    logger.info("Replica wrapped columns :")
    wrappedColumns.foreach(logger.info)
  }

}