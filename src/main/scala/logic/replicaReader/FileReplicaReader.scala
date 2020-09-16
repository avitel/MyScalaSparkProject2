package logic.replicaReader

import entity.ColumnDescr
import logic.Configuration
import logic.columnWrapper.ColumnWrapper
import org.apache.log4j.Logger
import org.apache.spark.sql._
import utils.HiveUtils
import org.apache.spark.sql.functions.{col, concat_ws, expr, input_file_name}
import org.apache.spark.util.sketch.BloomFilter
import scala.collection.mutable.ListBuffer

class FileReplicaReader(val conf: Configuration, val spark: SparkSession, val hiveUtils: HiveUtils, val wrapper: ColumnWrapper) extends ReplicaReader {
  var logger = Logger.getLogger("sparkETL.FileReplicaReader")

  def getHiveWrappedColumns(columns: ListBuffer[ColumnDescr], wrapperType: String): Array[Column]={
    columns.map(row => wrapper.wrapWithPattern(row.column_name, row.data_type, wrapperType)).map(str => expr(str)).toArray
  }

  def getFilePaths(replicaPath: String, source_df: Dataset[Row], columns: ListBuffer[ColumnDescr]): Array[String] = {
    val pkSource = conf.primaryKeys.split(",").map(colName => expr(colName))
    val columns_pk = columns.filter(column => conf.primaryKeys.contains(column.column_name))
    val wrappedPkReplica = getHiveWrappedColumns(columns_pk, "hive")
    val snpKeys = spark.read.parquet(replicaPath).select(concat_ws("|", wrappedPkReplica:_*).as("snp_keys"))
    val bloomFilter = BloomFilter.create(source_df.count)
    trainBloomFilter(bloomFilter, source_df, pkSource)
    val filteredKeys = snpKeys.filter(row => bloomFilter.mightContainString(row.getAs("snp_keys")))
    val filesDF = filteredKeys.withColumn("file", input_file_name).select(col("file"))

    val files = filesDF.collect.distinct.map(row => row.getString(0))
    logger.info("count of files : " + files.size)
    logger.info("list of files : " )
    files.foreach(logger.info)
    files
  }

  def trainBloomFilter(bloomFilter: BloomFilter,source_df: Dataset[Row], pk: Array[Column]): Unit ={
    source_df.select(concat_ws("|", pk:_*)).as("pk").collect
      .map(row => row.getString(0))
      .foreach(key => bloomFilter.putString(key))
  }

  def getReplicaDF(columns: ListBuffer[ColumnDescr], source_df: Dataset[Row]): Dataset[Row] = {
    val replicaPath  = hiveUtils.getTablePath(conf.replicaHiveSchema, conf.replicaHiveTable)
    val filePaths: Array[String] = getFilePaths(replicaPath, source_df, columns)
    val df = spark.read.parquet(filePaths: _*)
    val wrappedColumns = getHiveWrappedColumns(columns, "hive")
    printReplicaWrappedColumn(wrappedColumns)
    df.select(wrappedColumns: _*)
  }

  def printReplicaWrappedColumn(wrappedColumns: Array[Column]) = {
    logger.info("Replica wrapped columns :")
    wrappedColumns.foreach(logger.info)
  }
}