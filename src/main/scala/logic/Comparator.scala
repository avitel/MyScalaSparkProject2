package logic

import logic.bucketGenerator.BucketGenerator
import entity.{Bucket, ColumnDescr}
import logic.columGenerator.ColumnGenerator
import logic.replicaReader.ReplicaReader
import logic.sourceReader.SourceReader
import logic.summaryGenerator.SummaryGenerator
import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, Row}
import scala.collection.mutable.ListBuffer

class Comparator(val bucketGenerator: BucketGenerator, val columnGenerator: ColumnGenerator, val sourceReader: SourceReader, val replicaReader: ReplicaReader, val summaryGenerator: SummaryGenerator) {
  var logger = Logger.getLogger("sparkETL.Comparator")
  def getBuckets( verDate: String, samplePercent: Float): Array[Bucket] = {
    bucketGenerator.getBuckets(verDate, samplePercent)
  }
  def getColumns: ListBuffer[ColumnDescr] = {
    columnGenerator.getColumns
  }
  def getSourceDF(columns: ListBuffer[ColumnDescr], buckets: Array[Bucket]): Dataset[Row] = {
    sourceReader.getSourceDF(columns, buckets)
  }
  def getReplicaDF(columns: ListBuffer[ColumnDescr], source_df: Dataset[Row]): Dataset[Row] = {
    replicaReader.getReplicaDF(columns, source_df)
  }
  def getSummary(sourceDF: Dataset[Row], replicaDF: Dataset[Row]): Dataset[Row] = {
    summaryGenerator.getSummary(sourceDF, replicaDF)
  }
  def printProperties ={
    logger.info("List of properties : ")
    val vars = this.getClass.getDeclaredFields
    vars.foreach(v => {
      v.setAccessible(true)
      logger.info("property : " + v.getName + " value : " + v.get(this))
    })
  }

}