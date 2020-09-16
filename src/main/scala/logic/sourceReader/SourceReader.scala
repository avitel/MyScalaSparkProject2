package logic.sourceReader

import entity.{Bucket, ColumnDescr}

import scala.collection.mutable.ListBuffer

trait SourceReader {
  import org.apache.spark.sql._
  def getSourceDF(columns: ListBuffer[ColumnDescr], buckets: Array[Bucket]): Dataset[Row]
}

