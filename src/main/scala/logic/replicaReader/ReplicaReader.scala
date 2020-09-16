package logic.replicaReader

import entity.ColumnDescr
import scala.collection.mutable.ListBuffer

trait ReplicaReader {
  import org.apache.spark.sql._
  def getReplicaDF(columns: ListBuffer[ColumnDescr], source_df: Dataset[Row]): Dataset[Row]
  def printReplicaWrappedColumn(wrappedColumns: Array[Column]): Unit
}

