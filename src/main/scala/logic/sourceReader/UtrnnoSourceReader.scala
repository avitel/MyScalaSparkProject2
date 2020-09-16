package logic.sourceReader

import entity.{Bucket, ColumnDescr}
import logic.Configuration
import logic.columnWrapper.ColumnWrapper
import org.apache.commons.lang.text.StrSubstitutor
import org.apache.log4j.Logger

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql._

class UtrnnoSourceReader(val conf: Configuration, val spark: SparkSession, val wrapper: ColumnWrapper) extends SourceReader {
  val logger = Logger.getLogger("sparkETL.utrnnoSourceReader")

  def getSourceQuery(queryTemplate: String, columns: ListBuffer[ColumnDescr], buckets: Array[Bucket], wrapperType: String): String={
    val bucketClause = "(" + buckets.map(bucket => " utrnno between " + bucket.min_value + " and " + bucket.max_value).mkString("\n or ") + ")\n"
    val fields = columns.map(row => wrapper.wrapWithPattern(row.column_name, row.data_type, wrapperType)).mkString(", \n")
    val data = new java.util.HashMap[String,String]()
    data.put("fields", fields)
    data.put("schemaName", conf.sourceSchema)
    data.put("tableName", conf.sourceTable)
    data.put("bucketClause", bucketClause)
    data.put("verDate", conf.verDate)
    StrSubstitutor.replace(queryTemplate, data)
  }

  def  getSourceDF(columns: ListBuffer[ColumnDescr], buckets: Array[Bucket]): Dataset[Row] = {
    val queryTemplate = "select ${fields} from ${schemaName}.${tableName} where \n ${bucketClause} and ora_rowscn < timestamp_to_scn(to_timestamp('${verDate} 23:59:59','YYYYMMDD HH24:MI:SS'))"
    val query =  "(" + getSourceQuery(queryTemplate, columns, buckets, "oracle") + ")"
    logger.info("source query = " + query)
    println("source query = " + query)
    spark.read.jdbc(conf.connectionProperties.getProperty("sourceUrl"), query, conf.connectionProperties)
  }
}