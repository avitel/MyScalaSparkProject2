package logic.summaryGenerator

import logic.Configuration
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

class StepSummaryGenerator(val conf: Configuration, val spark: SparkSession) extends SummaryGenerator {
  var logger = Logger.getLogger("sparkETL.SimpleSummaryGenerator")

  import org.apache.spark.sql._

  def getSummary(source_df: Dataset[Row], repl_df: Dataset[Row]): Dataset[Row] = {
    val joinedDistributed = executeJoin(source_df, repl_df)
    val joined = collectToDriver(joinedDistributed)
    val  missing = getMissing(source_df, joined)
    val summary = joined.union(missing)
    val cmpField = getCmpField(source_df.schema.fieldNames)
    logger.info("cmpField : " + cmpField)
    val res = summary.withColumn("CMP_FIELD", cmpField)
      .filter(not(col("CMP_FIELD").equalTo(lit(""))))
    orderColumns(res, source_df.schema)
  }

  def executeJoin(source_df: Dataset[Row], repl_df: Dataset[Row]): Dataset[Row] = {
    val source_df_suf = addSuffixToColumns(source_df, "source")
    val repl_df_suf = addSuffixToColumns(repl_df, "replicat")
    val joinCondition = getJoinCondition
    logger.info("join condition : " + joinCondition)
    val joined = repl_df_suf.join(broadcast(source_df_suf), getJoinCondition)//broadcast works only with inner or right join
    joined.explain
    joined
  }

  def addSuffixToColumns(df: Dataset[Row], suffix: String): Dataset[Row] = {
    df.select(df.columns.map(c => col(c).alias(c + "_" + suffix)):_*)
  }

  def getJoinCondition: Column = {
    conf.primaryKeys.split(",")
      .map(colName => expr(colName + "_source = " + colName + "_replicat" ))
      .reduce((a, b) => a.and(b))
  }

  def collectToDriver(df: Dataset[Row]): Dataset[Row] = {
    val arr = df.collect()
    val schema = df.schema
    spark.createDataFrame(spark.sparkContext.parallelize(arr), schema).coalesce(1)
  }

  def getMissing(source_df: Dataset[Row], joined: Dataset[Row]) = {
    val joinCondition = getAntiJoinCondition
    logger.info("anti join condition : " + joinCondition)
    val res = source_df.join(joined, joinCondition, "leftanti")
    val columnList = source_df.schema.flatMap(stField => Array(col(stField.name).alias(stField.name + "_source"), lit(null).alias(stField.name + "_replicat")))
    res.select(columnList: _*)
  }

  def getAntiJoinCondition: Column = {
    conf.primaryKeys.split(",")
      .map(colName => expr(colName + " = " + colName + "_replicat" ))
      .reduce((a, b) => a.and(b))
  }

  def getCmpField(columns: Array[String]): Column  = {
    concat_ws(",", columns.
      map(columnName => {
        when(col(columnName + "_source").equalTo(col(columnName + "_replicat")), lit(null)).
          when(col(columnName + "_source").isNull.and(col(columnName + "_replicat").isNull), lit(null)).
          otherwise(lit(columnName))
      }): _*)
  }

  def orderColumns(input: Dataset[Row], schema: StructType): Dataset[Row] ={
    val columnList = schema.flatMap(stField => Array(col(stField.name + "_source"), col(stField.name + "_replicat")))
    val columnList2 = columnList:+col("CMP_FIELD")
    input.select(columnList2: _*)
  }
}