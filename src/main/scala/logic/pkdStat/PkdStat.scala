package logic.pkdStat

import java.sql.Timestamp

import logic.Configuration
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, SparkSession}
import utils.{HdfsUtils, HiveUtils}

class PkdStat(sc: SparkContext, spark: SparkSession, val hiveUtils: HiveUtils, val hdfsUtils: HdfsUtils) {
  val logger = Logger.getLogger("sparkETL.pkdStat")

  def createPkdStatictics(
                           conf: Configuration,
                           diffQty: Long,
                           checkedQty: Long,
                           startTime: Timestamp,
                           endTime: Timestamp,
                           srcTime: Timestamp,
                           finishStatus: Int
                         ): Unit = {

    val rdd = sc.parallelize(Seq(Row(
      "bigtables" //0 ReviseType
      , conf.replicaHiveSchema //1 SchemeName
      , conf.replicaHiveTable //2 TableName
      , diffQty //3 DiffQty
      , checkedQty //4 CheckedQty
      , conf.loadingId //5 first_pkd_loading
      , conf.loadingId //6 second_pkd_loading
      , startTime //7 ReviseStart
      , endTime //8 ReviseEnd
      , srcTime //9 SrcTime
      , null //10 ReplicaTime//"12" статистика
      , null //11 scn
      , finishStatus //12 FinishStatus
      , conf.flowName
    )))

    val schema = new StructType(Array(
      StructField("ReviseType", StringType, nullable = false)
      , StructField("SchemeName", StringType, nullable = false)
      , StructField("TableName", StringType, nullable = false)
      , StructField("DiffQty", LongType, nullable = false)
      , StructField("CheckedQty", LongType, nullable = false)
      , StructField("first_pkd_loading", StringType, nullable = false)
      , StructField("second_pkd_loading", StringType, nullable = false)
      , StructField("ReviseStart", TimestampType, nullable = false)
      , StructField("ReviseEnd", TimestampType, nullable = false)
      , StructField("SrcTime", TimestampType, nullable = true)
      , StructField("ReplicaTime", TimestampType, nullable = true)
      , StructField("csn", StringType, nullable = true)
      , StructField("FinishStatus", IntegerType, nullable = false)
      , StructField("FlowName", StringType, nullable = false)
    ))
    val res = spark.createDataFrame(rdd, schema)
    res.show
    logger.info("pkd stat dataset is generated successfully")
    val tablePath = hiveUtils.getTablePath(conf.pkdStatHiveSchema, conf.pkdStatHiveTable) + "/" + conf.pkdStatPartitionName + "=" + conf.pkdStatPartitionValue
    logger.info("pkd stat table path : " + tablePath)
    res.coalesce(1).write.mode("append").parquet(tablePath)
    logger.info("PKD statistics has been written")
  }
}
