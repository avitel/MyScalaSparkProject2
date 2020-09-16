package logic.pkdStat

import java.io.IOException

import logic.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import utils.{HdfsUtils, HiveUtils}

object pkdPartitionMaker {
  val logger = Logger.getLogger("sparkETL.partitionMaker")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sparkETL").getOrCreate()
    val sc = spark.sparkContext
    val hiveUtils = new HiveUtils(spark.sharedState.externalCatalog, new HiveMetaStoreClient(new HiveConf))
    val conf = new Configuration(spark.conf, hiveUtils)
    val hdfsUtils = new HdfsUtils(FileSystem.get(sc.hadoopConfiguration))
    val tablePath: String = hiveUtils.getTablePath(conf.pkdStatHiveSchema, conf.pkdStatHiveTable) + "/" + conf.pkdStatPartitionName + "=" + conf.pkdStatPartitionValue
    try{
      if (!hdfsUtils.exists(tablePath)) {
        hdfsUtils.mkdir(tablePath)
        logger.info("catalog for sparkETL statistics has been created (" + tablePath + ")")
      }else {
        logger.info("catalog for sparkETL statistics already exists (" + tablePath + ")")
      }
    }catch{
      case e: IOException =>
        logger.error("failed to create catalog for partition : " + tablePath, e)
    }
    hiveUtils.createHivePartition(conf.pkdStatHiveSchema, conf.pkdStatHiveTable, conf.pkdStatPartitionName, conf.pkdStatPartitionValue, tablePath, true)
    logger.info("hive partition for sparkETL statistics has been created " + conf.pkdStatPartitionName + " : " + conf.pkdStatPartitionValue)
  }
}
