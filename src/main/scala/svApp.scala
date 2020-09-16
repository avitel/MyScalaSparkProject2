import java.sql.Timestamp
import dao.{BucketDaoNode, SourceColumnDao}
import logic.columnWrapper.SimpleColumnWrapper
import logic.pkdStat.PkdStat
import logic.{Configuration, CtlStatistics, comparatorBuilder}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession, Row}
import org.apache.spark.storage.StorageLevel
import utils.{CtlUtils, FsUtils, HdfsUtils, HiveUtils}

object svApp {
	val logger = Logger.getLogger("sparkETL")
	Logger.getLogger("org").setLevel(Level.OFF)
	Logger.getLogger("akka").setLevel(Level.OFF)
	Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
	Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

	def main(args: Array[String]): Unit = {
		val startTime = new Timestamp(System.currentTimeMillis())
		logger.info(new java.util.Date() + " >>> SCRIPT STARTED")

		val spark = SparkSession.builder().appName("pkd_sparkETL").getOrCreate()
		val sc = spark.sparkContext
		val hiveUtils = new HiveUtils(spark.sharedState.externalCatalog, new HiveMetaStoreClient(new HiveConf))
		val conf = new Configuration(spark.conf, hiveUtils)
		val hdfsUtils = new HdfsUtils(FileSystem.get(sc.hadoopConfiguration))
		val fsUtils = new FsUtils()
		val ctlUtils = new CtlUtils(conf.ctlUri, conf.ctlProfileId)
		val pkdStat = new PkdStat(sc, spark, hiveUtils, hdfsUtils)
		val ctlStatistics = new CtlStatistics(ctlUtils)
		val wrapper = new SimpleColumnWrapper(fsUtils.getPropertyFile("./dataTypes.properties"),
						fsUtils.getPropertyFile("./wrapping.properties"),
						conf.excludedColumns,
						true)
		val columnDAO = new SourceColumnDao(conf.srcDataSource, conf.sourceSchema, conf.sourceTable)
		val bucketDAO = new BucketDaoNode(conf.srcDataSource, conf.bucketSchemaName, conf.bucketTableName)

		val now = new java.util.Date
		val yesterday = new java.util.Date(now.getTime + 86400000 * conf.deltaMinus) //millis in day
		val df = new java.text.SimpleDateFormat("yyyyMMdd")
		val verDate = df.format(yesterday)
		conf.verDate = verDate
		conf.printProperties

		var checkedQty: Long = 0
		var diffQty: Long = 0
		var finishStatus = 1

		val comparator = comparatorBuilder.build(conf, spark, hiveUtils, wrapper, columnDAO, bucketDAO)
		comparator.printProperties

		try {
			val buckets = comparator.getBuckets(verDate, conf.samplePercent)
			val columns = comparator.getColumns
			val source_df_temp = comparator.getSourceDF(columns, buckets)
			source_df_temp.write.parquet(conf.exportPath)
      logger.info("EXPORT parquet has been written")
			hiveUtils.createHivePartition(conf.exportHiveSchema, conf.exportHiveTable, conf.summaryHivePartitionName, conf.summaryHivePartitionValue, conf.exportPath, true)
			logger.info("EXPORT hive partition has been created")

			val source_df = spark.read.parquet(conf.exportPath)
			checkedQty = source_df.count
			logger.info("SOURCE COUNT : " + checkedQty)

			val repl_df = comparator.getReplicaDF(columns, source_df)
			val summary_df = comparator.getSummary(source_df, repl_df)
			summary_df.write.mode("overwrite").parquet(conf.summaryPath)
			diffQty = summary_df.count
			logger.info("SUMMARY has been written successfully. Size = " + diffQty)
			hiveUtils.createHivePartition(conf.summaryHiveSchema, conf.summaryHiveTable, conf.summaryHivePartitionName, conf.summaryHivePartitionValue, conf.summaryPath, true)
			logger.info("SUMMARY hive partition has been created")

			ctlStatistics.publishCtlStatistics(diffQty, conf.loadingId, conf.ctlQaEntityId)

			if (checkedQty == 0) {
				finishStatus = 0
			}else if(diffQty != 0){
				finishStatus = 4
			}

		} catch {
			case scala.util.control.NonFatal(e) =>
				logger.error("something went wrong", e)
				finishStatus = 3
		}

		val endTime = new Timestamp(System.currentTimeMillis())
		pkdStat.createPkdStatictics(
			conf,
			diffQty,
			checkedQty,
			startTime,
			endTime,
			startTime,
			finishStatus)

		//hdfsUtils.deleteFiles(conf.summaryTempPath)

		logger.info("Finish status : " + finishStatus)
		logger.info(endTime + " >>> SCRIPT FINISHED")
		if(finishStatus == 3){
			System.exit(finishStatus)
		}
	}
}