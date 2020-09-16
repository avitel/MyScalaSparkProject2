package logic

import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import javax.sql.DataSource
import org.apache.log4j.Logger
import org.apache.spark.sql.RuntimeConfig
import org.springframework.jdbc.datasource.DriverManagerDataSource
import utils.HiveUtils
import utils.Jceks

class Configuration() {
  var logger = Logger.getLogger("sparkETL.Configuration")

  var srcDataSource : DataSource = _
  var connectionProperties : Properties = new Properties()
  var sourceSchema: String = ""
  var sourceTable : String = ""
  var primaryKeys: String = "" //comma separated
  var replicaHiveSchema : String = ""
  var replicaHiveTable: String = ""
  var loadingId: String = ""
  var samplePercent: Float = 0
  var ctlQaEntityId: String = ""
  var ctlUri: String = "http://myhost:8888/v1/api"  //PROM
  var ctlProfileId: Int = 1
  var deltaMinus: Int = -1
  var pkdStatHiveSchema: String = "pkd_statistics"
  var pkdStatHiveTable: String = "statistics"
  var pkdStatPartitionName: String = "qc_date"
  val dateFormatter = new SimpleDateFormat("yyyyMMdd")
  var pkdStatPartitionValue: String = dateFormatter.format(new Date)
  var bucketSchemaName = "SCHEMA"
  var bucketTableName = "TABLE"
  var replicaReader: String = ""
  var replicaPartitionDivider: String = "1"
  var exportHiveSchema = ""
  var exportHiveTable = ""
  var summaryGenerator = ""
  var summaryHiveSchema = ""
  var summaryHiveTable = ""
  var summaryHivePartitionName = "pkd_loading"
  var summaryHivePartitionValue = ""
  var summaryPath = ""
  var summaryTempPath = ""
  var exportPath = ""
  var excludedColumns = ""
  var flowName = "flowname"
  var verDate: String = ""

  def this(conf: RuntimeConfig, hiveUtils: HiveUtils){
    this()
    val jceksPathQa = conf.get("spark.jceksPathQa", "")
    val passwordAliasQa = conf.get("spark.passwordAliasQa", "")
    var pass = ""
    if (!jceksPathQa.isEmpty && !passwordAliasQa.isEmpty) {
      pass = new Jceks().getValue(jceksPathQa, passwordAliasQa)
    }

    val ds = new DriverManagerDataSource()
    ds.setUrl(conf.get("spark.sourceUrl", "sometext"))
    ds.setDriverClassName(conf.get("spark.jdbcDriver", "oracle.jdbc.OracleDriver"))
    ds.setUsername(conf.get("spark.sourceUser", ""))
    ds.setPassword(pass)
    this.srcDataSource = ds
    this.connectionProperties.put("sourceUrl", conf.get("spark.sourceUrl", ""))
    this.connectionProperties.put("driver", conf.get("spark.jdbcDriver", "oracle.jdbc.OracleDriver"))
    this.connectionProperties.put("user", conf.get("spark.sourceUser", ""))
    this.connectionProperties.put("password", pass)

    this.loadingId = conf.get("spark.loadingId", "0")
    this.replicaReader = conf.get("spark.replicaReader", "")
    this.summaryGenerator = conf.get("spark.summaryGenerator", "")
    this.replicaPartitionDivider = conf.get("spark.replicaPartitionDivider", "")
    this.ctlQaEntityId = conf.get("spark.ctlQaEntityId", "")
    this.sourceSchema = conf.get("spark.sourceSchema", "")
    this.sourceTable = conf.get("spark.sourceTable", "")
    this.replicaHiveSchema = conf.get("spark.replicaSchema", "")
    this.replicaHiveTable = conf.get("spark.replicaTable", "")
    this.primaryKeys = conf.get("spark.primaryKeys", "")
    this.excludedColumns = conf.get("spark.excludedColumns", "")
    this.deltaMinus = conf.get("spark.deltaMinus", "-1").toInt
    this.samplePercent = conf.get("spark.samplePercent", "0.06").toFloat

    if (!replicaHiveSchema.isEmpty && !replicaHiveTable.isEmpty){
      this.summaryHiveSchema = this.replicaHiveSchema + "_qa"
      this.summaryHiveTable = this.replicaHiveTable + "_summary"
      this.exportHiveSchema = this.replicaHiveSchema + "_qa"
      this.exportHiveTable = this.replicaHiveTable + "_export"
      this.summaryHivePartitionValue = this.loadingId
      val summaryCommonPath = hiveUtils.getTablePath(summaryHiveSchema, summaryHiveTable)
      this.summaryTempPath = summaryCommonPath + "/" + summaryHivePartitionName + "=" + summaryHivePartitionValue + "/temp.parquet"
      this.summaryPath 		 = summaryCommonPath + "/" + summaryHivePartitionName + "=" + summaryHivePartitionValue + "/summary.parquet"
      val exportCommonPath = hiveUtils.getTablePath(exportHiveSchema, exportHiveTable)
      this.exportPath 		 = exportCommonPath + "/" + summaryHivePartitionName + "=" + summaryHivePartitionValue + "/export.parquet"
    }
  }

  def printProperties ={
    logger.info("List of properties:")
    val vars = this.getClass.getDeclaredFields
    vars.foreach(v => {
      v.setAccessible(true)
      logger.info("property : " + v.getName + " value : " + v.get(this))
    })
  }
}
