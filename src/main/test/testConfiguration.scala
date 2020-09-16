import java.util.Properties
import logic.Configuration
import oracle.jdbc.pool.OracleDataSource

object testConfiguration {

  def getConf: Configuration = {
    val conf = new Configuration()
    conf.srcDataSource  = new OracleDataSource
    conf.connectionProperties  = new Properties()
    conf.sourceSchema = ""
    conf.sourceTable  = ""
    conf.primaryKeys = "col1,col2" //comma separated
    conf.replicaHiveSchema  = ""
    conf.replicaHiveTable  = ""
    conf.loadingId = ""
    conf.samplePercent = 0.06F
    conf.ctlQaEntityId  = ""
    conf.ctlUri  = "http://myhost:8888/v1/api"
    conf.ctlProfileId = 1
    conf.deltaMinus  = -1
    conf.pkdStatHiveSchema  = "sparkETL_statistics"
    conf.pkdStatHiveTable  = "statistics"
    conf.pkdStatPartitionName  = "qc_date"
    conf.pkdStatPartitionValue  = ""
    conf.bucketSchemaName = "SCHEMA"
    conf.bucketTableName = "CURR_TRANS"
    conf.replicaReader = ""
    conf.summaryHiveSchema = ""
    conf.summaryHiveTable = ""
    conf.summaryHivePartitionName = "sparkETL_loading"
    conf.summaryHivePartitionValue = ""
    conf.summaryPath = ""
    conf.summaryTempPath = ""
    conf.excludedColumns = "col5"
    conf.flowName = "sparkETL"
    conf
  }
}
