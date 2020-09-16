package dao

import java.sql.{Connection, SQLException}
import javax.sql.DataSource
import entity.ColumnDescr
import org.apache.log4j.Logger
import scala.collection.mutable.ListBuffer

class SourceColumnDao(val dataSource: DataSource, val schemaName: String, val tableName: String) extends ColumnDAO {
  val logger = Logger.getLogger("sparkETL.sourceColumnDao")

  def getColumns: ListBuffer[ColumnDescr] = {
    var conn: Connection = null
    try{
      conn = dataSource.getConnection()
      val rs = conn.getMetaData.getColumns(null, schemaName.toUpperCase, tableName.toUpperCase, null)
      val list = ListBuffer.empty[ColumnDescr]
      while (rs.next()) {
        list += ColumnDescr(rs.getString("COLUMN_NAME"), rs.getString("DATA_TYPE"))
      }
      if (list.size == 0){
        throw new SQLException("got no columns from the source")
      }else{
        logger.info("source columns : ")
        list.foreach(col  => logger.info("  " + col.column_name + " = " + col.data_type))
      }
      list
    }catch {
      case e: SQLException => {
        logger.error("can`t get columns from source")
        throw e
      }
    }finally {
      if (conn != null) conn.close()
    }
  }
}
