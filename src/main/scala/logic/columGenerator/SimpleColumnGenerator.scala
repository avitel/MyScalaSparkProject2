package logic.columGenerator

import dao.ColumnDAO
import entity.ColumnDescr
import logic.Configuration
import logic.columnWrapper.ColumnWrapper
import org.apache.log4j.Logger

import scala.collection.mutable.ListBuffer

class SimpleColumnGenerator(val conf: Configuration, val wrapper: ColumnWrapper, val columnDAO: ColumnDAO) extends ColumnGenerator {
  val logger = Logger.getLogger("sparkETL.columnGenerator")

  def filterColumns(columns: ListBuffer[ColumnDescr]): ListBuffer[ColumnDescr] = {
    columns.filter(col => {
      val supported = wrapper.isTypeSupported("sql", col.data_type)
      if (!supported) {
        logger.warn("Column " + col.column_name + " consists not supported type " + col.data_type + " and was removed")
      }
      supported
    }).
      filter(col => wrapper.isColumnNotExcluded(col.column_name))
  }

  def getColumns: ListBuffer[ColumnDescr] = {
    val columns = filterColumns(columnDAO.getColumns)
    val result = columns.map(row => ColumnDescr(row.column_name, wrapper.toInternalType("sql", row.data_type)))
    logger.info("columns :")
    result.foreach(b => logger.info("  " + b.column_name + " = " + b.data_type))
    result
  }
}
