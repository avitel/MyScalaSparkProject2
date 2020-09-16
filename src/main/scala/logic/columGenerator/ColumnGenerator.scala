package logic.columGenerator

import entity.ColumnDescr
import scala.collection.mutable.ListBuffer

trait ColumnGenerator {
  def getColumns: ListBuffer[ColumnDescr]
}
