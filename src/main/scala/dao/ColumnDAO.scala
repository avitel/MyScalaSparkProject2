package dao

import entity.ColumnDescr

import scala.collection.mutable.ListBuffer

trait ColumnDAO {
  def getColumns: ListBuffer[ColumnDescr]
}
