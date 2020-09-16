package logic.columnWrapper

trait ColumnWrapper {
  def isTypeSupported(source_type: String, data_type: String): Boolean
  def toInternalType(source_type: String, data_type: String): String
  def wrapWithPattern(column_name: String, data_type: String, wrapperType: String): String
  def isColumnNotExcluded(col: String):Boolean
}
