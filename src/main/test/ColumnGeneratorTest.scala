import dao.SourceColumnDao
import entity.ColumnDescr
import logic.columGenerator.SimpleColumnGenerator
import logic.columnWrapper.{ColumnWrapper, SimpleColumnWrapper}
import org.junit.Assert.assertEquals
import org.junit.Test
import org.mockito.Mockito._
import utils.FsUtils

import scala.collection.mutable.ListBuffer

class ColumnGeneratorTest {

  @Test
  def getColumns(): Unit = {
    val conf = testConfiguration.getConf

    val list = ListBuffer.empty[ColumnDescr]
    list += ColumnDescr("col1","12")
    list += ColumnDescr("col2","3")
    list += ColumnDescr("col3","TRASH")
    list += ColumnDescr("col4","12")
    list += ColumnDescr("col5","12")

    val columnDAO = mock(classOf[SourceColumnDao])
    when(columnDAO.getColumns).thenReturn(list)

//    val wrapper = mock(classOf[ColumnWrapper])
//    when(wrapper.isTypeSupported("sql","STRING")).thenReturn(true)
//    when(wrapper.isTypeSupported("sql","INT")).thenReturn(true)
//    when(wrapper.isTypeSupported("sql","TRASH")).thenReturn(false)
//
//    when(wrapper.isColumnNotExcluded("col1")).thenReturn(true)
//    when(wrapper.isColumnNotExcluded("col2")).thenReturn(true)
//    when(wrapper.isColumnNotExcluded("col3")).thenReturn(true)
//    when(wrapper.isColumnNotExcluded("col4")).thenReturn(false)
//
//    when(wrapper.toInternalType("sql","STRING")).thenReturn("string")
//    when(wrapper.toInternalType("sql","INT")).thenReturn("integer")

    val fsUtils = new FsUtils()
    val dataTypes = fsUtils.getPropertyFile("./dataTypesTest.properties")
    val wrappings = fsUtils.getPropertyFile("./wrapping.properties")
    val wrapper = new SimpleColumnWrapper(dataTypes, wrappings, conf.excludedColumns, false)

    val columnGenerator = new SimpleColumnGenerator(conf, wrapper, columnDAO)

    val cols = columnGenerator.getColumns

    assertEquals(3, cols.size)
    cols.foreach(println)
  }
}
