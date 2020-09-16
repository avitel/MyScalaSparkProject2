import logic.columnWrapper.{ColumnWrapper, SimpleColumnWrapper}
import org.junit.Test
import utils.FsUtils

class ColumnWrapperTest {

  @Test
  def getColumns(): Unit = {
    val conf = testConfiguration.getConf

    val fsUtils = new FsUtils()

    val dataTypes = fsUtils.getPropertyFile("./dataTypesTest.properties")
    val wrappings = fsUtils.getPropertyFile("./wrapping.properties")

    val wrapper = new SimpleColumnWrapper(dataTypes, wrappings, conf.excludedColumns, false)

    val res = wrapper.wrapWithPattern("col1", "varchars", "oracle")
    println(res)
    val res1 = wrapper.wrapWithPattern("col1", "numerics", "mssql")
    println(res1)

    val res3 = wrapper.toInternalType("sql","12")
    println(res3)
    val res4 = wrapper.toInternalType("sql","3")
    println(res4)

    println(wrapper.isColumnNotExcluded("col1"))
    println(wrapper.isColumnNotExcluded("col3"))

    println(wrapper.isTypeSupported("sql","12"))
    println(wrapper.isTypeSupported("sql","22"))
  }
}
