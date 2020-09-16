package logic.columnWrapper

import java.util.Properties
import org.apache.log4j.Logger
import scala.collection.JavaConverters._
import javax.script._

class SimpleColumnWrapper() extends ColumnWrapper {
  var dataTypes : Properties = new Properties()
  var wrapping : Properties = new Properties()
  var excludedColumns : String = ""

  val logger = Logger.getLogger("sparkETL.ColumnWrapper")

  def this(dataTypes: Properties, wrapping: Properties, excludedColumns: String, transformationNecessity: Boolean){
    this()
    this.wrapping = wrapping
    this.excludedColumns = excludedColumns
    this.dataTypes = if (transformationNecessity) {
      evalJavaSqlTypes(dataTypes, "java.sql.Types", "sql.")
    } else {
      dataTypes
    }
  }

  def evalJavaSqlTypes(dataTypes: Properties, old_prefix: String, new_prefix: String): Properties = {
    val engine = new ScriptEngineManager().getEngineByName("scala")
    //val settings = engine.asInstanceOf[scala.tools.nsc.interpreter.IMain].settings
    //settings.embeddedDefaults[SimpleColumnWrapper]
    val prop = new Properties
    dataTypes.entrySet().asScala.foreach(entry => {
      val key: String = entry.getKey.asInstanceOf[String]
      if (key.startsWith(old_prefix)){
        prop.put(new_prefix + engine.eval(key),entry.getValue)
      }else{
        prop.put(key,entry.getValue)
      }
    })
    prop
  }

  def isTypeSupported(source_type: String, data_type: String):Boolean={
    Option(dataTypes.getProperty(source_type + "." + data_type)).isDefined
  }

  def toInternalType(source_type: String, data_type: String):String={
    Option(dataTypes.getProperty(source_type + "." + data_type)) match {
      case Some(res) => res
      case None => logger.error("Internal error! Type " + source_type + "." + data_type + " is unsupported ")
        "unsupportedType"
    }
  }

  def wrapWithPattern(column_name: String, data_type: String, wrapperType: String): String={
    Option(wrapping.getProperty(wrapperType + "." + data_type)) match {
      case Some(w) => val res = w.replace("${column_name}", column_name)
        logger.info("column " + column_name + " : " + data_type + " was wrapped with type '" + wrapperType + "' to  " + res)
        res
      case None => logger.error("Column " + column_name + " is not wrapped because there is no wrapper for type " + wrapperType + "." + data_type)
        column_name
    }
  }

  def isColumnNotExcluded(col: String): Boolean = {
    !excludedColumns.contains(col)
  }
}
