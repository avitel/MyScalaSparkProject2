import logic.summaryGenerator.{SimpleSummaryGenerator, SummaryGenerator}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.Test

class SummaryGeneratorTest {

  @Test
  def getSummary(): Unit = {
    val conf = testConfiguration.getConf
    val summaryGenerator = new SimpleSummaryGenerator(conf)

    val spark = SparkSession.builder().appName("sparkETL_test").master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val schema1 = new StructType(Array(
      StructField("col1", StringType, nullable = false)
      ,StructField("col2", StringType, nullable = false)
      ,StructField("col3", StringType, nullable = false)
      ,StructField("col4", StringType, nullable = false)
      ,StructField("col5", StringType, nullable = false)
    ))

    val rdd1 = sc.parallelize(Seq(Row("1", "2", "3", "4", "5")
      ,Row("10", "20", "30", "40", "50")
    ))

    val rdd2 = sc.parallelize(Seq(Row("1", "2", "3", "44", "55")
      ,Row("10", "20", "30", "40", "505")
    ))

    val df1 = spark.createDataFrame(rdd1, schema1)
    df1.show

    val df2 = spark.createDataFrame(rdd2, schema1)
    df2.show

    val resdf = summaryGenerator.getSummary(df1, df2)
    resdf.show()
  }
}
