package logic.summaryGenerator

trait SummaryGenerator {
  import org.apache.spark.sql._
  def getSummary(source_df: Dataset[Row], repl_df2: Dataset[Row]): Dataset[Row]
}

