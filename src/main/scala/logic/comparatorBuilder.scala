package logic

import dao.{BucketDAO, ColumnDAO}
import logic.bucketGenerator.{BucketGenerator, NodeBucketGenerator}
import logic.columGenerator.SimpleColumnGenerator
import logic.columnWrapper.ColumnWrapper
import logic.replicaReader.{PartitionedReplicaReader1, PartitionedReplicaReader2, ReplicaReader, FileReplicaReader}
import logic.sourceReader.{SourceReader, UtrnnoSourceReader}
import logic.summaryGenerator.{SimpleSummaryGenerator, StepSummaryGenerator, SummaryGenerator}
import org.apache.spark.sql.SparkSession
import utils.HiveUtils

object comparatorBuilder {

  def build(conf: Configuration, spark: SparkSession, hiveUtils: HiveUtils, wrapper: ColumnWrapper, columnDAO: ColumnDAO, bucketDAO: BucketDAO): Comparator = {

    val bucketGenerator: BucketGenerator =  new NodeBucketGenerator(conf, bucketDAO)
    val columnGenerator =  new SimpleColumnGenerator(conf, wrapper, columnDAO)
    val sourceReader: SourceReader = new UtrnnoSourceReader(conf, spark, wrapper)
    val replicaReader: ReplicaReader = conf.replicaReader match {
      case "PartitionedReplicaReader1" => new PartitionedReplicaReader1(conf, spark, hiveUtils, wrapper)
      case "PartitionedReplicaReader2" => new PartitionedReplicaReader2(conf, spark, hiveUtils, wrapper)
      case _ => new FileReplicaReader(conf, spark, hiveUtils, wrapper)
    }
    val summaryGenerator: SummaryGenerator = conf.summaryGenerator match {
      case "StepSummaryGenerator" => new StepSummaryGenerator(conf, spark)
      case _ => new SimpleSummaryGenerator(conf, spark)
    }

    new Comparator(bucketGenerator, columnGenerator, sourceReader, replicaReader, summaryGenerator)
  }
}
