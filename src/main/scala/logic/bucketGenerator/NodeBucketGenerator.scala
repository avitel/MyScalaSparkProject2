package logic.bucketGenerator

import dao.BucketDAO
import entity.Bucket
import logic.Configuration
import org.apache.log4j.Logger

import scala.collection.mutable.ListBuffer

class NodeBucketGenerator(val conf: Configuration, val bucketDAO: BucketDAO) extends BucketGenerator {
  val logger = Logger.getLogger("sparkETL.NodeBucketGenerator")

  def getBuckets(verDate: String, samplePercent: Float): Array[Bucket] = {
    val nodes: List[String] = bucketDAO.getNodes(verDate)
    logger.info("node list :")
    nodes.foreach(logger.info)
    nodes
      .map(node => bucketDAO.getMinMaxUtrnno(verDate, node))
      .filter(limits => limits._1 != 0 && limits._2 != 0)
      .map(minmax => getBucket(minmax, samplePercent))
      .toArray
  }

  def getBucket(limits: (Long, Long), samplePercent: Float): Bucket = {
    logger.info("min max utrnno " + limits._1 + " : " + limits._2)
    val commonSize: Long = limits._2 - limits._1
    val bucketSize: Long = (commonSize * samplePercent/100).round
    var maxOffset: Int = (commonSize - bucketSize).toInt
    if (maxOffset <= 0) {
      maxOffset = 1
    }
    val random = scala.util.Random
    val offset = random.nextInt(maxOffset)
    val bucket = Bucket(limits._1 + offset, limits._1 + offset + bucketSize)
    logger.info("bucket : " + bucket.min_value + " : " + bucket.max_value)
    bucket
  }
}