package logic.bucketGenerator

import entity.Bucket

trait BucketGenerator {
  def getBuckets(verDate: String, samplePercent: Float): Array[Bucket]
}


