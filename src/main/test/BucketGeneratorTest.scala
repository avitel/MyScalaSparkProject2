import dao.BucketDaoNode
import logic.bucketGenerator.{BucketGenerator, NodeBucketGenerator}
import org.mockito.Mockito._
import org.junit.Test

class BucketGeneratorTest {

  @Test
  def getBuckets(): Unit = {
    val conf = testConfiguration.getConf
    val fakeBucketDAO = mock(classOf[BucketDaoNode])
    when(fakeBucketDAO.getNodes("")).thenReturn(List("21","31"))
    when(fakeBucketDAO.getMinMaxUtrnno("","21")).thenReturn((21000L,21100L))
    when(fakeBucketDAO.getMinMaxUtrnno("","31")).thenReturn((31000L,31100L))

    val bucketGenerator: BucketGenerator = new NodeBucketGenerator(conf, fakeBucketDAO)
    val buckets = bucketGenerator.getBuckets("",0.1F)
    buckets.foreach(println)
  }
}
