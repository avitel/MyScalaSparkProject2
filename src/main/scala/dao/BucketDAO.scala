package dao

trait BucketDAO {
  def getNodes(verDate: String): List[String]
  def getMinMaxUtrnno(verDate: String, node: String):(Long, Long)
}
