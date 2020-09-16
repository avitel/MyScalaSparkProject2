package dao

import javax.sql.DataSource
import org.apache.log4j.Logger
import org.springframework.jdbc.core.{JdbcTemplate, RowMapper}
import java.sql.ResultSet
import collection.JavaConverters._

class BucketDaoNode(val dataSource: DataSource, val schemaName: String, val tableName: String) extends BucketDAO {
  val logger = Logger.getLogger("sparkETL.BucketDaoNode")


  def getNodes(verDate: String): List[String] = {
    val query =
      s"""select distinct node_id
         |from ${schemaName}.${tableName}
         |where udate=${verDate}
         |and (time = 0
         |or time = 90000
         |or time = 180000
         |or time = 233000)
         |order by node_id
         |""".stripMargin
    logger.info("node query : " + query)

    val jdbcTemplate = new JdbcTemplate(dataSource)
    val result = jdbcTemplate.query(query, new RowMapper[String] {
      def mapRow(rs: ResultSet, rowNum: Int): String = {
        rs.getString(1)
      }
    })
    result.asScala.toList
  }

  def getMinMaxUtrnno(verDate: String, node: String): (Long, Long) = {
    val queryTemplate =
      s"""select {minMax}(utrnno)
         |from ${schemaName}.${tableName}
         |where udate=${verDate}
         |and node_id = ${node}
         |and time between {lowerLimit} and {upperLimit}
         |""".stripMargin
    var min = executeScalarQuery(queryTemplate.
      replace("{minMax}", "min").
      replace("{lowerLimit}", "0").
      replace("{upperLimit}", "59"))
    if (min == 0) {
      logger.warn("got empty bucket min limit query for node " + node + ". Trying extend query... ")
      min = executeScalarQuery(queryTemplate.
        replace("{minMax}", "min").
        replace("{lowerLimit}", "0").
        replace("{upperLimit}", "30000"))
      if (min == 0) {
        logger.warn("bucket min limit query is still empty. Node " + node + " is excluding from revising")
      }
    }
    var max = executeScalarQuery(queryTemplate.
      replace("{minMax}", "max").
      replace("{lowerLimit}", "235900").
      replace("{upperLimit}", "235959"))
    if (max == 0) {
      logger.warn("got empty bucket max limit query for node " + node + ". Trying extend query... ")
      max = executeScalarQuery(queryTemplate.
        replace("{minMax}", "max").
        replace("{lowerLimit}", "210000").
        replace("{upperLimit}", "235959"))
      if (min == 0) {
        logger.warn("bucket max limit query is still empty. Node " + node + " is excluding from revising")
      }
    }
    (min, max)
  }

  def executeScalarQuery(query: String): Long = {
    logger.info("executing bucket mimax query : " + query)

    val jdbcTemplate = new JdbcTemplate(dataSource)
    val result: Long = Option(jdbcTemplate.queryForObject(query, classOf[Long])).getOrElse(0)
    result
  }
}
