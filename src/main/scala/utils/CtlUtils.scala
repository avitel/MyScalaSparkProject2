package utils

import javax.json.Json
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.log4j.Logger

class CtlUtils(val ctlUri: String, val profile_id: Int) {

    def createStatistic(loading_id: Int, entity_id: String, stat_id: Int, avalue: String): Int = {
        val logger = Logger.getLogger("sparkETL.ctlUtils")

        if(loading_id == 0) {
            logger.error("Invalid 'loading_id' : " + loading_id)
            return 400
        }
        if(entity_id == "" || entity_id == "0"  || entity_id == " "){
            logger.error("Invalid 'entity_id' : " + entity_id)
            return 400
        }

        val param = Json.createObjectBuilder()
          .add("profile_id", profile_id)
          .add("loading_id", loading_id)
          .add("entity_id", entity_id.toInt)
          .add("stat_id", stat_id)
          .add("avalue", Json.createArrayBuilder().add(avalue))
          .build().toString
        val CONTENT_TYPE = "Content-type"
        val APPLICATION_JSON = "application/json"

        val entity = new StringEntity(param, ContentType.APPLICATION_FORM_URLENCODED)
        logger.info("CTL param : " + param)
        val post = new HttpPost(ctlUri + "/statval/m")
        post.addHeader(CONTENT_TYPE, APPLICATION_JSON)
        post.setEntity(entity)
        val client = new DefaultHttpClient()

        logger.info("CTL URI : " + post.getURI)
        logger.info("CTL request line : " + post.getRequestLine)
        logger.info("CTL method : " + post.getMethod)
        logger.info("CTL entity : " + post.getEntity)

        val response = client.execute(post)
        val statusCode = response.getStatusLine.getStatusCode
        val message = IOUtils.toString(response.getEntity.getContent, "utf-8")
        logger.info("CTL status code : " + statusCode)
        logger.info("CTL answer : " + message)
        logger.info("CTL statistic has been created")
        statusCode
    }
}




