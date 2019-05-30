package au.com.agl.arc.transform

import java.lang._
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

object GraphTransform {

  def transform(transform: GraphTransform)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger): Option[DataFrame] = {
    val startTime = System.currentTimeMillis() 
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", transform.getType)
    stageDetail.put("name", transform.name)
    for (description <- transform.description) {
      stageDetail.put("description", description)    
    }    

    val nodeSignature = "GraphTransform requires the nodesView to have a column named 'id'."


    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()    
      
    val transformedDF = try {
      transform.source match {
        case Left((nodeDF, relationshipDF)) => {

          // validate nodeDF
          val id = nodesDF.schema.fields.filter(_.name == "id")
          if (id.length != 1) {
            throw new Exception(s"${nodeSignature} nodesView '${transform.nodesView}' has ${nodeDF.schema.length} columns named ${nodeDF.columns.mkString("[", ", ", "]")}.")
          }



        }
        case Right(_) =>
      }
      } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail          
      }
    }
    

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log()  

    None
  }

}
