package au.com.agl.arc.transform

import java.lang._
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types._
import org.opencypher.morpheus.api.MorpheusSession
import org.opencypher.morpheus.api.io.{MorpheusNodeTable, MorpheusRelationshipTable}

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

    val nodeSignature = "GraphTransform requires nodesView to have a column named 'id'."
    val relsSignature = "GraphTransform requires relationshipsView to have a columns named 'id', 'source' and 'target'."
    val relsTypeSignature = "GraphTransform requires relationshipsView to have 'source' and 'target' the same data type as nodesView 'id'."

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()    

      
    implicit val morpheus: MorpheusSession = MorpheusSession.create(spark)
    
    val graph = try {
      transform.source match {
        case Left((nodesView, relationshipsView, nodesType, relationshipsType)) => {
          val nodesDF = spark.table(nodesView)
          val relsDF = spark.table(relationshipsView)

          // validation is basically to override error messages for clarity
          // validate nodesDF
          val nodesIds = nodesDF.schema.fields.filter(_.name == "id")
          if (nodesIds.length != 1) {
            throw new Exception(s"${nodeSignature} nodesView '${nodesView}' has ${nodesDF.schema.length} columns named ${nodesDF.columns.mkString("[", ", ", "]")}.")
          }
          val nodesId = nodesIds(0)

          // validate relsDF
          val relsIds = relsDF.schema.fields.filter(_.name == "id")
          val relsSources = relsDF.schema.fields.filter(_.name == "source")
          val relsTargets = relsDF.schema.fields.filter(_.name == "target")

          // ensure required columns exist
          if (relsIds.length == 1 && relsSources.length == 1 && relsTargets.length == 1) {
            throw new Exception(s"${relsSignature} relationshipsView '${relationshipsView}' has ${relsDF.schema.length} columns named ${relsDF.columns.mkString("[", ", ", "]")}.")
          }

          // ensure source and target have same dataType as nodesDF.id
          val relsSource = relsSources(0)
          val relsTarget = relsTargets(0)          
          if (relsSource.dataType != nodesId.dataType || relsTarget.dataType != nodesId.dataType) {
            throw new Exception(s"${relsTypeSignature} nodesView 'id' is of type '${nodesId.dataType.simpleString}' and relationshipsView ['source','target'] are of types ['${relsSource.dataType.simpleString}','${relsTarget.dataType.simpleString}'].")
          }

          // create the tables
          val nodesTable = MorpheusNodeTable(Set(nodesType), nodesDF)
          val relationshipsTable = MorpheusRelationshipTable(relationshipsType, relsDF)

          // create the graph
          morpheus.readFrom(nodesTable, relationshipsTable)
        }
        case Right(cypher) => {
          morpheus.cypher(cypher).graph
        }
      }
      } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail          
      }
    }

    // put the graph into the catalog. will be named 'session.[outputGraph]'
    morpheus.catalog.store(transform.outputGraph, graph)
    
    if (transform.persist) {
      graph.cache
    }      

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log()  

    None
  }

}
