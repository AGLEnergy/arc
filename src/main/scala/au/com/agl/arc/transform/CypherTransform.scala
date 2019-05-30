package au.com.agl.arc.transform

import java.lang._
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.opencypher.morpheus.api.MorpheusSession

import au.com.agl.arc.api.API._
import au.com.agl.arc.util._

object CypherTransform {

  def transform(transform: CypherTransform)(implicit spark: SparkSession, logger: au.com.agl.arc.util.log.logger.Logger, morpheus: MorpheusSession): Option[DataFrame] = {
    val startTime = System.currentTimeMillis() 
    val stageDetail = new java.util.HashMap[String, Object]()
    stageDetail.put("type", transform.getType)
    stageDetail.put("name", transform.name)
    for (description <- transform.description) {
      stageDetail.put("description", description)    
    }    
    stageDetail.put("inputURI", transform.inputURI.toString)  
    stageDetail.put("outputView", transform.outputView)   
    stageDetail.put("cypherParams", transform.cypherParams.asJava)   
    stageDetail.put("persist", Boolean.valueOf(transform.persist))

    // inject sql parameters
    val stmt = SQLUtils.injectParameters(transform.cypher, transform.cypherParams, false)
    stageDetail.put("cypher", stmt)

    logger.info()
      .field("event", "enter")
      .map("stage", stageDetail)      
      .log()      
    
    val transformedDF = try {
      val result = morpheus.cypher(stmt)
      result.records.table.df
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail          
      }
    }

    // repartition to distribute rows evenly
    val repartitionedDF = transform.partitionBy match {
      case Nil => { 
        transform.numPartitions match {
          case Some(numPartitions) => transformedDF.repartition(numPartitions)
          case None => transformedDF
        }   
      }
      case partitionBy => {
        // create a column array for repartitioning
        val partitionCols = partitionBy.map(col => transformedDF(col))
        transform.numPartitions match {
          case Some(numPartitions) => transformedDF.repartition(numPartitions, partitionCols:_*)
          case None => transformedDF.repartition(partitionCols:_*)
        }
      }
    }

    repartitionedDF.createOrReplaceTempView(transform.outputView)    

    if (!repartitionedDF.isStreaming) {
      // add partition and predicate pushdown detail to logs
      stageDetail.put("partitionFilters", QueryExecutionUtils.getPartitionFilters(repartitionedDF.queryExecution.executedPlan).toArray)
      stageDetail.put("dataFilters", QueryExecutionUtils.getDataFilters(repartitionedDF.queryExecution.executedPlan).toArray)
      stageDetail.put("outputColumns", Integer.valueOf(repartitionedDF.schema.length))
      stageDetail.put("numPartitions", Integer.valueOf(repartitionedDF.rdd.partitions.length))

      if (transform.persist) {
        repartitionedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
        stageDetail.put("records", Long.valueOf(repartitionedDF.count)) 
      }      
    }

    logger.info()
      .field("event", "exit")
      .field("duration", System.currentTimeMillis() - startTime)
      .map("stage", stageDetail)      
      .log()  

    Option(repartitionedDF)
  }

}
