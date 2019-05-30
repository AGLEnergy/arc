package au.com.agl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
// import org.opencypher.morpheus.api.MorpheusSession

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util.log.LoggerFactory 

import au.com.agl.arc.util._

class GraphTransformSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val inputNodes0 = "inputNodes"
  val inputRelationships0 = "inputRelationships"
  val nodesType0 = "Person"
  val relationshipsType0 = "KNOWS"
  val outputGraph = "outputGraph"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")   

    session = spark
    import spark.implicits._
  }

  after {
    session.stop()
  }

  test("GraphTransform") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    // implicit val morpheus: MorpheusSession = MorpheusSession.create(spark)

    // val transformed = transform.GraphTransform.transform(
    //   GraphTransform(
    //     name="GraphTransform", 
    //     description=None,
    //     source=Left(inputNodes0, inputRelationships0, nodesType0, relationshipsType0),
    //     outputGraph=outputGraph,
    //     persist=false,
    //     params=Map.empty
    //   )
    // )

    // val catalogGraphs = morpheus.catalog.graphNames.filter(graph => graph.graphName == outputGraph)
    // assert(catalogGraphs.nonEmpty)
  }  
}
