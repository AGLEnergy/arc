package au.com.agl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util.log.LoggerFactory 

import au.com.agl.arc.util._
import au.com.agl.arc.util.ControlUtils._

class TextExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  

  val outputView = "outputView"
  val targetFile = getClass.getResource("/conf/simple.conf").toString
  val targetDirectory = s"""${getClass.getResource("/conf").toString}/*.conf"""

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("FATAL")

    session = spark
  }


  after {
    session.stop
  }

  test("TextExtract: multiLine false") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false)

    val extractDataset = extract.TextExtract.extract(
      TextExtract(
        name="dataset",
        cols=Right(List.empty),
        outputView=outputView, 
        input=targetFile,
        authentication=None,
        persist=false,
        numPartitions=None,
        contiguousIndex=None,
        multiLine=Option(false),
        params=Map.empty
      )
    ).get

    assert(extractDataset.filter($"_filename".contains(targetFile.replace("file:", "file://"))).count != 0)
    assert(extractDataset.count == 29)
  }    

  test("TextExtract: multiLine true") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false)

    val extractDataset = extract.TextExtract.extract(
      TextExtract(
        name="dataset",
        cols=Right(List.empty),
        outputView=outputView, 
        input=targetFile,
        authentication=None,
        persist=false,
        numPartitions=None,
        contiguousIndex=None,
        multiLine=Option(true),
        params=Map.empty
      )
    ).get

    assert(extractDataset.filter($"_filename".contains(targetFile.replace("file:", "file://"))).count != 0)
    assert(extractDataset.count == 1)
  }    

  test("TextExtract: Structured Streaming") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=true)

    val jsonSchema = """
    [
      {
        "id": "982cbf60-7ba7-4e50-a09b-d8624a5c49e6",
        "name": "value",
        "description": "value",
        "type": "string",
        "trim": false,
        "nullable": false,
        "nullableValues": [
          "",
          "null"
        ],
        "metadata": {
          "booleanMeta": true,
          "booleanArrayMeta": [true, false],
          "stringMeta": "string",
          "stringArrayMeta": ["string0", "string1"],
          "longMeta": 10,
          "longArrayMeta": [10,20],
          "doubleMeta": 0.141,
          "doubleArrayMeta": [0.141, 0.52],
          "private": false,
          "securityLevel": 0
        }
      }
    ]
    """
    val cols = au.com.agl.arc.util.MetadataSchema.parseJsonMetadata(jsonSchema)

    val extractDataset = extract.TextExtract.extract(
      TextExtract(
        name="dataset",
        cols=Right(cols.right.getOrElse(Nil)),
        outputView=outputView, 
        input=targetDirectory,
        authentication=None,
        persist=false,
        numPartitions=None,
        contiguousIndex=None,
        multiLine=Option(true),
        params=Map.empty
      )
    ).get

    val writeStream = extractDataset
      .writeStream
      .queryName("extract") 
      .format("memory")
      .start

    val df = spark.table("extract")

    try {
      Thread.sleep(2000)
      assert(df.count != 0)
    } finally {
      writeStream.stop
    }  
  }    
}