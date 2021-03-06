package au.com.agl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util.log.LoggerFactory 

import au.com.agl.arc.util.TestDataUtils

class TextLoadSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val targetFile = FileUtils.getTempDirectoryPath() + "extract.txt" 
  val targetSingleFile = FileUtils.getTempDirectoryPath() + "extractsingle.txt" 
  val outputView = "dataset"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")   

    session = spark

    // ensure targets removed
    FileUtils.deleteQuietly(new java.io.File(targetFile)) 
    FileUtils.deleteQuietly(new java.io.File(targetSingleFile)) 
  }

  after {
    session.stop()

    // clean up test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))     
    FileUtils.deleteQuietly(new java.io.File(targetSingleFile)) 
  }

  test("TextLoad") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset.select("stringDatum")
    dataset.createOrReplaceTempView(outputView)

    load.TextLoad.load(
      TextLoad(
        name=outputView, 
        description=None,
        inputView=outputView, 
        outputURI=new URI(targetFile), 
        numPartitions=None, 
        authentication=None, 
        saveMode=SaveMode.Overwrite, 
        params=Map.empty,
        singleFile=false,
        prefix="",
        separator="",
        suffix=""
      )
    )

    val expected = dataset.withColumnRenamed("stringDatum", "value")
    val actual = spark.read.text(targetFile)

    assert(TestDataUtils.datasetEquality(expected, actual))
  }  

  test("TextLoad: singleFile") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset.select("stringDatum")
    dataset.createOrReplaceTempView(outputView)

    load.TextLoad.load(
      TextLoad(
        name=outputView, 
        description=None,
        inputView=outputView, 
        outputURI=new URI(targetFile), 
        numPartitions=None,  
        authentication=None, 
        saveMode=SaveMode.Overwrite, 
        params=Map.empty,
        singleFile=true,
        prefix="",
        separator="",
        suffix=""
      )
    )    

    val actual = spark.read.text(targetFile)
    val expected = Seq("test,breakdelimiterbreakdelimiter,test").toDF

    assert(TestDataUtils.datasetEquality(expected, actual))
  } 

  test("TextLoad: singleFile prefix/separator/suffix") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset.toJSON
    dataset.createOrReplaceTempView(outputView)

    load.TextLoad.load(
      TextLoad(
        name=outputView, 
        description=None,
        inputView=outputView, 
        outputURI=new URI(targetFile), 
        numPartitions=None,  
        authentication=None, 
        saveMode=SaveMode.Overwrite, 
        params=Map.empty,
        singleFile=true,
        prefix="[",
        separator=",",
        suffix="]"
      )
    )    

    val actual = spark.read.text(targetFile)
    val expected = Seq("""[{"booleanDatum":true,"dateDatum":"2016-12-18","decimalDatum":54.321000000000000000,"doubleDatum":42.4242,"integerDatum":17,"longDatum":1520828868,"stringDatum":"test,breakdelimiter","timeDatum":"12:34:56","timestampDatum":"2017-12-20T21:46:54.000Z"},{"booleanDatum":false,"dateDatum":"2016-12-19","decimalDatum":12.345000000000000000,"doubleDatum":21.2121,"integerDatum":34,"longDatum":1520828123,"stringDatum":"breakdelimiter,test","timeDatum":"23:45:16","timestampDatum":"2017-12-29T17:21:49.000Z"}]""").toDF

    assert(TestDataUtils.datasetEquality(expected, actual))
  }   

}
