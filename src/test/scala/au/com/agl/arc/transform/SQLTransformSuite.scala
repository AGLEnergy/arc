package au.com.agl.arc

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import au.com.agl.arc.api._
import au.com.agl.arc.api.API._
import au.com.agl.arc.util.log.LoggerFactory 

import au.com.agl.arc.util._

class SQLTransformSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val targetFile = FileUtils.getTempDirectoryPath() + "transform.parquet" 
  val inputView = "inputView"
  val outputView = "outputView"

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

    // recreate test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile)) 
    // parquet does not support writing NullType
    // include partition by to test pushdown
    TestDataUtils.getKnownDataset.drop($"nullDatum").withColumn("_monotonically_increasing_id", monotonically_increasing_id()).write.partitionBy("dateDatum").parquet(targetFile)  
  }

  after {
    session.stop()

    // clean up test dataset
    FileUtils.deleteQuietly(new java.io.File(targetFile))         
  }

  test("SQLTransform") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset
    dataset.createOrReplaceTempView(inputView)

    val transformed = transform.SQLTransform.transform(
      SQLTransform(
        name="SQLTransform", 
        description=None,
        inputURI=new URI(targetFile),
        sql=s"SELECT * FROM ${inputView} WHERE booleanDatum = FALSE",
        outputView=outputView,
        persist=false,
        sqlParams=Map.empty,
        params=Map.empty,
        numPartitions=None,
        partitionBy=Nil           
      )
    ).get

    val actual = transformed.drop($"nullDatum")
    val expected = dataset.filter(dataset("booleanDatum")===false).drop($"nullDatum")

    assert(TestDataUtils.datasetEquality(expected, actual))
  }  

  test("SQLTransform: end-to-end") {
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=Nil, disableDependencyValidation=false)

    val conf = s"""{
      "stages": [
        {
          "type": "SQLTransform",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${getClass.getResource("/conf/sql/").toString}/basic.sql",
          "outputView": "customer",
          "persist": false,
          "sqlParams": {
            "placeholder": "value",
          }
        }
      ]
    }"""
    
    val argsMap = collection.mutable.Map[String, String]()
    val graph = ConfigUtils.Graph(Nil, Nil, false)
    val pipelineEither = ConfigUtils.parseConfig(Left(conf), argsMap, graph, arcContext)

    pipelineEither match {
      case Left(_) => assert(false)
      case Right((pipeline, _, _)) => ARC.run(pipeline)(spark, logger, arcContext)
    }  
  }

  test("SQLTransform: persist") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset
    dataset.createOrReplaceTempView(inputView)

    transform.SQLTransform.transform(
      SQLTransform(
        name="SQLTransform", 
        description=None,
        inputURI=new URI(targetFile),
        sql=s"SELECT * FROM ${inputView}",
        outputView=outputView,
        persist=false,
        sqlParams=Map.empty,
        params=Map.empty,
        numPartitions=None,
        partitionBy=Nil           
      )
    )
    assert(spark.catalog.isCached(outputView) === false)

    transform.SQLTransform.transform(
      SQLTransform(
        name="SQLTransform", 
        description=None,
        inputURI=new URI(targetFile),
        sql=s"SELECT * FROM ${inputView}",
        outputView=outputView,
        persist=true,
        sqlParams=Map.empty,
        params=Map.empty,
        numPartitions=None,
        partitionBy=Nil           
      )
    )
    assert(spark.catalog.isCached(outputView) === true)    
  }    

  test("SQLTransform: sqlParams") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val dataset = TestDataUtils.getKnownDataset
    dataset.createOrReplaceTempView(inputView)

    val transformed = transform.SQLTransform.transform(
      SQLTransform(
        name="SQLTransform", 
        description=None,
        inputURI=new URI(targetFile),
        sql=s"SELECT * FROM ${inputView} WHERE booleanDatum = $${sql_boolean_param}",
        outputView=outputView,
        persist=false,
        sqlParams=Map("sql_boolean_param" -> "FALSE"),
        params=Map.empty,
        numPartitions=None,
        partitionBy=Nil           
      )
    ).get

    val actual = transformed.drop($"nullDatum")
    val expected = dataset.filter(dataset("booleanDatum")===false).drop($"nullDatum")

    assert(TestDataUtils.datasetEquality(expected, actual))
  }     

  test("SQLTransform: sqlParams missing") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    // try with wildcard
    val thrown0 = intercept[Exception with DetailException] {
      val transformed = transform.SQLTransform.transform(
        SQLTransform(
          name="SQLTransform", 
          description=None,
          inputURI=new URI(targetFile),
          sql=s"SELECT * FROM ${inputView} WHERE stringDatum = $${sql_string_param} AND booleanDatum = $${sql_boolean}",
          outputView=outputView,
          persist=false,
          sqlParams=Map("sql_string_param" -> "test"),
          params=Map.empty,
          numPartitions=None,
          partitionBy=Nil           
        )
      ).get
    }
    assert(thrown0.getMessage === "No replacement value found in sqlParams [sql_string_param] for placeholders: [${sql_boolean}].")
  }     

  test("SQLTransform: partitionPushdown") {
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=Nil, disableDependencyValidation=false)

    extract.ParquetExtract.extract(
      ParquetExtract(
        name=inputView,
        description=None,
        cols=Right(Nil),
        outputView=outputView,
        input=targetFile,
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        basePath=None,
        contiguousIndex=true
      )
    )    

    val transformed = transform.SQLTransform.transform(
      SQLTransform(
        name="SQLTransform", 
        description=None,
        inputURI=new URI(targetFile),
        sql=s"SELECT * FROM ${outputView} WHERE dateDatum = TO_DATE('2016-12-19')",
        outputView=outputView,
        persist=false,
        sqlParams=Map.empty,
        params=Map.empty,
        numPartitions=None,
        partitionBy=Nil           
      )
    ).get

    val partitionFilters = QueryExecutionUtils.getPartitionFilters(transformed.queryExecution.executedPlan).toArray.mkString(",")
    assert(partitionFilters.contains("dateDatum"))
    assert(partitionFilters.contains("isnotnull(dateDatum"))
  }  

  test("SQLTransform: predicatePushdown") {
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val transformed = transform.SQLTransform.transform(
      SQLTransform(
        name="SQLTransform", 
        description=None,
        inputURI=new URI(targetFile),
        sql=s"SELECT * FROM parquet.`${targetFile}` WHERE booleanDatum = $${sql_boolean_param}",
        outputView=outputView,
        persist=false,
        sqlParams=Map("sql_boolean_param" -> "FALSE"),
        params=Map.empty,
        numPartitions=None,
        partitionBy=Nil           
      )
    ).get

    val dataFilters = QueryExecutionUtils.getDataFilters(transformed.queryExecution.executedPlan).toArray.mkString(",")
    assert(dataFilters.contains("isnotnull(booleanDatum"))
    assert(dataFilters.contains("),(booleanDatum"))
    assert(dataFilters.contains(" = false)"))
  }    

  test("SQLTransform: partitionPushdown and predicatePushdown") {
    implicit val spark = session
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = ARCContext(jobId=None, jobName=None, environment="test", environmentId=None, configUri=None, isStreaming=false, ignoreEnvironments=false, lifecyclePlugins=Nil, disableDependencyValidation=false)

    extract.ParquetExtract.extract(
      ParquetExtract(
        name=inputView,
        description=None,
        cols=Right(Nil),
        outputView=inputView,
        input=targetFile,
        authentication=None,
        params=Map.empty,
        persist=false,
        numPartitions=None,
        partitionBy=Nil,
        basePath=None,
        contiguousIndex=true
      )
    )

    val transformed = transform.SQLTransform.transform(
      SQLTransform(
        name="SQLTransform", 
        description=None,
        inputURI=new URI(targetFile),
        sql=s"SELECT * FROM ${inputView} WHERE DAY(dateDatum) = 19 AND booleanDatum = FALSE",
        outputView=outputView,
        persist=false,
        sqlParams=Map.empty,
        params=Map.empty,
        numPartitions=None,
        partitionBy=Nil           
      )
    ).get

    val partitionFilters = QueryExecutionUtils.getPartitionFilters(transformed.queryExecution.executedPlan).toArray.mkString(",")
    assert(partitionFilters.contains("(dayofmonth(dateDatum"))
    assert(partitionFilters.contains(") = 19)"))
    val dataFilters = QueryExecutionUtils.getDataFilters(transformed.queryExecution.executedPlan).toArray.mkString(",")
    assert(dataFilters.contains("isnotnull(booleanDatum"))
    assert(dataFilters.contains("),(booleanDatum"))
    assert(dataFilters.contains(" = false)"))
  }    


  test("SQLTransform: Execute with Structured Streaming" ) {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)

    val readStream = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", "1")
      .load

    readStream.createOrReplaceTempView("readstream")

    val transformDataset = transform.SQLTransform.transform(
      SQLTransform(
        name="SQLTransform", 
        description=None,
        inputURI=new URI(targetFile),
        sql=s"SELECT * FROM readstream",
        outputView=outputView,
        persist=false,
        sqlParams=Map.empty,
        params=Map.empty,
        numPartitions=None,
        partitionBy=Nil           
      )
    ).get

    val writeStream = transformDataset
      .writeStream
      .queryName("transformed") 
      .format("memory")
      .start

    val df = spark.table("transformed")

    try {
      Thread.sleep(2000)
      assert(df.count > 0)
    } finally {
      writeStream.stop
    }
  }     
}
