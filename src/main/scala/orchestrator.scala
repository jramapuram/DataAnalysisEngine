package DataAnalysisEngine

import scopt._
import scalaj.http._

import java.nio.file.{Files, Paths}

import org.json4s._
import org.json4s.native.JsonMethods._

import scala.io.Source
import scala.collection.JavaConversions._

import org.apache.avro.util.Utf8
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericData, GenericRecord}


case class Config(endpoint: String = "localhost:8081"
                    , spark_endpoint: String = "localhost:8081"
                    , query: Boolean = false
                    , post_jar: Boolean = false
                    , post_jar_file: String = ""
                    , post_jar_target: String = ""
                    , post_job: Boolean = false
                    , post_job_app: String = ""
                    , post_job_conf_file: String = ""
                    , raw_json: Boolean = false
                    , classpath: String = ""
                    , revision: Int = -1)

class Parser {
  def parse(args: Array[String]): Option[Config] = {
    val p = new scopt.OptionParser[Config]("DataAnalyticsEngine") {
      head("\nDataAnalyticsEngine", "0.0.1")
      note("\n")

      cmd("query") action { (_, c) =>
        c.copy(query = true) } text("lists all available metrics from endpoint") children(
        opt[String]("endpoint") required() action { (x, c) =>
          c.copy(endpoint = x) } text("The schema-registry endpoint"),
        opt[Unit]("raw-json") action { (_, c) =>
          c.copy(raw_json = true) } text("Simply print the raw json instead of filtering the fields"),
        opt[Int]("revision") action { (x, c) =>
          c.copy(revision = x) } text("The revision to filter")
      )
      note("\n")

      cmd("post-jar") action { (_, c) =>
        c.copy(post_jar = true) } text("post a JAR to the jobserver") children(
        opt[String]("endpoint") required() action { (x, c) =>
          c.copy(endpoint = x) } text("The schema-registry endpoint"),
        opt[String]("spark-endpoint") required() action { (x, c) =>
          c.copy(spark_endpoint = x) } text("The spark endpoint"),
        opt[String]("jar-file") required() action { (x, c) =>
          c.copy(post_jar_file = x) } text("The path to the file to post"),
        opt[String]("target-app") required() action { (x, c) =>
          c.copy(post_jar_target = x) } text("The destination app name")
      )
      note("\n")

      cmd("post-job") action { (_, c) =>
        c.copy(post_job = true) } text("post a job to to the jobserver") children(
        opt[String]("spark-endpoint") required() action { (x, c) =>
          c.copy(spark_endpoint = x) } text("The spark endpoint"),
        opt[String]("app") required() action { (x, c) =>
          c.copy(post_job_app = x) } text("The application name"),
        opt[String]("classpath") action { (x, c) =>
          c.copy(classpath = x) } text("The classpath for the entrypoint into the job [optional / needed for custom jobs]"),
        opt[String]("conf-file") required() action { (x, c) =>
          c.copy(post_job_conf_file = x) } text("The path to the config file to post")
      )
      note("\n")

      help("help") text("prints this usage text")
      //TODO: Conf verification
      // checkConfig { c =>
      //   if (c.query != true && c.metric == "") failure("Please specify a metric")
      //   //else if ()
      //   else success }
    }

    return p.parse(args, Config())
  }
}

object Orchestrator {
  private val version: String = "0.0.1"; // TODO: Property-fy

  def getSchema(endpoint: String, name: String, raw_format: Boolean = false, revision: Int = -1) {
    val full_schema = revision match {
      case -1 => parse(Http("http://%s/subjects/%s/versions/latest".format(endpoint, name)).asString.body)
      case _  => parse(Http("http://%s/subjects/%s/versions/%d".format(endpoint, name, revision)).asString.body)
    }

    implicit val formats = DefaultFormats
    val schema_str = (full_schema \ "schema").extract[String]
    if(schema_str.contains("fields")) {
      raw_format match {
        case true  => println(pretty(render(parse(schema_str))) + "\n")
        case false => {
          val parser = new Schema.Parser()
          val fields = parser.parse(schema_str)
          val featureMap = collection.mutable.Map.empty[String,String]
          fields.getFields().foreach(field => {featureMap += field.name() -> field.schema().toString()
                                                 .substring(1, field.schema().toString().length()-1)})
          featureMap.foreach(feature => println(feature._1 + "-->" + feature._2 + "\n"))
        }
      }
    }
  }

  def query(conf: Config) {
    val subjects = Http("http://%s/subjects".format(conf.endpoint)).asString.body
    val filtered_subjects = subjects.replaceAll("[\\[\"\\]]", "").split(",")
    for (subject <- filtered_subjects){
      getSchema(conf.endpoint, subject, conf.raw_json, conf.revision)
    }
  }

  def uuid = java.util.UUID.randomUUID.toString

  def postJar(conf: Config) {
    try{
      val byteArray = Files.readAllBytes(Paths.get(conf.post_jar_file))
      val postResult = Http("http://%s/jars/%s".format(conf.spark_endpoint, conf.post_jar_target)).postData(byteArray)
      println("POST jar result: " + postResult.asString.code)
    }catch {
      case ex: Exception => println("Couldn't POST JAR: " + ex)
    }
  }

  def classPathFromName(name: String) : String = name.toLowerCase().trim() match {
    case "anomaly" => "rawfie.eu.analysis.anomalyjob.AnomalyJob"
    case _         => throw new IllegalArgumentException("Unknown model type")
  }

  def buildContext(conf: Config, rndId: String, retryCount: Int) {
    val ctx = Http("http://%s/contexts/%s?num-cpu-cores=4&memory-per-node=1024m&context-factory=spark.jobserver.context.StreamingContextFactory"
                     .format(conf.spark_endpoint, rndId)).timeout(connTimeoutMs=50000, readTimeoutMs=50000).postData("")
    ctx.asString.code match {
      case 200 => println("Successfully created context")
      case _   =>
        {
          if(retryCount <= 0)
            throw new Exception("Error creating context: " + ctx.asString.body)
          else
            buildContext(conf, rndId, retryCount - 1)
        }
    }
  }

  def postConfig(byteArray: Array[Byte], conf: Config, rndId: String, retryCount: Int) {
    val cp = conf.classpath match {
      case ""  => classPathFromName(conf.post_job_app)
      case _   => conf.classpath
    }

    val postResult = Http("http://%s/jobs?appName=%s&classPath=%s&context=%s"
                            .format(conf.spark_endpoint, conf.post_job_app, cp, rndId)).postData(byteArray)
    //.header("content-type", "application/json")

    postResult.asString.code match{
      case 200 => println(rndId)
      case 202 => println(rndId)
      case _   =>
        {
          if(retryCount <= 0)
            throw new Exception("Error posting job: " + postResult.asString.body)
          else
            postConfig(byteArray, conf, rndId, retryCount - 1)
        }
    }
  }

  def postJob(conf: Config) {
    try{
      val byteArray = Files.readAllBytes(Paths.get(conf.post_job_conf_file))
      val rndId = "ctx-" + uuid

      buildContext(conf, rndId, 3)          // try 3 times to build a context
      postConfig(byteArray, conf, rndId, 3) // try to post the actual job definitions 3 times

    }catch {
      case ex: Exception => println("Couldn't POST job: " + ex)
    }
  }


  def main(args: Array[String]) {
    // parse our config
    val conf = new Parser().parse(args) match {
      case Some(value) => value
      case None => throw new IllegalArgumentException("invalid arguments")
    }

    // handle query parsing
    if (conf.query)
      query(conf)

    // handle jar posting
    if (conf.post_jar)
      postJar(conf)

    // handle job posting
    if (conf.post_job)
      postJob(conf)

  }
}
