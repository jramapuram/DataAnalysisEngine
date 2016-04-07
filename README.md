# Data Analytics Engine
A scala based tool to ease the posting of jobs and job-parameters to the spark jobserver.  
This tool decouples 'models' from 'model parameters'. A model is a JAR that needs to be built once. It accepts input in terms of a JSON blob which it then parses to start a job.  
  
The DataAnalysisEngine automatically builds a new random Spark context for each job which it utilizes to run a SparkStreaming job with parameters K. You can run as many jobs using the same JAR as you would like.

## Features
  - POST job jar
  - POST job
  - Get job status **[TBD]**
```bash
DataAnalyticsEngine 0.0.1
Usage: DataAnalyticsEngine [query|post-jar|post-job] [options]



Command: query [options]
lists all available metrics from endpoint
  --endpoint <value>
        The schema-registry endpoint
  --raw-json
        Simply print the raw json instead of filtering the fields
  --revision <value>
        The revision to filter


Command: post-jar [options]
post a JAR to the jobserver
  --spark-endpoint <value>
        The spark endpoint
  --jar-file <value>
        The path to the file to post
  --target-app <value>
        The destination app name


Command: post-job [options]
post a job to to the jobserver
  --spark-endpoint <value>
        The spark endpoint
  --app <value>
        The application name
  --classpath <value>
        The classpath for the entrypoint into the job [optional / needed for custom jobs]
  --conf-file <value>
        The path to the config file to post


  --help
        prints this usage text
```

## post-jar example
```bash
sbt "run post-jar --spark-endpoint spark-jobserver:8090 --jar-file ~/myprojectjar/target/scala-2.10/anomaly-job.jar --target-app my_app_name"
```

## post-job example
```bash
sbt "run post-job --spark-endpoint spark-jobserver:8090 --app my_app_name --conf-file ./params.config"
```

## Example build.sbt file for a job jar
It is important to maintain version compatibility with the spark server & job server.  
An example of the above is:

```scala
organization  := "my.org"
version       := "0.0.1"
scalaVersion  := "2.10.4"


resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"
resolvers += Resolver.sonatypeRepo("public")

libraryDependencies ++= Seq (
  "spark.jobserver" %% "job-server-api" % "0.6.0",
  "org.apache.spark" %% "spark-core" % "1.5.1"
)
```
