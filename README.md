# Data Analytics Engine
A scala based tool to receive messages from the Analytics Tool, parse them and relay the jobs to the spark jobserver.
Currently we offer a CLI.

## Features
  - POST job jar
  - POST job
  - Get job status **[TBD]**

## Example build.sbt file for a job jar
It is important to maintain version compatibility with the spark server & job server.

```scala
organization  := "eu.rawfie"
version       := "0.0.1"
scalaVersion  := "2.10.4"


resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"
resolvers += Resolver.sonatypeRepo("public")

libraryDependencies ++= Seq (
  "spark.jobserver" %% "job-server-api" % "0.6.0",
  "org.apache.spark" %% "spark-core" % "1.5.1"
)
```
