name          := "data_analytics_engine"
organization  := "eu.rawfie"
version       := "0.0.1"
scalaVersion  := "2.10.4"

resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"
resolvers += Resolver.sonatypeRepo("public")

libraryDependencies ++= Seq (
  "com.github.scopt" %% "scopt" % "3.4.0",
  "org.scalaj" %% "scalaj-http" % "2.2.1",
  "org.json4s" %% "json4s-native" % "3.3.0",
   "org.apache.avro" % "avro" % "1.7.7"
)
