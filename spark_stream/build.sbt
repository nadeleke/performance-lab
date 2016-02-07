name := "spark_stream"

version := "1.0"

scalaVersion := "2.10.4"

resolvers += "Apache HBase" at "https://repository.apache.org/content/repositories/releases"

resolvers += "Thrift" at "http://people.apache.org/~rawson/repo/"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.5.2" % "provided",
  "org.apache.spark" % "spark-sql_2.10" % "1.5.2" % "provided",
  "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.5.0-M3",
  "org.apache.spark" % "spark-streaming_2.10" % "1.3.0" % "provided",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.3.0",
  "net.debasishg" %% "redisclient" % "3.0"
)

libraryDependencies += "org.apache.commons" % "commons-math3" % "3.5"


mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}
