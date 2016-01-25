name := "spark_stream"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.5.2" % "provided",
  "org.apache.spark" % "spark-sql_2.10" % "1.5.2" % "provided",
  "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.5.0-M3",
  "org.apache.spark" % "spark-streaming_2.10" % "1.3.0" % "provided",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.3.0",
  "net.debasishg" %% "redisclient" % "3.0"
)

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}
