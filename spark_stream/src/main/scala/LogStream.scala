import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import com.datastax.spark.connector._

import java.sql.Timestamp
import java.text.DateFormat
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util
import java.sql

object LogStream {
  def convert_date(dateString: String):java.sql.Date = {
    val formatter:DateFormat = new SimpleDateFormat("MM/dd/yyyy HH-mm-ssaa")
    val date:java.util.Date = formatter.parse(dateString)
    val sqlDate = new java.sql.Date(date.getTime())
    sqlDate
  }

  def main(args: Array[String]) {

    val brokers = "ec2-52-34-250-158.us-west-2.compute.amazonaws.com:9092"
    val topics = "logs"
    val topicsSet = topics.split(",").toSet

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("spark_stream")
    val stream_sc = new StreamingContext(sparkConf, Seconds(2))

    // Create context to connect to cassandra
    val cassandraConf = new SparkConf(true)
      .set("spark.cassandra.connection.host","ec2-52-34-219-20.us-west-2.compute.amazonaws.com")
      .setAppName("spark_stream")

    val cassandra_sc = new SparkContext(cassandraConf)

    val sqlContext = new SQLContext(cassandra_sc)

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](stream_sc, kafkaParams, topicsSet)

    import sqlContext.implicits._
    // Get the lines and show results
    messages.foreachRDD { rdd =>

      val lines = rdd.map(_._2)
      val log_entries_DF = lines.map( x => {
        val tokens = x.split(":")
        LogEntry(convert_date(tokens(0)), tokens(1), tokens.slice(2, tokens.length).mkString(":"))}).toDF()
//      val tweets_per_source_DF = logEntriesDF.groupBy("id")
//        .agg("retweeted" -> "count")

      log_entries_DF.write.format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "log", "keyspace" -> "datamill_test")).mode(SaveMode.Append).save()
    }


    // Start the computation
    stream_sc.start()
    stream_sc.awaitTermination()
  }
}

case class LogEntry(time: java.sql.Date, level: String, message: String)

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}