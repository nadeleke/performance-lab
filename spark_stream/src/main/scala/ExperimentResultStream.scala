import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import com.datastax.spark.connector._

object ExperimentResultStream {

  case class Job(experiment_id: Int, job_id: Int, package_id: Int, worker_id: Int, setup_time: Double, run_time: Double, collect_time: Double, hw_cpu_arch: String, hw_cpu_mhz: Int, hw_gpu_mhz: Int, hw_num_cpus: Int, hw_page_sz: Int, hw_ram_mhz: Int, hw_ram_sz: Int, sw_address_randomization: String, sw_autogroup: String, sw_drop_caches: String, sw_freq_scaling: String, sw_link_order: String, sw_opt_flag: String, sw_swap: String, sw_sys_time: String)

  object Job {

    // function to parse line of csv data into Job class
    def parseJob(str: String): Job = {
      val p = str.split(",")
      if (p.length < 29) {
        throw new IllegalArgumentException("parseJob requires at least 29 columns")
      }
      Job(p(0).toInt, p(1).toInt, p(3).toInt, p(5).toInt, p(8).toDouble, p(9).toDouble, p(10).toDouble, p(11), p(12).toInt, p(13).toInt, p(14).toInt, p(15).toInt, p(16).toInt, p(17).toInt, p(18), p(19), p(21), p(24), p(25), p(26), p(27), p(28))
    }

  }

  def mean(l: Array[Double]): Double = {
    var i = 0
    var sum = 0
    while (i < l.length) {
      sum += l(i)
      i += 1
    }
    sum / l.length
  }

  def main(args: Array[String]) {
    val brokers = "ec2-52-34-250-158.us-west-2.compute.amazonaws.com:9092"
    val topics = "results"
    val topicsSet = topics.split(",").toSet

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("spark_stream")
    val stream_sc = new StreamingContext(sparkConf, Seconds(10))
    stream_sc.checkpoint("checkpoint")

    // Create context to connect to cassandra
    val cassandraConf = new SparkConf(true)
      .set("spark.cassandra.connection.host","ec2-52-34-219-20.us-west-2.compute.amazonaws.com")
      .setAppName("spark_stream")

    val cassandra_sc = new SparkContext(cassandraConf)

    val sqlContext = new SQLContext(cassandra_sc)

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](stream_sc, kafkaParams, topicsSet)

    // Get the lines and show results
    messages.foreachRDD { rdd =>
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._

      val lines = rdd.map(_._2)
      // parse the lines into job objects
      try {
        val jobRDD = lines.map(Job.parseJob)
        val jobDF = jobRDD.toDF()
        // group together trials with the same factor levels and calculate average run times
        val avg_run_time_DF = jobDF.groupBy(jobDF("avg_hw_cpu_arch"), jobDF("avg_hw_cpu_mhz"), jobDF("avg_hw_gpu_mhz"), jobDF("avg_hw_num_cpus"), jobDF("avg_hw_page_sz"), jobDF("avg_hw_ram_mhz"), jobDF("avg_hw_ram_sz"), jobDF("avg_sw_address_randomization"), jobDF("avg_sw_autogroup"), jobDF("avg_sw_compiler"), jobDF("avg_sw_drop_caches"), jobDF("avg_sw_env_padding"), jobDF("avg_sw_filesystem"), jobDF("avg_sw_freq_scaling"), jobDF("avg_sw_link_order"), jobDF("avg_sw_opt_flag"), jobDF("avg_sw_swap"), jobDF("avg_sw_sys_time"))
          .agg("run_time" -> "avg")
        val summary_DF = jobDF.describe("run_time").toDF()
        // get the average run time from summary statistics
        val overall_avg_run_time = summary_DF.select("run_time").take(2)

      } catch {
        case ex: IllegalArgumentException => print("skipping current job")
      }
//      tweets_per_source_DF.write.format("org.apache.spark.sql.cassandra")
//        .options(Map("table" -> "tweet_prototype", "keyspace" -> "twitter")).mode(SaveMode.Append).save()
    }


    // Start the computation
    stream_sc.start()
    stream_sc.awaitTermination()
  }
}

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