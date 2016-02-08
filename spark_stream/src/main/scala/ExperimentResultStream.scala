import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.commons.math3.distribution.FDistribution
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

import com.redis.RedisClient
import com.redis.serialization._
import Parse.Implicits._

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

  def calculateStatistics(id:Int, jobDF:DataFrame): Double = {
    // group together trials by the worker ID and calculate average run times
    val run_time_DF = jobDF.groupBy(jobDF("experiment_id"), jobDF("worker_id"))

    val avg_run_time_DF = run_time_DF.agg("run_time" -> "avg")
    val summary_DF = jobDF.describe("run_time").toDF()
    // get the average run time from summary statistics
    val overall_avg_run_time = summary_DF.select("run_time").take(2)(1)(0)
    // calculate the degrees of freedom for treatment and residuals
    val deg_freedom_treat = avg_run_time_DF.count() - 1
    val deg_freedom_total = jobDF.count() - 1
    val deg_freedom_res = deg_freedom_total - deg_freedom_treat

    // calculate sum of squares
    val count_run_time_DF = run_time_DF.agg("run_time" -> "count")
    val diffSquaredDF = avg_run_time_DF.select((avg_run_time_DF("avg(run_time)") - overall_avg_run_time) * (avg_run_time_DF("avg(run_time)") - overall_avg_run_time))
    var SS_treat:Double = 0
    // calculate the sum of squares for treatments
    for (num_repetition <- count_run_time_DF.select("count(run_time)").rdd.map(r => r(0)).collect(); square <- diffSquaredDF.rdd.map(r => r(0)).collect()) {
      SS_treat += num_repetition.toString().toDouble * square.toString().toDouble
    }
    val joinedDF = jobDF.join(avg_run_time_DF, Seq("experiment_id", "worker_id"))
    // calculate the average regression sum of squares
    val run_time_Tuples = joinedDF.select("run_time","avg(run_time)").rdd.map(r => {
      (r.getDouble(0), r.getDouble(1))
    })
    val regressionMetrics = new RegressionMetrics(run_time_Tuples)
    // RegressionMetrics.explainedVariance returns the average regression sum of squares
    val SS_error:Double = regressionMetrics.explainedVariance * run_time_Tuples.count()

    // Mean Sum of Squares between the groups
    val MSB = SS_treat / deg_freedom_treat
    val MSE = SS_error / deg_freedom_res

    // Calculate the F-statistic and p-value
    val F_statistic = MSB / MSE
    val fdist = new FDistribution(null, deg_freedom_treat, deg_freedom_res)
    val pvalue = 1.0 - fdist.cumulativeProbability(F_statistic)
    pvalue
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val brokers = "ec2-52-34-250-158.us-west-2.compute.amazonaws.com:9092"
    val topics = "m5"
    val topicsSet = topics.split(",").toSet

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("spark_stream")
    // val ssc = new StreamingContext(sc, Seconds(1))
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("hdfs://ec2-52-89-35-171.us-west-2.compute.amazonaws.com:9000/tmp")

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    val redis = new RedisClient("54.213.91.125", 6379)

    val jobs = messages.map({ line => val pieces = line._2.split(",")
      ((pieces(0).toInt, pieces(5).toInt), pieces(9).toDouble)
    })
    val latestJobs = jobs.updateStateByKey((newJobs: Seq[Double], currentJobs: Option[Seq[Double]]) => {
        if (!currentJobs.isEmpty) {
          Option(currentJobs.get ++ newJobs)
        } else {
          Option(newJobs)
        }
      })
    latestJobs.foreachRDD { jobRDD =>
      val sqlContext = SQLContextSingleton.getInstance(jobRDD.sparkContext)
      import sqlContext.implicits._

      val jobDF = jobRDD.toDF()
      // calculate statistics for each experiment
      val statsByExp = jobDF.select("experiment_id").distinct().map(row => Map(
        "id" -> row.getInt(0),
        "statistics" -> calculateStatistics(row.getInt(0), jobDF.where(jobDF("experiment_id") === row.getInt(0)))))

      statsByExp.foreach(experiment => redis.hmset("statistic", Map("experiment_id" -> experiment("id"), "F_statistic" -> experiment("statistics"))))

    }
    // publish all experiment rows to redis
    messages.map(line => Job.parseJob(line._2)).foreachRDD { jobRDD =>
      jobRDD.foreach(job => redis.hmset("log", Map("experiment_id" -> job.experiment_id, "row" -> job.toString)))
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
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