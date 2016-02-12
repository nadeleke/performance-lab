import com.hastyexperiment.Util._
import com.redis.RedisClient
import kafka.serializer.StringDecoder
import org.apache.commons.math3.distribution.FDistribution
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import java.lang.ArrayIndexOutOfBoundsException

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

  def resultTableUpdate(in: Seq[MessageTuple], lastState: Option[TimeTuple]): Option[TimeTuple] = {

    in.foldLeft(lastState) {
      (state, newTuple) => {
        val (newType, newSetupTime, newRunTime, newCollectTime, newCount) = newTuple

        newType match {

          // if the experiment is done, don't keep state for it
          case MessageType.EXPERIMENT_DONE => None

          // otherwise, update the state of the experiment and keep track of the run times
          case _ => {
            if (state.isDefined) {
              val (oldSetupTime, oldRunTime, oldCollectTime, oldCount) = state.get
              val setupTimeSum = newSetupTime.get + oldSetupTime.getOrElse(0.0)
              val runTimeSum = newRunTime.get + oldRunTime.getOrElse(0.0)
              val collectTimeSum = newCollectTime.get + oldCollectTime.getOrElse(0.0)
              val count = newCount.get + oldCount.getOrElse(0)
              Some((Option(setupTimeSum), Option(runTimeSum), Option(collectTimeSum), Option(count)))
            } else {
              Some ((newSetupTime, newRunTime, newCollectTime, newCount))
            }
          }
        }
      }
    }
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val brokers = "ec2-52-36-57-191.us-west-2.compute.amazonaws.com:9092"
    val topics = "m27"
    val topicsSet = topics.split(",").toSet

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("spark_stream")
    // val ssc = new StreamingContext(sc, Seconds(1))
    val ssc = new StreamingContext(sparkConf, Milliseconds(500))
    ssc.checkpoint("hdfs://ec2-52-89-35-171.us-west-2.compute.amazonaws.com:9000/tmp")

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    val wordsStream = kafkaStream.map(pair => {
      val (_, str) = pair
      str.stripLineEnd.split(",")
    }).persist()

    val jobStream = wordsStream.map({ pieces =>
      try {
        val experiment_id = pieces(0)
        val hw_cpu_arch = pieces(11)
        val setup_time = pieces(8).toDouble
        val run_time = pieces(9).toDouble
        val collect_time = pieces(10).toDouble
        val msgType = MessageType.fromMessageString(pieces(29))

        msgType match {
          case MessageType.RESULT => (experiment_id + ";" + hw_cpu_arch, (msgType, Some(setup_time), Some(run_time), Some(collect_time), Some(1)))
          case _ => (experiment_id + ";" + hw_cpu_arch, (msgType, None, None, None, None))
        }
      } catch {
        case e: ArrayIndexOutOfBoundsException => (";", (MessageType.EXPERIMENT_DONE, None, None, None, None));
      }
    })
//    jobStream.print()
    val latestJobs = jobStream.updateStateByKey(resultTableUpdate)
    // Filter out experiment with missing times and group by experiment id and architecture
    val jobLists = latestJobs.filter(pair => {
      val (key, (setup_time, run_time, collect_time, count)) = pair
      setup_time.isDefined && run_time.isDefined && collect_time.isDefined
    }).map(pair => {
      val (key, (setup_time, run_time, collect_time, count)) = pair
      (key, (setup_time.get, run_time.get, collect_time.get, count.get))
    }).groupByKey()

    // Calculate summary statistics for each key
    val stats = jobLists.map(pair => {
      val (k, jobList) = pair
      val words = k.split(";")
      val experiment_id = words(0).toInt
      val hw_cpu_arch = words(1)
//      val count = jobList.count()
      val count = jobList.map(_._4).sum
      println(f"jobList key: $k%s, count: $count%d")
      val (avg_setup, avg_run, avg_collect) = (jobList.map(_._1).sum / count, jobList.map(_._2).sum / count, jobList.map(_._3).sum / count)
      ((experiment_id, hw_cpu_arch), (avg_setup, avg_run, avg_collect, count))
    })

    stats.foreachRDD(rdd => {
      rdd.foreachPartition(pairs => {
        val redisClient = new RedisClient(redisHost, redisPort, secret=Option("1f56a48f2031433b385483b7566c85f1255af5d3dca24fa378a66645534cf8a7"))
        pairs.foreach(pair => {
          val ((id, arch), (avg_setup, avg_run, avg_collect, num_jobs)) = pair
          redisClient.set("experiment", f"$id%s,$arch%s,$avg_setup%s,$avg_run%s,$avg_collect%s,$num_jobs%s")
        })
      })
    })

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