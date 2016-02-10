import com.hastyexperiment.Util._
import com.redis.RedisClient
import kafka.serializer.StringDecoder
import org.apache.commons.math3.distribution.FDistribution
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object ExperimentResultStream {

  case class Job(experiment_id: Int, hw_cpu_arch: String, run_time: Double)

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
    val topics = "m6"
    val topicsSet = topics.split(",").toSet

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("spark_stream")
    // val ssc = new StreamingContext(sc, Seconds(1))
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint("hdfs://ec2-52-89-35-171.us-west-2.compute.amazonaws.com:9000/tmp")

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    val wordsStream = kafkaStream.map(pair => {
      val (_, str) = pair
      str.stripLineEnd.split(",")
    }).persist()

    val jobStream = wordsStream.map({ pieces =>
      val experiment_id = pieces(0)
      val hw_cpu_arch = pieces(6)
      val setup_time = pieces(3).toDouble
      val run_time = pieces(4).toDouble
      val collect_time = pieces(5).toDouble
      val msgType = MessageType.fromMessageString(pieces(7))

      msgType match {
        case MessageType.RESULT => (experiment_id + ";" + hw_cpu_arch, (msgType, Some(setup_time), Some(run_time), Some(collect_time), Some(1)))
        case _ => (experiment_id + ";" + hw_cpu_arch, (msgType, None, None, None, None))
      }
    })
//    jobStream.print()
    val latestJobs = jobStream.updateStateByKey(resultTableUpdate)
    latestJobs.print()

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