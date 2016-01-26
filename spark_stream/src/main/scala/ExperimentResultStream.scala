import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import com.datastax.spark.connector._

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

object ExperimentResultStream {

  val cfDataBytes = Bytes.toBytes("data")

  case class Job(experiment_id: Int, job_id: Int, package_id: Int, worker_id: Int, setup_time: Double, run_time: Double, collect_time: Double, hw_cpu_arch: String, hw_cpu_mhz: Int, hw_gpu_mhz: Int, hw_num_cpus: Int, hw_page_sz: Int, hw_ram_mhz: Int, hw_ram_sz: Int, sw_address_randomization: String, sw_autogroup: String, sw_drop_caches: String, sw_freq_scaling: String, sw_link_order: String, sw_opt_flag: String, sw_swap: String, sw_sys_time: String)

  object Job {

    // function to parse line of csv data into Job class
    def parseJob(str: String): Job = {
      val p = str.split(",")
      Job(p(0).toInt, p(1).toInt, p(3).toInt, p(5).toInt, p(8).toDouble,
        p(9).toDouble, p(10).toDouble, p(11), p(12).toInt, p(13).toInt, p(14).toInt, p(15).toInt, p(16).toInt, p(17).toInt, p(18), p(19), p(21), p(24), p(25), p(26), p(27), p(28))
    }

    //  Convert a row of job object data to an HBase put object
    def convertToPut(job: Job): (ImmutableBytesWritable, Put) = {
      // create a composite row key: jobid_date time
      val rowkey = job.experiment_id + "_" + job.package_id + "_" + job.worker_id
      val put = new Put(Bytes.toBytes(rowkey))
      // add to column family data, column  data values to put object
      // as a slower alternative, first loop over all the fields http://stackoverflow.com/questions/28458881/scala-how-to-access-a-class-property-dynamically-by-name
      put.add(cfDataBytes, Bytes.toBytes("job_id"), Bytes.toBytes(job.job_id))
      put.add(cfDataBytes, Bytes.toBytes("setup_time"), Bytes.toBytes(job.setup_time))
      put.add(cfDataBytes, Bytes.toBytes("run_time"), Bytes.toBytes(job.run_time))
      put.add(cfDataBytes, Bytes.toBytes("collect_time"), Bytes.toBytes(job.collect_time))
      put.add(cfDataBytes, Bytes.toBytes("hw_cpu_arch"), Bytes.toBytes(job.hw_cpu_arch))
      put.add(cfDataBytes, Bytes.toBytes("hw_cpu_mhz"), Bytes.toBytes(job.hw_cpu_mhz))
      put.add(cfDataBytes, Bytes.toBytes("hw_gpu_mhz"), Bytes.toBytes(job.hw_gpu_mhz))
      put.add(cfDataBytes, Bytes.toBytes("hw_num_cpus"), Bytes.toBytes(job.hw_num_cpus))
      put.add(cfDataBytes, Bytes.toBytes("hw_page_sz"), Bytes.toBytes(job.hw_page_sz))
      put.add(cfDataBytes, Bytes.toBytes("hw_ram_mhz"), Bytes.toBytes(job.hw_ram_mhz))
      put.add(cfDataBytes, Bytes.toBytes("hw_ram_sz"), Bytes.toBytes(job.hw_ram_sz))
      put.add(cfDataBytes, Bytes.toBytes("sw_address_randomization"), Bytes.toBytes(job.sw_address_randomization))
      put.add(cfDataBytes, Bytes.toBytes("sw_autogroup"), Bytes.toBytes(job.sw_autogroup))
      put.add(cfDataBytes, Bytes.toBytes("sw_drop_caches"), Bytes.toBytes(job.sw_drop_caches))
      put.add(cfDataBytes, Bytes.toBytes("sw_freq_scaling"), Bytes.toBytes(job.sw_freq_scaling))
      put.add(cfDataBytes, Bytes.toBytes("sw_link_order"), Bytes.toBytes(job.sw_link_order))
      put.add(cfDataBytes, Bytes.toBytes("sw_opt_flag"), Bytes.toBytes(job.sw_opt_flag))
      put.add(cfDataBytes, Bytes.toBytes("sw_swap"), Bytes.toBytes(job.sw_swap))
      put.add(cfDataBytes, Bytes.toBytes("sw_sys_time"), Bytes.toBytes(job.sw_sys_time))
      return (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
    }

  }

  def main(args: Array[String]) {
    val tableName = "experiment"

    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val jobConfig: JobConf = new JobConf(conf, this.getClass)
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val brokers = "ec2-52-34-250-158.us-west-2.compute.amazonaws.com:9092"
    val topics = "logs"
    val topicsSet = topics.split(",").toSet

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("spark_stream")
    val sc = new SparkContext(sparkConf)
    val stream_sc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](stream_sc, kafkaParams, topicsSet)

    // for each RDD calculate the run time average grouped by worker and package
    messages.foreachRDD { rdd =>
      val lines = rdd.map(_._2)
      // parse the lines into job objects
      val jobRDD = lines.map(Job.parseJob)
      // convert job data to put object and write to HBase
      jobRDD.map(Job.convertToPut).saveAsHadoopDataset(jobConfig)
    }

    // Start the computation
    stream_sc.start()
    stream_sc.awaitTermination()
  }
}
