import pyspark_cassandra
import pyspark_cassandra.streaming

from pyspark_cassandra import CassandraSparkContext

from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1

conf = SparkConf().setAppName("Experiment Statistics")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# set up our contexts
sc = CassandraSparkContext(conf=conf)
sql = SQLContext(sc)
stream = StreamingContext(sc, 60*60) # 1 hour window

kafka_stream = KafkaUtils.createStream(stream,                                       "ec2-52-89-46-183.us-west-2.compute.amazonaws.com:2181", "experiment_results")

DATABASE = 'datamill'

df = kafka_stream.map(lambda (k, v): v.split(','))

header = 'experiment_id,job_id,results_file,package_id,package_name,worker_id,config_id,replicate_no,setup_time,run_time,collect_time,hw_cpu_arch,hw_cpu_mhz,hw_gpu_mhz,hw_num_cpus,hw_page_sz,hw_ram_mhz,hw_ram_sz,sw_address_randomization,sw_autogroup,sw_compiler,sw_drop_caches,sw_env_padding,sw_filesystem,sw_freq_scaling,sw_link_order,sw_opt_flag,sw_swap,sw_sys_time'.split(',')

for field in [i for i in header if not i.endswith('time')]:
    if df.select(field).distinct().count() > 1:
        for time_field in ['setup', 'run', 'collect']:
            performance_metric_field = time_field + '_time'
            table_name = 'avg_by_' + field
            df.groupBy(field, 'experiment_id').agg({performance_metric_field: "avg"}).show()

stream.start()
stream.awaitTermination()