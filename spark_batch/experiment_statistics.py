import csv, tarfile, os
import pyspark_cassandra
from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
import settings
conf = SparkConf().setAppName("ExperimentStats").setMaster("spark://ip-172-31-3-41:7077")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

RESULTS_DIR='hdfs://ec2-52-89-35-171.us-west-2.compute.amazonaws.com:9000/datamill/'
DATABASE = 'datamill'


# os.chdir(RESULTS_DIR)
# file_list = os.listdir(RESULTS_DIR)
file_list = ['1525_results_index.csv']
for file_name in file_list:
    header = settings.CSV_HEADER
    df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('{}{}'.format(RESULTS_DIR, file_name))
    for field in [i for i in header if not i.endswith('time')]:
        if field.startswith('hw') or field.startswith('sw') and df.select(field).distinct().count() > 1:
            table_name = 'avg_' + field
            avg_df = df.groupBy('experiment_id', field).agg({'setup_time': "avg",'collect_time': "avg",'run_time': "avg"})
            for time_field in ['setup', 'run', 'collect']:
                performance_metric_field = time_field + '_time'
                avg_df = avg_df.withColumnRenamed('avg({})'.format(performance_metric_field), performance_metric_field)
            # avg_df.show()
            # to avoid bug when saving rdd directly using saveToCassandra
            def flatten(x):
              x_dict = x.asDict()
              return x_dict
            avg_df.map(flatten).saveToCassandra(DATABASE, table_name)

# df.groupBy('sw_swap', 'experiment_id').count('sw_swap').show()
# df.groupBy('sw_swap', 'experiment_id').agg({"setup_time":"avg"}).rdd
# df.select('hw_cpu_mhz').agg(countDistinct('hw_cpu_mhz')).show()
# df.select('hw_cpu_mhz').distinct().count()