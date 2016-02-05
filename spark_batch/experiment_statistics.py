import csv, tarfile, os
import pyspark_cassandra
from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
import boto3, re
s3 = boto3.resource('s3')
conf = SparkConf().setAppName("ExperimentStats").setMaster("spark://ip-172-31-3-41:7077")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

RESULTS_DIR='hdfs://ec2-52-89-35-171.us-west-2.compute.amazonaws.com:9000/datamill/'
DATABASE = 'datamill'


# os.chdir(RESULTS_DIR)
my_bucket = s3.Bucket('yuguang-experiments')
file_list = my_bucket.objects.all()
for file in file_list:
    matches = re.search('^\d{1,4}_results_index.csv', file.key)
    if not matches:
        continue
    try:
        df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('s3n://yuguang-experiments/{}'.format(file.key))
    except:
        # in the case of empty files
        pass
    else:
        # drop rows that have null values in these columns
        df = df.dropna(how='any', subset=['setup_time', 'collect_time', 'run_time'])
        num_rows = 0
        # if the data frame only has no rows, then go on to the next csv file
        if df.count() <  1:
            continue
        header = df.columns
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
                # drop rows with null values
                avg_df = avg_df.na.drop()
                num_rows = num_rows + avg_df.count()
                avg_df.map(flatten).saveToCassandra(DATABASE, table_name)
        if num_rows > 0:
            df.select('experiment_id', 'job_id', 'package_name').distinct().map(flatten).saveToCassandra(DATABASE, 'jobs')
            df.select('experiment_id').distinct().map(flatten).saveToCassandra(DATABASE, 'experiments')
# df.groupBy('sw_swap', 'experiment_id').count('sw_swap').show()
# df.groupBy('sw_swap', 'experiment_id').agg({"setup_time":"avg"}).rdd
# df.select('hw_cpu_mhz').agg(countDistinct('hw_cpu_mhz')).show()
# df.select('hw_cpu_mhz').distinct().count()