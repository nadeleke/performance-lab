import csv, tarfile, os
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
sqlContext = SQLContext(sc)

RESULTS_DIR='/home/yuguang/Downloads/' #TODO change to HDFS folder
RESULTS_DIR='/var/datamill/results/'

os.chdir(RESULTS_DIR)
for tar_file in os.listdir(RESULTS_DIR):
    if tar_file.endswith('.tar'):
        tar = tarfile.open(tar_file)
        tar.extractall()
        tar.close()
        file_name = os.path.basename(tar_file)
        file_name_parts = file_name.split('_')
        folder_name = file_name_parts[0] + '_' + file_name_parts[1]
        experiment_id = file_name_parts[1]
        header = None
        with open('{}experiment_{}/{}_results_index.csv'.format(RESULTS_DIR, experiment_id, experiment_id)) as result_csv:
            reader = csv.reader(result_csv, delimiter=',')
            for row in reader:
                if not header:
                    header = row
                    break
        df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('{}experiment_{}/{}_results_index.csv'.format(RESULTS_DIR, experiment_id, experiment_id))
        for field in [i for i in header if not i.endswith('time')]:
            if df.select(field).distinct().count() > 1:
                df.groupBy(field).agg({"setup_time":"avg"})
                df.groupBy(field).agg({"run_time":"avg"})
                df.groupBy(field).agg({"collect_time":"avg"})


# df.groupBy('sw_swap', 'experiment_id').count('sw_swap').show()
# df.groupBy('sw_swap', 'experiment_id').agg({"setup_time":"avg"}).rdd
# df.select('hw_cpu_mhz').agg(countDistinct('hw_cpu_mhz')).show()
df.select('hw_cpu_mhz').distinct().count()