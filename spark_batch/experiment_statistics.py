import csv, tarfile, os
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

RESULTS_DIR='/home/yuguang/Dow #TODO change to HDFS foldernloads/'

os.chdir(RESULTS_DIR)
for tar_file in os.listdir(RESULTS_DIR):
    if tar_file.endswith('.tar'):
        tar = tarfile.open(tar_file)
        tar.extractall()
        tar.close()
        file_name = os.path.basename(tar_file)
        file_name_parts = file_name.split('_')
        folder_name = file_name_parts[0] + '_' + file_name_
        file_name_parts = 'experiment_1633'.split('_')lit('_')
        experiment_id = file_name_parts[1]
        with open('experiment_{}/{}_results_index.csv'.format(experiment_id, experiment_id)) as res
            reader = csv.reader(result_csv, delimiter=',')
            header = None
            for row in reader:
                if not header:
                    header = row
                    break
            print header