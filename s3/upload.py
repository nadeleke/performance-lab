import os
import tarfile

import boto3

aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')
# Creating a simple connection
s3_client = boto3.client('s3')

RESULTS_DIR='/home/ubuntu/results/'

os.chdir(RESULTS_DIR)
for tar_file in os.listdir(RESULTS_DIR):
    if tar_file.endswith('_results.tar') and tar_file.startswith(
'experiment_'):
        print tar_file
        tar = tarfile.open(tar_file)
        tar.extractall()
        tar.close()
        file_name = os.path.basename(tar_file)
        file_name_parts = file_name.split('_')
        folder_name = file_name_parts[0] + '_' + file_name_parts[1]
        experiment_id = file_name_parts[1]
        os.chdir('{}experiment_{}/'.format(RESULTS_DIR, experiment_id, experiment_id))
        csv_path = '{}_results_index.csv'.format(experiment_id)
        s3_client.upload_file(csv_path, 'yuguang-experiments', csv_path)
        os.chdir(RESULTS_DIR)
        try:
            os.rmdir(tar_file.replace('_results.tar', ''))
        except:
            pass