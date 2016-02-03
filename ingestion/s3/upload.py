import os
import sleep
import tarfile
import tinys3

aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')
# Creating a simple connection
conn = tinys3.Connection(aws_access_key, aws_secret_access_key)

RESULTS_DIR='/var/datamill/results/'

for tar_file in os.listdir(RESULTS_DIR):
    if tar_file.endswith('_results.tar') and tar_file.startswith(
'experiment_'):
        try:
            print tar_file
            tar = tarfile.open(tar_file)
            tar.extractall()
            tar.close()
            file_name = os.path.basename(tar_file)
            file_name_parts = file_name.split('_')
            folder_name = file_name_parts[0] + '_' + file_name_parts[1]
            experiment_id = file_name_parts[1]
            first_line = True
            # Uploading a single file
            file_name = '{}experiment_{}/{}_results_index.csv'.format(RESULTS_DIR, experiment_id, experiment_id)
            f = open(file_name,'rb')
            conn.upload(file_name,f,'yuguang-dataset')
            sleep(2)
            os.rmdir(tar_file.replace('.tar', ''))
        except:
            # older experiment files have a different structure
            pass