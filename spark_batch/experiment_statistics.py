
RESULTS_DIR='/home/yuguang/Downloads/'
os.chdir(RESULTS_DIR)
for tar_file in os.listdir(RESULTS_DIR):
    if tar_file.endswith('.tar'):
        tar = tarfile.open(tar_file)
        tar.extractall()
        tar.close()
        file_name = os.path.basename(tar_file)
        file_name_parts = file_name.split('_')
        folder_name = file_name_parts[0] + '_' + file_name_parts[1]
        print folder_name
        file_name_parts = 'experiment_1633'.split('_')
        experiment_id = file_name_parts[1]
        with open('experiment_{}/{}_results_index.csv'.format(experiment_id, experiment_id)) as result_csv:
            reader = csv.reader(result_csv, delimiter=',')
            for row in reader:
                print row