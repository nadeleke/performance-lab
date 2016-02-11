#!/usr/bin/python3
#
#   Put documents from the stream into Kafka
#

import argparse
import os
import tarfile
from time import sleep

from kafka import SimpleProducer, KafkaClient


def chunk_iterable(A,n):
    '''An iterable that contains the iterates of A divided into lists of size n.
       A    iterable
       n    int, size of chunk
    '''
    cnt = 0
    chunk = []
    for v in A:
        if cnt<n:
            cnt+=1
            chunk.append(v)
        else:
            yield(chunk)
            cnt = 0
            chunk = []
    if len(chunk)>0:
        yield(chunk)

RESULTS_DIR='/var/datamill/results/'
# RESULTS_DIR='/home/ubuntu/results/'
BATCH_SIZE = 10000
LINES_PER_FILE = BATCH_SIZE * 100
if __name__=="__main__":

    # arg parsing
    parser = argparse.ArgumentParser(description="Feed Kafka a stream from a file")
    # parser.add_argument("file", help="A file of data, one datum per line")
    parser.add_argument("host", help="Public IP address of a Kafka node")
    parser.add_argument("topic", help="Kafka topic to feed")
    parser.add_argument("partition_key", default=None, help="Partition key")
    parser.add_argument("-p", "--port", default=9092, help="port for zookeeper, default 9092")
    parser.add_argument("-c", "--chunksize", default=100, help="Number of messages to send at one time,  default 100")
    parser.add_argument("-d", "--delay", default=1000, help="Delay in ms between shipment of chunks to Kafka, default 0")
    args = parser.parse_args()

    # get a client
    print("Connecting to Kafka node {0}:{1}".format(args.host, args.port))
    kafka = KafkaClient("{0}:{1}".format(args.host, args.port))
    producer = SimpleProducer(kafka, async=True, batch_send_every_n=BATCH_SIZE, async_queue_maxsize=BATCH_SIZE)

    def send_row(row):
        if args.partition_key:
            print("Sending messages to Kafka topic {0}, key {1}".format(args.topic, args.partition_key))
            producer.send_messages(args.topic, args.partition_key, row)
        else:
            print("Sending messages to Kafka topic {0}".format(args.topic))
            producer.send_messages(args.topic, row)

    os.chdir(RESULTS_DIR)
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Interfaces with a real world system where files are stored in a folder
    # Each tar file contains a folder which contains a csv file
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    for tar_file in os.listdir(RESULTS_DIR):
        if tar_file.endswith('_results.tar') and tar_file.startswith(
'experiment_'):
            if not os.path.exists(tar_file.replace('_results.tar', '')):
                tar = tarfile.open(tar_file)
                tar.extractall()
                tar.close()
            file_name = os.path.basename(tar_file)
            file_name_parts = file_name.split('_')
            folder_name = file_name_parts[0] + '_' + file_name_parts[1]
            experiment_id = file_name_parts[1]
            experiment_file = '{}experiment_{}/{}_results_index.csv'.format(RESULTS_DIR, experiment_id, experiment_id)
            if not os.path.exists(experiment_file):
                continue
            with open(experiment_file) as result_csv:
                print("Uploading data for experiment {0}".format(experiment_id))
                counter = 0
                # replay lines from a file to simulate a large number of workers
                while counter < LINES_PER_FILE:
                    # while replaying the file, start from beginning and reset header to None
                    header = None
                    for row in result_csv:
                        # the first line of CSV sheets starts with a header with names of the columns
                        if not header and row.startswith('experiment_id'):
                            # set header to current row
                            header = row
                            # check if this CSV file has all the columns
                            # if not, then don't send any lines from this file
                            if row.count(',') < 28:
                                counter = LINES_PER_FILE + 1
                                break
                            continue
                        # send row as an experiment result
                        row = '{},RES'.format(row)
                        send_row(row)
                        counter += 1
                    # send experiment done message
                    row = '{},DONE'.format(header)
                    send_row(row)
                    sleep(1.0*int(args.delay)/1000.0)