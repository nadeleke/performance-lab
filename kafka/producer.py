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

if __name__=="__main__":

    # arg parsing
    parser = argparse.ArgumentParser(description="Feed Kafka a stream from a file")
    # parser.add_argument("file", help="A file of data, one datum per line")
    parser.add_argument("host", help="Public IP address of a Kafka node")
    parser.add_argument("topic", help="Kafka topic to feed")
    parser.add_argument("-p", "--port", default=9092, help="port for zookeeper, default 9092")
    parser.add_argument("-c", "--chunksize", default=100, help="Number of messages to send at one time,  default 100")
    parser.add_argument("-d", "--delay", default=1000, help="Delay in ms between shipment of chunks to Kafka, default 0")
    args = parser.parse_args()

    # get a client
    print("Connecting to Kafka node {0}:{1}".format(args.host, args.port))
    kafka = KafkaClient("{0}:{1}".format(args.host, args.port))
    producer = SimpleProducer(kafka)

    os.chdir(RESULTS_DIR)
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Interfaces with a real world system where files are stored in a folder
    # Each tar file contains a folder which contains a csv file
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
                with open('{}experiment_{}/{}_results_index.csv'.format(RESULTS_DIR, experiment_id, experiment_id)) as result_csv:
                    for row in result_csv:
                        if first_line:
                            first_line = False
                            continue
                        producer.send_messages(args.topic, row)
                        sleep(1.0*int(args.delay)/1000.0)
                os.rmdir(tar_file.replace('.tar', ''))
            except:
                # older experiment files have a different structure
                pass