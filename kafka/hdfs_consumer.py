import time
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
import os
import argparse

class Consumer(object):

    def __init__(self, addr, group, topic):
        self.client = KafkaClient(addr)
        self.consumer = SimpleConsumer(self.client, group, topic, max_buffer_size=1310720000)
        self.temp_file_path = None
        self.temp_file = None
        self.topic = topic
        self.group = group
        self.block_cnt = 0


    def consume_topic(self):

        timestamp = time.strftime('%Y%m%d%H%M%S')

        #open file for writing
        self.temp_file_path = "/home/ubuntu/datamill/kafka_%s_%s_%s.dat" % (self.topic, self.group, timestamp)
        self.temp_file = open(self.temp_file_path,"w")
        header = 'experiment_id,job_id,results_file,package_id,package_name,worker_id,config_id,replicate_no,setup_time,run_time,collect_time,hw_cpu_arch,hw_cpu_mhz,hw_gpu_mhz,hw_num_cpus,hw_page_sz,hw_ram_mhz,hw_ram_sz,sw_address_randomization,sw_autogroup,sw_compiler,sw_drop_caches,sw_env_padding,sw_filesystem,sw_freq_scaling,sw_link_order,sw_opt_flag,sw_swap,sw_sys_time'
        self.temp_file.write(header)

        while True:
            try:
                messages = self.consumer.get_messages(count=100, block=False)

                for message in messages:
                    self.temp_file.write(message.message.value + "\n")

                if self.temp_file.tell() > 2000:
                    self.save_to_hdfs()

                self.consumer.commit()
            except:
                self.consumer.seek(0, 2)

        self.consumer.commit()

    def save_to_hdfs(self):
        self.temp_file.close()

        timestamp = time.strftime('%Y%m%d%H%M%S')
        hadoop_path = "/datamill/%s_%s_%s.csv" % (self.group, self.topic, timestamp)
        print "Block " + str(self.block_cnt) + ": Saving file to HDFS " + hadoop_path
        self.block_cnt += 1

        # place blocked messages into history and cached folders on hdfs
        os.system("sudo -u hdfs hdfs dfs -put %s %s" % (self.temp_file_path, hadoop_path))
        os.remove(self.temp_file_path)

        timestamp = time.strftime('%Y%m%d%H%M%S')

        self.temp_file_path = "/home/ubuntu/datamill/kafka_%s_%s_%s.dat" % (self.topic, self.group, timestamp)
        self.temp_file = open(self.temp_file_path, "w")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Read a Kafka stream and write files to HDFS")
    parser.add_argument("host", help="Public IP address of a Kafka node")
    parser.add_argument("topic", help="Kafka topic to feed")
    parser.add_argument("-p", "--port", default=9092, help="port for zookeeper, default 9092")
    args = parser.parse_args()
    print "\nConsuming topic: [%s] into HDFS" % args.topic
    cons = Consumer(addr="{0}:{1}".format(args.host, args.port), group="hdfs", topic=args.topic)
    cons.consume_topic()
    cons.save_to_hdfs()