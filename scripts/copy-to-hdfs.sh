cd /var/datamill/results
find . -name "experiment*" -size -32M -delete
hdfs dfs -copyFromLocal /var/datamill/results /
