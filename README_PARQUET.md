#to execute the cluster mapReduce avro parquet converter:

hdfs dfs -rm -r /ParquetFile

sudo -u hdfs hdfs dfs -copyFromLocal /usr/hdp/2.3.2.0-2950/hadoop/mapreduce.tar.gz /hdp/apps/2.3.2.0-2950/mapreduce/

yarn jar target/hadoop-tutorial-0.2-SNAPSHOT-jar-with-dependencies.jar fr.finaxys.tutorials.utils.parquet.AvroParquetConverter