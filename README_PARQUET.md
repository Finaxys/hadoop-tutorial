#to launch or compile the Avro Parquet Converter (MapReduce Job)
cp ~/.m2/repository/org/apache/avro/avro-mapred/1.7.7/avro-mapred-1.7.7-hadoop2.jar ~/.m2/repository/org/apache/avro/avro-mapred/1.7.7/avro-mapred-1.7.7.jar

#to execute the cluster mapReduce job :
hdfs dfs -rm -r /ParquetFile
sudo -u hdfs hdfs dfs -copyFromLocal /usr/hdp/2.3.2.0-2950/hadoop/mapreduce.tar.gz /hdp/apps/2.3.2.0-2950/mapreduce/
yarn jar target/hadoop-tutorial-0.2-SNAPSHOT-jar-with-dependencies.jar fr.finaxys.tutorials.utils.parquet.AvroParquetConverter