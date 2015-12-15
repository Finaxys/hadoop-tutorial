# for model auto-generation
java -jar ~/.m2/repository/org/apache/avro/avro-tools/1.7.7/avro-tools-1.7.7.jar compile schema avro/* src/main/java/

#to launch or compile the Avro Parquet Converter (MapReduce Job)
cp ~/.m2/repository/org/apache/avro/avro-mapred/1.7.7/avro-mapred-1.7.7-hadoop2.jar ~/.m2/repository/org/apache/avro/avro-mapred/1.7.7/avro-mapred-1.7.7.jar