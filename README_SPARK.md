# to start launch spark job :

#HBase Batch
spark-submit --class fr.finaxys.tutorials.utils.spark.batch.HBaseSparkRequester target/hadoop-tutorial-0.2-SNAPSHOT-jar-with-dependencies.jar
#Streaming HBase
spark-submit --class fr.finaxys.tutorials.utils.spark.streaming.HBaseStreamingAnalysis target/hadoop-tutorial-0.2-SNAPSHOT-jar-with-dependencies.jar
#Parquet Batch
spark-submit --class fr.finaxys.tutorials.utils.spark.batch.ParquetSparkRequester target/hadoop-tutorial-0.2-SNAPSHOT-jar-with-dependencies.jar

#Spark with yarn
spark-submit --num-executors 20 --driver-memory 12g --executor-cores 6 --master yarn-client

#cluster command 
/home/finaxys/spark-1.5.2-bin-hadoop2.6/bin/spark-submit

# to start spark unit test with miniCluster you should add to your jvm params :
-XX:MaxPermSize=1024m