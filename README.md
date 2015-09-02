# hadoop-tutorial
$HADOOP_HOME/bin/hdfs dfs -mkdir /user/
$HADOOP_HOME/bin/hdfs dfs -mkdir /user/pierre
mvn clean compile package
./atom-generate.sh 
$HADOOP_HOME/bin/hdfs dfs -put simul.out /user/pierre/simul.out
$HADOOP_HOME/bin/hadoop jar target/hadoop-tutorial-1.0-SNAPSHOT.jar fr.finaxys.tutorials.hadoop.TraceCount /user/pierre/simul.out /user/pierre/tracecount
$HADOOP_HOME/bin/hdfs dfs -cat /user/pierre/tracecount
$HADOOP_HOME/bin/hadoop jar target/hadoop-tutorial-1.0-SNAPSHOT.jar fr.finaxys.tutorials.hadoop.AgentPosition /user/pierre/simul.out /user/pierre/agentposition
$HADOOP_HOME/bin/hdfs dfs -cat /user/pierre/agentposition
$HADOOP_HOME/bin/hdfs dfs -rm -r /user/pierre/output3