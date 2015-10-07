# hadoop-tutorial
### Dependencies
	You'll need atom.jar (http://atom.univ-lille1.fr). Put it in the root of the project.
### Compile/Build project
    mvn clean compile package
### Generate Atom files simulation into AtomLogs/*
    ./atom-generate.sh
	-- Update properties.txt to generate more (or less) data
	-- You can look at atom-example.txt for sample file format

	-- Update log4j2.xml to change output directory and rolling file strategy
### If Hadoop has not been setup and started
	$HADOOP_HOME/bin/hdfs namenode -format
	$HADOOP_HOME/sbin/start-dfs.sh
### To check that hadoop is up and running
	Web Interface - http://localhost:50070
	With $> Jps 
	6262 DataNode
	6366 SecondaryNameNode
	6182 NameNode
	3073 
	6802 Jps
### Create working directories on HDFS
    $HADOOP_HOME/bin/hdfs dfs -mkdir /user/
    $HADOOP_HOME/bin/hdfs dfs -mkdir /user/pierre
    $HADOOP_HOME/bin/hdfs dfs -mkdir /user/pierre/input
### Put Atom logs files on HDFS
    $HADOOP_HOME/bin/hdfs dfs -put AtomLogs/* /user/pierre/input/
    -- Look through admin interface created files in HDFS
    -- check allocated bloc size
    -- Now try to find the files in the local file size and compare file size
### Start YARN    
    $HADOOP_HOME/sbin/start-yarn.sh
### To check that YARN is up and running
	Web Interface - http://localhost:8088/cluster
	With $> Jps
	6680 ResourceManager
	6762 NodeManager
### Execute map reduce jobs on cluster TraceCount
    $HADOOP_HOME/bin/hadoop jar target/hadoop-tutorial-1.0-SNAPSHOT.jar TraceCount /user/pierre/input /user/pierre/tracecount
	-- regarder le traitement se dérouler dans la console d'admin
### Display result
    $HADOOP_HOME/bin/hdfs dfs -cat /user/pierre/tracecount/part-r-00000
### Calculate agent position per day
    $HADOOP_HOME/bin/hadoop jar target/hadoop-tutorial-1.0-SNAPSHOT.jar AgentPosition /user/pierre/input/ /user/pierre/agentposition
### Display agent position per day
    $HADOOP_HOME/bin/hdfs dfs -cat /user/pierre/agentposition/part-r-00000
### Exercice #1
	Implement a  MapReduce function which calculate the volume of order for each order book
### Calculate agent position per day (Join example)
    $HADOOP_HOME/bin/hadoop jar target/hadoop-tutorial-1.0-SNAPSHOT.jar LeftOrder /user/pierre/input/ /user/pierre/leftorder
	$HADOOP_HOME/bin/hdfs dfs -cat /user/pierre/leftorder/part-r-00000
### Exercice #2
	Implement a  MapReduce function which calculate the volume of executed order for each order book
### Clean data
    $HADOOP_HOME/bin/hdfs dfs -rm -r /user/pierre/agentposition
    $HADOOP_HOME/bin/hdfs dfs -rm -r /user/pierre/tracecount
    $HADOOP_HOME/bin/hdfs dfs -rm -r /user/pierre/leftorder
### Stop YARN and HDFS
     $HADOOP_HOME/sbin/stop-yarn.sh
     $HADOOP_HOME/sbin/stop-dfs.sh