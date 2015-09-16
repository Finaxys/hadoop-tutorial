# hadoop-tutorial
### Create working directories on HDFS
    $HADOOP_HOME/bin/hdfs dfs -mkdir /user/
    $HADOOP_HOME/bin/hdfs dfs -mkdir /user/pierre
    $HADOOP_HOME/bin/hdfs dfs -mkdir /user/pierre/input
### Compile/Build project
    mvn clean compile package
### Generate Atom files simulation into AtomLogs/*
    ./atom-generate.sh
### Put Atom logs files on HDFS
    $HADOOP_HOME/bin/hdfs dfs -put AtomLogs/* /user/pierre/input/
### Execute map reduce jobs on cluster TraceCount
    $HADOOP_HOME/bin/hadoop jar target/hadoop-tutorial-1.0-SNAPSHOT.jar TraceCount /user/pierre/input /user/pierre/tracecount
### Display result
    $HADOOP_HOME/bin/hdfs dfs -cat /user/pierre/tracecount
### Calculate agent position per day
    $HADOOP_HOME/bin/hadoop jar target/hadoop-tutorial-1.0-SNAPSHOT.jar AgentPosition /user/pierre/input/ /user/pierre/agentposition
### Display
    $HADOOP_HOME/bin/hdfs dfs -cat /user/pierre/agentposition
### Clean
    $HADOOP_HOME/bin/hdfs dfs -rm -r /user/pierre/output3