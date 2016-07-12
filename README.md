# hadoop-tutorial



### Adding Atom 

Manually add atom.jar to the project directory once you've pulled it from github.
You can download it [here](http://atom.univ-lille1.fr/atom.zip) : unzip the archive and move the atom.jar to the project directory.



~~Patch your JDK~~

~~Download the certificat: https://alm.finaxys.com/ALMsite/cacert.alm.finaxys.com.cer~~

~~Add the previous certificat downloaded to your java trust store.~~

```ruby
# sudo keytool -importcert -file cacert.alm.finaxys.com.cer -keystore $JAVA_HOME/jre/lib/security/cacerts -trustcacerts
```



### Compile/Build project
Run the command in the project directory.
You need to have maven and java installed.

    mvn clean compile package


**From this point, you will need to install what you choose to use : Hadoop, Hbase, Avro, Kafka, ...**



### Installations required to run the project

* Hadoop
* Hbase
* Zookeeper



    
### Generate Atom files simulation into AtomLogs/*

    -- Update properties.txt to generate more (or less) data
        -- You can look at atom-example.txt for sample file format
    
    -- Update properties.txt to set your outpu choices
        -- By default, the output is set to false for all 4 possible choices (local file, kafka, hbase, avro) : set to true the outputs you want to enable.
    	
    
    -- Update properties.txt to set the path to the config files for hadoop and hbase
        -- Depending on your OS and your installation, the path to the files can change
        -- Here is the name of the files needed :
            - hadoop.conf.core : the conf file is core-site.xml
            - hadoop.conf.hdfs : the conf file is hdfs-site.xml
            - hbase.conf.hbase : the conf file is hbase-site.xml
    
    -- Update log4j2.xml to change output directory and rolling file strategy
    
    
    Finally, you can run the command :
    ./atom-generate.sh
	
### If Hadoop has not been setup and started

	$HADOOP_HOME/bin/hdfs namenode -format
	$HADOOP_HOME/sbin/start-dfs.sh
	
### To check that hadoop is up and running

	Web Interface - http://localhost:50070
	
	-- With the Jps command in your terminal that should give this sort of output
	6262 DataNode
	6366 SecondaryNameNode
	6182 NameNode
	3073 
	6802 Jps
	
### Configuring Hbase (specifically zookeeper) if connection is refused

Here is the error that might occur : 

```
zookeeper.ClientCnxn: Session 0x0 for server null, unexpected error, closing socket connection and attempting reconnect
java.net.ConnectException: Connection refused
	at sun.nio.ch.SocketChannelImpl.checkConnect(Native Method)
	at sun.nio.ch.SocketChannelImpl.finishConnect(SocketChannelImpl.java:717)
	at org.apache.zookeeper.ClientCnxnSocketNIO.doTransport(ClientCnxnSocketNIO.java:361)
	at org.apache.zookeeper.ClientCnxn$SendThread.run(ClientCnxn.java:1081)
```

To solve this, you need to add a property to your Hbase configuration file.
The file to edit is hbase-site.xml in your installation folder and you need to add this property :

```xml
<property>
    <name>hbase.zookeeper.quorum</name>
    <value>your_hostname.local</value>
</property>
```

Replace "your_hostname" by your real host name (type hostname in a terminal to have it).

More information [here](http://stackoverflow.com/questions/10188889/hbase-connection-refused).

	
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
    
### ALM links

* Sonar - https://alm.finaxys.com:9000/
* Nexus -https://alm.finaxys.com:44313/nexus/index.html#welcome
* Jenkins - https://alm.finaxys.com:44312/job/hadoop-tutorial/
