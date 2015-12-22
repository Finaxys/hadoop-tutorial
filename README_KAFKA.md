# Kafka Installation

## Download kafka
    wget http://apache.crihan.fr/dist/kafka/0.8.2.2/kafka_2.10-0.8.2.2.tgz

## extract it
    tar -xvf kafka_2.10-0.8.2.2.tgz
    cd kafka_2.10-0.8.2.2

## start zookeeper and kafka
    bin/zookeeper-server-start.sh config/zookeeper.properties > zookeeper.log &
    bin/kafka-server-start.sh config/server.properties  > kafka.log &

## create a topic
    bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic atomTopic

## send a message
    bin/kafka-console-producer.sh --broker-list localhost:9092 --topic atomTopic

## start a consumer
    bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic atomTopic --from-beginning

# start a multi-broker cluster

## edit server configuration
    cp config/server.properties config/server-n.properties
    vm config/server-1.properties
        - change params :
                config/server-n.properties:
                    broker.id=n
                    port=9092+(n)
                    log.dir=/tmp/kafka-logs-n

## start kafka
    bin/kafka-server-start.sh config/server-1.properties &

## create replicated topic
    bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor n --partitions 1 --topic my-replicated-topic

## describe the new topic
    bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic my-replicated-topic
