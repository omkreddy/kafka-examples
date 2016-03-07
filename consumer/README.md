###Apache Kafka Consumer
This project includes New Java Kafka consumer examples.

Read [Javadocs](http://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html) for implementation details. Configuaration details are [here](https://kafka.apache.org/documentation.html#newconsumerconfigs). Confluent's documentaion is 
[here](http://docs.confluent.io/2.0.1/clients/consumer.html).

####Quick Start
Before running the below examples, make sure that Zookeeper and Kafka are running.

```shell
# start zookeeper
$ sh zookeeper-server-start.sh ../config/zookeeper.properties

# start kafka
$ sh kafka-server-start.sh  ../config/server.properties
```
####Build

```shell
# checkout kafka-examples repo
$ cd kafka-examples/consumer/
$ mvn clean package
```
