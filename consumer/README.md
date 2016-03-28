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
####Basic Consumer Example
This example demonstrates the usage of basic consumer poll loop and
consumer coordination in a group

**Example Classes:**  BasicConsumeLoop.java

Run multiple BasicConsumeLoop by providing same group id and different client id 
so that all the consumer instances belongs to a same group and consumption load gets 
re-balanced across the consumer instances.

create a topic with two or more partitions

 ```shell
# cd kafkaHome/bin
#sh kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1  --partitions 4 --topic TEST-TOPIC
 ```

Then run kafka console-producer script or examples from producer project to produce some messages to newly created topic.

Then run the BasicConsumeLoop example to read the published messages.

```shell
# sh bin/runBasicConsumeLoop.sh --bootstrap.servers localhost:9092 --topics TEST-TOPIC --groupId group1 --clientId client1 // consumer1
# sh bin/runBasicConsumeLoop.sh --bootstrap.servers localhost:9092 --topics TEST-TOPIC --groupId group1 --clientId client2 // consumer2
```

We can list all the consumer groups using kafka-consumer-groups.sh

```shell
# cd kafkaHome/bin
# sh kafka-consumer-groups.sh  --bootstrap-server localhost:9092 --list  --new-consumer
```
