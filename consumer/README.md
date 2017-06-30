### Apache Kafka Consumer
This project includes Java Kafka consumer examples.

Read [Javadocs](http://kafka.apache.org/0110/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html) for implementation details. Configuaration details are [here](https://kafka.apache.org/documentation.html#newconsumerconfigs). 

#### Quick Start
Before running the below examples, make sure that Zookeeper and Kafka are running.

```shell
# start zookeeper
$ sh zookeeper-server-start.sh ../config/zookeeper.properties

# start kafka
$ sh kafka-server-start.sh  ../config/server.properties
```
#### Build

```shell
# checkout kafka-examples repo
$ cd kafka-examples/consumer/
$ mvn clean package
```
#### Basic Consumer Example
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
#### Manual Partition Assignment
This example demonstrates the usage of Simple Consumer where user has to explicitly
provide a list of TopicPartition. Manually specify the partitions that are assigned 
to it through assign(List), which disables this dynamic partition assignment.

Consumer reads data only from the provided list of {@link TopicPartition}. On failure, 
it does nothing. User has to take care of Fault-tolerance and committing the offset 
periodically

**Example Classes:**  SimpleConsumer.java

Run below script to read the published messages from given partitions.

```shell
# sh bin/runSimpleConsumer.sh  --bootstrap.servers localhost:9092 --topic.partitions TEST-TOPIC:0 --clientId client1 //consume from Partition 0
```
#### Advanced Consumer

The new consumer API is centered around the poll() method which is used to
retrieve records from the brokers. This poll loop is used to collect, process
messages and to commit offsets. Poll loop is also used for heart-beat mechanism. Some times high message
procesing delays may create unwanted consumer rebalances. Some of these problems are discussed [here.]
(http://users.kafka.apache.narkive.com/4vvhuBZO/low-latency-high-message-size-variance)

This example demonstrates sample implementation with ExecutorService for processing and Pause/Resume API.

AdvancedConsumer synchronously commits the offset after processing the messages.
It's fault-tolerant, it manages to consume messages as long as one consumer in the group 
is alive.


**Example Classes:**  AdvancedConsumer.java

Run below script to run the AdvancedConsumer example.

```shell
sh bin/runAdvancedConsumer.sh --bootstrap.servers localhost:9092 --topics TEST-TOPIC  --numConsumer 3 --groupId group1
```
