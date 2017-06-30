### Apache Kafka Producer
This project includes New Java Kafka producer examples. Read [Javadocs](http://kafka.apache.org/0110/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html) for implementation details. Configuaration details are [here](http://kafka.apache.org/documentation.html#producerconfigs). 

#### Quick Start
Before running the below examples, make sure that Zookeeper and Kafka are running.

```shell
# start zookeeper
$ sh zookeeper-server-start.sh ../config/zookeeper.properties
# start kafka
$ sh kafka-server-start.sh  ../config/server.properties
```
#### Build Examples

```shell
# checkout kafka-examples repo
$ cd kafka-examples/producer/
$ mvn clean package
```

#### Basic Producer Example
This example is to demonstrate kafka producer functionality.

**Example Classes:** SimpleProducer.java, BasicProducerExample.java, BasicConsumerExample.java, MyEvent.java.

Use **bin/runProducer.sh** script to produce string messages (or) test event objects (MyEvent.java). 
 
To produce 100 string messages

 ```shell
 # ./bin/runProducer.sh --bootstrap.servers localhost:9092 --topic my-topic  --messages 100 --delay 1000 
 ```

To produce 100 MyEvent messages

```shell
# ./bin/runProducer.sh --bootstrap.servers localhost:9092 --topic my-event-topic  --messages 100 --delay 1000 --messagetype myevent
```

Then run the Kafka console consumer script (or) **bin/runConsumer.sh** script to read the published messages.

```shell
#. /bin/runConsumer.sh --bootstrap.servers localhost:9092 --topic my-topic
# ./bin/runConsumer.sh --bootstrap.servers localhost:9092 --topic my-event-topic
```

####  Transactional Producer
 
To produce 100 MyEvent messages

```shell
# ./bin/runTransactionalProducer.sh --bootstrap.servers localhost:9092 --topic my-event-topic  --messages 100 --delay 1000 --messagetype myevent
```


#### Custom Serialization Examples
Kafka Producer and Consumers allows applications to pass Custom Serializer and Deserializer implementations.
**key.serializer**, **value.serializer** config properties are used to pass custom serializer for Producer.
**key.deserializer**, **value.deserializer** config properties are used to pass custom deserializer for Consumer.

##### Kryo Serializer
This examples shows serailization using [Kryo Serialization Framework](https://github.com/EsotericSoftware/kryo)

**Example Classes:** KryoSerializer.java, KryoDeserializer.java, KryoUtils.java, KryoProducerExample.java, KryoConsumerExample.java 

Use **bin/runKryoProducer.sh** script to produce string messages (or) test event objects (MyEvent.java). 
 
 To produce 100 string messages

 ```shell
 # ./bin/runKryoProducer.sh --bootstrap.servers localhost:9092 --topic my-topic  --messages 100 --delay 1000 
 ```

To produce 100 MyEvent messages

```shell
# ./bin/runKryoProducer.sh --bootstrap.servers localhost:9092 --topic my-event-topic  --messages 100 --delay 1000 --messagetype myevent
```

Then run **bin/runKryoConsumer.sh** script to print the published messages.

```shell
#. /bin/runKryoConsumer.sh --bootstrap.servers localhost:9092 --topic my-topic
# ./bin/runKryoConsumer.sh --bootstrap.servers localhost:9092 --topic my-event-topic
```
#### Custom Partitioning: 

By default kafka producer uses DefaultPartitioner to distribute the records to available partitions.
If a key is present, a partition will be chosen using a hash of the key, else a partition will be
assigned in a round-robin fashion. But, many times applications want to distribute the records
based on their partition design. Available options for custom partitioning are

**Option 1**: Pass partition number, while producing the records, using [ProducerRecord](https://kafka.apache.org/090/javadoc/org/apache/kafka/clients/producer/ProducerRecord.html) class.

**Example Classes:** BasicPartitionExample.java, SimpleProducer.java

This examples distributes the even numbered records to Partition 0 and odd numbered records to Partition 1.

create a topic with two partitions

 ```shell
# cd kafkaHome/bin
#sh kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1  --partitions 1 --topic test-partition
 ```

To run producer with basic partitioning

 ```shell
#./bin/runBasicPartitioner.sh --bootstrap.servers localhost:9092 --topic test-partition --messages 100 --delay 1000 
 ```

Then run the Kafka console consumer script (or) **bin/runConsumer.sh** script to read the published messages.

```shell
#. /bin/runConsumer.sh --bootstrap.servers localhost:9092 --topic test-partition
```

**Option 2**: Use "**partitioner.class**" config property to pass custom Partitioner class that implements the [Partitioner](https://kafka.apache.org/090/javadoc/org/apache/kafka/clients/producer/Partitioner.html) interface.

**Example Classes:** CustomPartitioner.java, CustomPartitionerExample.java, SimpleProducer.java

This examples uses CustomPartitioner to distribute the even numbered records to Partition 0 and odd numbered records to Partition 1.

To run producer with custom partitioner

```shell
#./bin/runCustomPartitioner.sh --bootstrap.servers localhost:9092 --topic test-partition --messages 100 --delay 1000 
```

Then run the Kafka console consumer script (or) **bin/runConsumer.sh** script to read the published messages.

```shell
#. /bin/runConsumer.sh --bootstrap.servers localhost:9092 --topic test-partition
```

#### Error Handling and Reliability Guarantees

Check these slide decks for reliable producer settings: [here] (http://www.slideshare.net/jhols1/apache-kafka-reliability-guarantees-stratahadoop-nyc-2015?qid=2f75758d-01c5-4a02-9400-a7d93929bfa9&v=&b=&from_search=2) and  [here](http://www.slideshare.net/JiangjieQin/no-data-loss-pipeline-with-apache-kafka-49753844)


#### Troubleshooting
Change logger level to enable debug logs : producer/src/main/resources/log4j.properties



