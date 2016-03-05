###Apache Kafka Producer
This project includes New Java Kafka producer examples.

Read [Javadocs](https://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html) for implementation details. Configuaration details are [here](http://kafka.apache.org/documentation.html#producerconfigs). Confluent's documentaion is 
[here](http://docs.confluent.io/2.0.1/clients/producer.html).

####Quick Start
Before running the below examples, make sure that Zookeeper and Kafka are running.

```shell
# start zookeeper
$ sh zookeeper-server-start.sh ../config/zookeeper.properties

# start kafka
$ sh kafka-server-start.sh  ../config/server.properties
```

####Basic Producer Example
This example is to demonstrate kafka producer functionality.

 bin/runProducer.sh can be used produce string messages (or) test events (MyEvent.java). 
 
 To generate 100 string messages

 ```shell
 # ./bin/runProducer.sh --bootstrap.servers localhost:9092 --topic my-topic  --messages 100 --delay 1000 
 ```

 To generate 100 MyEvent messages

```shell
# ./bin/runProducer.sh --bootstrap.servers localhost:9092 --topic my-event-topic  --messages 100 --delay 1000 --messagetype myevent
```

Then run the Kafka console consumer script (or) bin/runConsumer.sh script to read the published messages.

```shell
#. /bin/runConsumer.sh --bootstrap.servers localhost:9092 --topic my-topic
# ./bin/runConsumer.sh --bootstrap.servers localhost:9092 --topic my-event-topic
```



####Custom Serialization Example
##### Kryo Serializer




