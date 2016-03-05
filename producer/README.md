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
####Build

```shell
# checkout kafka-examples repo
$ cd kafka-examples/producer/
$ mvn clean package
```

####Basic Producer Example
This example is to demonstrate kafka producer functionality.

Main Classes: SimpleProducer.java, BasicProducerExample.java, BasicConsumerExample.java, MyEvent.java                       

 **bin/runProducer.sh** script can be used produce string messages (or) test events (MyEvent.java). 
 
 To generate 100 string messages

 ```shell
 # ./bin/runProducer.sh --bootstrap.servers localhost:9092 --topic my-topic  --messages 100 --delay 1000 
 ```

 To generate 100 MyEvent messages

```shell
# ./bin/runProducer.sh --bootstrap.servers localhost:9092 --topic my-event-topic  --messages 100 --delay 1000 --messagetype myevent
```

Then run the Kafka console consumer script (or) **bin/runConsumer.sh** script to read the published messages.

```shell
#. /bin/runConsumer.sh --bootstrap.servers localhost:9092 --topic my-topic
# ./bin/runConsumer.sh --bootstrap.servers localhost:9092 --topic my-event-topic
```

####Custom Serialization Examples
Kafka Producer and Consumers allows applications to pass Custom Serializer and Deserializer implementations.
key.serializer, value.serializer config properties are used to pass custom serializer for Producer.
key.deserializer, value.deserializer config properties are used to pass custom deserializer for Consumer.

##### Kryo Serializer
This examples shows serailization using [Kryo Serialization Framework](https://github.com/EsotericSoftware/kryo)

Main Classes: KryoSerializer.java, KryoDeserializer.java, KryoUtils.java, KryoProducerExample.java, KryoConsumerExample.java

 **bin/runKryoProducer.sh** script can be used produce string messages (or) test events (MyEvent.java). 
 
 To generate 100 string messages

 ```shell
 # ./bin/runKryoProducer.sh --bootstrap.servers localhost:9092 --topic my-topic  --messages 100 --delay 1000 
 ```

 To generate 100 MyEvent messages

```shell
# ./bin/runKryoProducer.sh --bootstrap.servers localhost:9092 --topic my-event-topic  --messages 100 --delay 1000 --messagetype myevent
```

Then run **bin/runKryoConsumer.sh** script to print the published messages.

```shell
#. /bin/runConsumer.sh --bootstrap.servers localhost:9092 --topic my-topic
# ./bin/runConsumer.sh --bootstrap.servers localhost:9092 --topic my-event-topic
```




