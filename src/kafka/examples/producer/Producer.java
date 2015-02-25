package kafka.examples.producer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer {

  public static void main(String[] args) throws Exception {

    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "my-producer");

    int i = 0;
    
    KafkaProducer<String, String> producer = new KafkaProducer<>(props);

     while(i<10) {
      producer.send(new ProducerRecord<String, String>("MY-TOPIC", "value-"+i));
      i++;
     } 
    
    producer.close();
  }
}
