/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.examples.producer;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;
import java.util.concurrent.Future;

public class SimpleProducer<K extends Serializable, V extends Serializable> {

    private final Logger logger = LoggerFactory.getLogger(SimpleProducer.class);

    private KafkaProducer<byte[], byte[]> producer;
    private boolean syncSend;
    private volatile boolean shutDown = false;


    public SimpleProducer(Properties producerConfig) {
        this(producerConfig, true);
    }

    public SimpleProducer(Properties producerConfig, boolean syncSend) {
        this.syncSend = syncSend;
        this.producer = getProducer(producerConfig);
        logger.info("Started Producer.  sync  : {}", syncSend);
    }

    private KafkaProducer<byte[], byte[]> getProducer(Properties producerConfig) {
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        return new KafkaProducer<>(producerConfig);
    }

    public void send(String topic, V v) {
        send(topic, null, v, new DummyCallback());
    }

    public void send(String topic, K k, V v) {
        send(topic, k, v, new DummyCallback());
    }

    public void send(String topic, K k, V v, Callback callback) {
        if (shutDown) {
            throw new RuntimeException("Producer is closed.");
        }

        byte[] key, value;

        try {
            key = serialize(k);
        } catch (IOException e) {
            logger.error("Error while serializing key", e);
            return;
        }

        try {
            value = serialize(v);
        } catch (IOException e) {
            logger.error("Error while serializing value", e);
            return;
        }


        try {
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, key, value), callback);
            if (!syncSend) return;
            future.get();
        } catch (Exception e) {
            logger.error("Error while producing event for topic : {}. Ignoring event = {}", topic, v, e);
        }

    }

    private byte[] serialize(Serializable object) throws IOException {
        if (object == null)
            return null;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeObject(object);
            return bos.toByteArray();
        } catch (Exception e) {
            logger.error("Error while serializing object", e);
        }
        return null;
    }

    public void close() {
        shutDown = true;
        try {
            producer.flush();
            producer.close();
        } catch (Exception e) {
            logger.error("Exception occurred while stopping the producer", e);
        }
    }

    private class DummyCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                logger.error("Error while producing message to topic : {}", recordMetadata.topic(), e);
            } else
                logger.debug("sent message to topic:{} partition:{}  offset:{}", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
        }
    }
}