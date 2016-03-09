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

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Properties;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class BasicPartitionExample {

    public static void main(String[] args) {
        ArgumentParser parser = argParser();

        try {
            Namespace res = parser.parseArgs(args);

            /* parse args */
            String brokerList = res.getString("bootstrap.servers");
            String topic = res.getString("topic");
            Boolean syncSend = res.getBoolean("syncsend");
            long noOfMessages = res.getLong("messages");
            long delay = res.getLong("delay");
            String messageType = res.getString("messagetype");


            Properties producerConfig = new Properties();
            producerConfig.put("bootstrap.servers", brokerList);
            producerConfig.put("client.id", "basic-producer");
            producerConfig.put("acks", "all");
            producerConfig.put("retries", "3");
            producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

            SimpleProducer<byte[], byte[]> producer = new SimpleProducer<>(producerConfig, syncSend);

            for (int i = 0; i < noOfMessages; i++) {

                if(i % 2 == 0)
                    producer.send(topic, 0,  getKey(i), getEvent(messageType, i)); // send  even numbered messages
                else
                    producer.send(topic, 1, getKey(i), getEvent(messageType, i)); // send  odd numbered messages

                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            producer.close();
        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                System.exit(0);
            } else {
                parser.handleError(e);
                System.exit(1);
            }
        }

    }

    private static byte[] getEvent(String messageType, int i) {
        if ("string".equalsIgnoreCase(messageType))
            return serialize("message" + i);
        else
            return serialize(new MyEvent(i, "event" + i, "test", System.currentTimeMillis()));
    }


    private static byte[] getKey(int i) {
        return serialize(new Integer(i));
    }

    public static byte[] serialize(final Object obj) {
        return org.apache.commons.lang3.SerializationUtils.serialize((Serializable) obj);
    }

    /**
     * Get the command-line argument parser.
     */
    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("simple-producer")
                .defaultHelp(true)
                .description("This example is to demonstrate kafka producer capabilities");

        parser.addArgument("--bootstrap.servers").action(store())
                .required(true)
                .type(String.class)
                .metavar("BROKER-LIST")
                .help("comma separated broker list");

        parser.addArgument("--topic").action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .help("produce messages to this topic");

        parser.addArgument("--messages").action(store())
                .required(true)
                .type(Long.class)
                .metavar("NUM-MESSAGE")
                .help("number of messages to produce");

        parser.addArgument("--syncsend").action(store())
                .required(false)
                .type(Boolean.class)
                .setDefault(true)
                .metavar("TRUE/FALSE")
                .help("sync/async send of messages");

        parser.addArgument("--delay").action(store())
                .required(false)
                .setDefault(10l)
                .type(Long.class)
                .metavar("DELAY")
                .help("number of milli seconds delay between messages.");

        parser.addArgument("--messagetype").action(store())
                .required(false)
                .setDefault("string")
                .type(String.class)
                .choices(Arrays.asList("string", "myevent"))
                .metavar("STRING/MYEVENT")
                .help("generate string messages or MyEvent messages");

        return parser;
    }

}