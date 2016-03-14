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
package kafka.examples.consumer;

import static net.sourceforge.argparse4j.impl.Arguments.store;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import kafka.examples.common.serialization.CustomDeserializer;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>This example demonstrates the usage of basic poll loop and 
 * consumer coordination in a group<p>
 * 
 * <p> Run multiple {@link BasicConsumeLoop} by providing same group id and 
 * different client id so that all the consumer instances belongs
 * to a same group and consumption load gets re-balanced across
 * the consumer instances.<p>
 */
public class BasicConsumeLoop<K extends Serializable, V extends Serializable> implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(BasicConsumeLoop.class);

	private final KafkaConsumer<K, V> consumer;
	private final List<String> topics;
	private final String clientId;

	private CountDownLatch shutdownLatch = new CountDownLatch(1);
	
	public BasicConsumeLoop(Properties configs, List<String> topics) {
		this.clientId = configs.getProperty(ConsumerConfig.CLIENT_ID_CONFIG);
		this.topics = topics;
		this.consumer = new KafkaConsumer<>(configs);
	}

	@Override
	public void run() {

		try {
			logger.info("Starting the Consumer : {}", clientId);

			ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
				@Override
				public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
					logger.info("C : {}, Revoked partitionns : {}", clientId, partitions);
				}
				
				@Override
				public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
					logger.info("C : {}, Assigned partitions : {}", clientId, partitions);
				}
			};
			consumer.subscribe(topics, listener);

			logger.info("C : {}, Started to process records", clientId);
			while(true) {
				ConsumerRecords<K, V> records = consumer.poll(Long.MAX_VALUE);
				
				if(records.isEmpty()) {
					logger.info("C : {}, Found no records", clientId);
					continue;
				}
			
				logger.info("C : {} Total No. of records received : {}", clientId, records.count());
				for (ConsumerRecord<K, V> record : records) {
					logger.info("C : {}, Record received partition : {}, key : {}, value : {}, offset : {}",
							clientId, record.partition(), record.key(), record.value(), record.offset());
					sleep(50);
				}
				
				
				// `enable.auto.commit` set to true. coordinator automatically commits the offsets
				// returned on the last poll(long) for all the subscribed list of topics and partitions
			}
		} catch (WakeupException e) {
			// Ignore, we're closing
		} finally {
			consumer.close();
			shutdownLatch.countDown();
			logger.info("C : {}, consumer exited", clientId);
		}
	}
	
	private void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			logger.error("Error", e);
		}
	}
	
	public void close() {
		try {
			consumer.wakeup();
			shutdownLatch.await();
		} catch (InterruptedException e) {
			logger.error("Error", e);
		}
	}

	public static void main(String[] args) throws InterruptedException {

		ArgumentParser parser = argParser();

		try {
			Namespace result = parser.parseArgs(args);
			List<String> topics = Arrays.asList(result.getString("topics").split(","));
			Properties configs = getConsumerConfigs(result);

			final BasicConsumeLoop<Serializable, Serializable> consumer = new BasicConsumeLoop<>(configs, topics);
			consumer.run();
			
			Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
				
				@Override
				public void run() {
					consumer.close();
				}
			}));
			
		} catch (ArgumentParserException e) {
			if(args.length == 0)
				parser.printHelp();
			else 
				parser.handleError(e);
			System.exit(0);
		}
	}

	private static Properties getConsumerConfigs(Namespace result) {
		Properties configs = new Properties();
		configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, result.getString("bootstrap.servers"));
		configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, result.getString("auto.offset.reset"));
		configs.put(ConsumerConfig.GROUP_ID_CONFIG, result.getString("groupId"));
		configs.put(ConsumerConfig.CLIENT_ID_CONFIG, result.getString("clientId"));
		configs.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, result.getString("max.partition.fetch.bytes"));

		configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, CustomDeserializer.class.getName());
		configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomDeserializer.class.getName());
		return configs;
	}

	/**
	 * Get the command-line argument parser.
	 */
	private static ArgumentParser argParser() {
		ArgumentParser parser = ArgumentParsers
				.newArgumentParser("basic-consumer-loop")
				.defaultHelp(true)
				.description("This example demonstrates kafka consumer auto subscription capabilities");

		parser.addArgument("--bootstrap.servers").action(store())
				.required(true)
				.type(String.class)
				.help("comma separated broker list");

		parser.addArgument("--topics").action(store())
				.required(true)
				.type(String.class)
				.help("consume messages from topics. Comma separated list e.g. t1,t2");

		parser.addArgument("--groupId").action(store())
				.required(true)
				.type(String.class)
				.help("Group Identifier");

		parser.addArgument("--clientId").action(store())
				.required(true)
				.type(String.class)
				.help("Client Identifier");

		parser.addArgument("--auto.offset.reset").action(store())
				.required(false)
				.setDefault("earliest")
				.type(String.class)
				.choices("earliest", "latest")
				.help("What to do when there is no initial offset in Kafka");

		parser.addArgument("--max.partition.fetch.bytes").action(store())
				.required(false)
				.setDefault("1024")
				.type(String.class)
				.help("The maximum amount of data per-partition the server will return");

		return parser;
	}

}


/**
 * $Log$
 *  
 */
