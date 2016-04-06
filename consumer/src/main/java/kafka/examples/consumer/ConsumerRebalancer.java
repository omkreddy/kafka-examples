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
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.examples.common.serialization.CustomDeserializer;
import kafka.examples.consumer.processor.AsyncRecordProcessor;
import kafka.examples.consumer.processor.RecordProcessor;
import kafka.examples.consumer.processor.SyncRecordProcessor;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ConsumerRebalancer<K extends Serializable, V extends Serializable> implements Runnable {

	private final Logger logger = LoggerFactory.getLogger(ConsumerRebalancer.class);
	
	private String clientId;
	private KafkaConsumer<K, V> consumer;
	private RecordProcessor<K, V> processor;
	
	private List<String> topics;
	private AtomicBoolean closed = new AtomicBoolean();
	private CountDownLatch shutdownLatch = new CountDownLatch(1);
	
	/**
	 * Constructor
	 * @param configs consumer configurations
	 * @param topics  Subscription topics
	 */
	public ConsumerRebalancer(Properties configs, List<String> topics) {
		this.topics = topics;
		this.clientId = configs.getProperty(ConsumerConfig.CLIENT_ID_CONFIG);
		this.consumer = new KafkaConsumer<>(configs);
	}
	
	/**
	 * Sets the processor to process the records received
	 * from the Kafka. The processor should take care of 
	 * committing the offsets periodically.
	 * 
	 * @param processor Processor to process the records
	 */
	public void setProcessor(RecordProcessor<K, V> processor) {
		this.processor = processor;
	}
	
	/**
	 * Fetches data for the partitions assigned to it by the coordinator
	 */
	@Override
	public void run() {
		
		Preconditions.checkArgument(processor != null, "Processor cannot be null");

		logger.info("Starting the consumer : {}", clientId);
		
		ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				logger.info("C : {}, Revoked partitionns : {}", clientId, partitions);
				processor.commit(consumer);
			}
			
			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				for (TopicPartition tp : partitions) {
					OffsetAndMetadata offsetAndMetaData = consumer.committed(tp);
					long startOffset = offsetAndMetaData != null ? offsetAndMetaData.offset() : -1L;
					logger.info("C : {}, Assigned topicPartion : {} offset : {}", clientId, tp, startOffset);

					if(startOffset >= 0)
						consumer.seek(tp, startOffset);
				}
			}
		};
		consumer.subscribe(topics, listener);

		logger.info("C : {}, Started to process records", clientId);
		while(!closed.get()) {
			ConsumerRecords<K, V> records = consumer.poll(1000);
			
			if(records.isEmpty()) {
				logger.info("C : {}, Found no records", clientId);
				continue;
			}

			logger.info("C : {} Total No. of records received : {}", clientId, records.count());
			try {
				processor.process(consumer, records);
			} catch (InterruptedException e) {
				logger.error("Error", e);
			}
		}
		consumer.close();
		shutdownLatch.countDown();
		logger.info("C : {}, consumer exited", clientId);
	}
	
	/**
	 * Closes the consumer, waiting indefinitely for any needed cleanup.
	 */
	public void close() {
		try {
			closed.set(true);
			shutdownLatch.await();
		} catch (InterruptedException e) {
			logger.error("Error", e);
		}
	}
	
	public static void main(String[] args) {
		
		ArgumentParser parser = argParser();
		
		try {
			Namespace result = parser.parseArgs(args);
			Properties configs = getConsumerConfigs(result);
			List<String> topics = Arrays.asList(result.getString("topics").split(","));
			RecordProcessor<Serializable, Serializable> processor = getRecordProcessor(result);
			
			final ConsumerRebalancer<Serializable, Serializable> consumer = new ConsumerRebalancer<>(configs, topics);
			consumer.setProcessor(processor);
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
	
	private static RecordProcessor<Serializable, Serializable> getRecordProcessor(Namespace result) {
		
		String clientId = result.getString("clientId");
		String processorType = result.getString("processor");
		RecordProcessor<Serializable, Serializable> processor = null;
		if("sync".equals(processorType)) 
			processor = new SyncRecordProcessor<>(clientId);
		else
			processor = new AsyncRecordProcessor<>(clientId);
		return processor;
	}

	/**
	 * Configurations required to instantiate the consumer
	 * @param result User given arguments
	 * @return Consumer configurations
	 */
	private static Properties getConsumerConfigs(Namespace result) {
		Properties configs = new Properties();
		configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, result.getString("bootstrap.servers"));
		configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, result.getString("auto.offset.reset"));
		configs.put(ConsumerConfig.GROUP_ID_CONFIG, result.getString("groupId"));
		configs.put(ConsumerConfig.CLIENT_ID_CONFIG, result.getString("clientId"));
		configs.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, result.getString("max.partition.fetch.bytes"));
		
		configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, CustomDeserializer.class.getName());
		configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomDeserializer.class.getName());
		return configs;
	}
	
	/**
     * Get the command-line argument parser.
     */
    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("consumer-rebalancer")
                .defaultHelp(true)
                .description("This example demonstrates kafka consumer auto-rebalance capabilities");

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
        		.help("Group identifier");
        
        parser.addArgument("--clientId").action(store())
				.required(true)
				.type(String.class)
				.help("Client identifier");
        
        parser.addArgument("--processor").action(store())
        		.required(true)
        		.type(String.class)
        		.choices("sync", "async")
        		.help("Once records processed, commits the offset synchronously / asynchronously");
        
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
