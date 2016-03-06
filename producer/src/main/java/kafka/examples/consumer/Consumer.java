package kafka.examples.consumer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import jersey.repackaged.com.google.common.base.Preconditions;
import jersey.repackaged.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer<K extends Serializable, V extends Serializable> implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

	private final String groupId;
	private final String clientId;
	private String partitionAssignorStrategy;
	
	private List<String> topics;
	private KafkaConsumer<K, V> consumer;
	private AtomicBoolean closed = new AtomicBoolean();
	
	public Consumer(String groupId, String clientId, List<String> topics) {
		
		Preconditions.checkArgument(groupId != null && !groupId.isEmpty(), "GroupId cannot be null / empty");
		Preconditions.checkArgument(clientId != null && !clientId.isEmpty(), "ClientId cannot not be null / empty");
		Preconditions.checkArgument(topics != null && !topics.isEmpty(), "Topics cannot be  null / empty"); 

		this.groupId = groupId;
		this.clientId = clientId;
		this.topics = topics;
	}
	
	public void setPartitionAssignorStrategy(String partitionAssignorClassName) {
		partitionAssignorStrategy = partitionAssignorClassName;
	}
	
	private Properties getConsumerProperties() {

		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 3000);
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

		if(partitionAssignorStrategy != null)
			props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, partitionAssignorStrategy);

		return props;
	}
	
	private void sleep(long sleepMs) {
        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
        	logger.error("Exception while sleeping ", e);
        }
    }
	
	@Override
	public void run() {
	
		logger.info("Starting consumer : {}", clientId);
		Properties configs = getConsumerProperties();
		consumer = new KafkaConsumer<>(configs, new CustomSerDeserializer<K>(), new CustomSerDeserializer<V>());
		
		ExecutorService executor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
					.setNameFormat("Dispatcher-" + clientId).build());
		final AtomicBoolean canConsume = new AtomicBoolean(true);
		
		ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				if(!partitions.isEmpty()) // First time, there won't be any partitions to revoke
					canConsume.compareAndSet(true, false);
				logger.info("C : {}, Revoked topicPartitions : {}", clientId, partitions);
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
		logger.info("Started to process records for consumer : {}", clientId);
		
		while(!closed.get()) {
			
			ConsumerRecords<K, V> records = consumer.poll(1000);
			
			if(records == null || records.isEmpty()) {
				logger.info("C: {}, Found no records, Sleeping for a while", clientId);
				sleep(500);
				continue;
			}
			
			logger.info("C: {}, Number of records received : {}", clientId, records.count());
			
			/**
			 * After receiving the records, pause all the partitions and do heart-beat manually
			 * to avoid the consumer instance gets kicked-out from the group by the consumer coordinator
			 * due to the delay in the processing of messages
			 */
			consumer.pause(consumer.assignment().toArray(new TopicPartition[0]));
			Future<Boolean> future = executor.submit(new ConsumeRecords(records));
			
			Boolean isCompleted = false;
			while(!isCompleted) {
				try	{
					isCompleted = future.get(3, TimeUnit.SECONDS); // wait up-to heart-beat interval
				} catch (TimeoutException e) {
					if(!canConsume.get() || closed.get()) {
						future.cancel(true);
						canConsume.compareAndSet(false, true);
						break;
					}

					logger.info("C : {}, heartbeats the coordinator", clientId);
					consumer.poll(0); // does heart-beat
				} catch (Exception e) {
					logger.error("Error while consuming records", e);
					break;
				}
			}
			consumer.resume(consumer.assignment().toArray(new TopicPartition[0]));
		}
		executor.shutdown();
		consumer.close();
		logger.info("C : {}, consumer exited", clientId);
	}

	private void commitOffsets(Map<TopicPartition, Long> partitionToOffsetMap) {

		if(!partitionToOffsetMap.isEmpty()) {
			Map<TopicPartition, OffsetAndMetadata> partitionToMetadataMap = new HashMap<>();
			
			for(Entry<TopicPartition, Long> e : partitionToOffsetMap.entrySet()) {
				partitionToMetadataMap.put(e.getKey(), new OffsetAndMetadata(e.getValue() + 1));
			}
			
			logger.info("C : {}, committing the offsets : {}", clientId, partitionToMetadataMap);
			consumer.commitSync(partitionToMetadataMap);
			partitionToOffsetMap.clear();
		}
	}

	public void close() {
		closed.compareAndSet(false, true);
	}
	
	private class ConsumeRecords implements Callable<Boolean> {
		
		ConsumerRecords<K, V> records;
		
		public ConsumeRecords(ConsumerRecords<K, V> records) {
			this.records = records;
		}
		
		@Override
		public Boolean call() {

			Map<TopicPartition, Long> partitionToUncommittedOffsetMap = new HashMap<>();
			try {
				for (TopicPartition tp : records.partitions()) {
					for (ConsumerRecord<K, V> record : records.records(tp)) {
						
						logger.info("C : {}, Record received topicPartition : {} offset : {}", clientId, tp, record.offset());
						partitionToUncommittedOffsetMap.put(tp, record.offset());
						Thread.sleep(1000); // Adds more processing time for a record
					}
					commitOffsets(partitionToUncommittedOffsetMap); // Commits offset once a partition data is consumed
				}
			} catch (InterruptedException e) {
				logger.info("C : {}, On revoke committing the offsets : {}", clientId, partitionToUncommittedOffsetMap);
				commitOffsets(partitionToUncommittedOffsetMap);
			}
			return true;
		}
	}
	
	private class CustomSerDeserializer<T extends Serializable> implements Serializer<T>, Deserializer<T> {
		
		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
		}

		@Override
		public T deserialize(String topic, byte[] data) {
			if(data == null)
				return null;
			
			return SerializationUtils.deserialize(data);
		}

		@Override
		public byte[] serialize(String topic, T data) {
			if(data == null)
				return null;
			
			return SerializationUtils.serialize(data);
		}

		@Override
		public void close() {
			
		}
	}
	
	public static void main(String[] args) throws InterruptedException {
		
		List<Consumer<String, Integer>> consumers = new ArrayList<>();
		
		for(int i=0; i<3; i++) {
			String clientId = "Worker" + i;
			Consumer<String, Integer> consumer = new Consumer<>("test-group", clientId, Arrays.asList("test4"));
			consumers.add(consumer);
		}
		
		ExecutorService executor = Executors.newFixedThreadPool(consumers.size());
		
		// New consumer added to the group every one minute
		for (Consumer<String, Integer> consumer : consumers) {
			executor.execute(consumer);
			Thread.sleep(TimeUnit.SECONDS.toMillis(30)); // let the consumer run for half-a-minute
		}
		
		Thread.sleep(TimeUnit.SECONDS.toMillis(60)); // let all the consumers run for few minutes

		// Close the consumer one by one
		for (Consumer<String, Integer> consumer : consumers) {
			consumer.close();
			Thread.sleep(TimeUnit.SECONDS.toMillis(30));
		}
		
		executor.shutdown();
		while(!executor.awaitTermination(5, TimeUnit.SECONDS));
		logger.info("Exiting the application");
	}
}


/**
 * $Log$
 *  
 */
