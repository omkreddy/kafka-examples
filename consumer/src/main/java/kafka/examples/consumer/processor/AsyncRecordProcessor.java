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
package kafka.examples.consumer.processor;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncRecordProcessor<K, V> implements RecordProcessor<K, V> {

	private final Logger logger = LoggerFactory.getLogger(AsyncRecordProcessor.class);
	private String clientId;
	
	private final Map<TopicPartition, Long> partitionToUncommittedOffsetMap = new HashMap<TopicPartition, Long>();
	private OffsetCommitCallback callback;
	
	public AsyncRecordProcessor(String clientId) {
		this.clientId = clientId;
		this.callback = new DummyOffsetCommitCallback(clientId);
	}
	
	@Override
	public boolean process(KafkaConsumer<K, V> consumer, ConsumerRecords<K, V> records) throws InterruptedException {

		long lastCommitTimeInMs = System.currentTimeMillis();
		// process records
		for (ConsumerRecord<K, V> record : records) {
			
			TopicPartition tp = new TopicPartition(record.topic(), record.partition());
			logger.info("C : {}, Record received partition : {}, key : {}, value : {}, offset : {}", 
					clientId, tp, record.key(), record.value(), record.offset());
			
			partitionToUncommittedOffsetMap.put(tp, record.offset());
			Thread.sleep(100);
			
			// commit offset of processed messages after specified interval
			if((System.currentTimeMillis() - lastCommitTimeInMs) > 1000) {
				commit(consumer);
				lastCommitTimeInMs = System.currentTimeMillis();
			}
		}
		commit(consumer); // [OR] consumer.commitAsync(callback);
		return true;
	}
	
	@Override
	public boolean commit(KafkaConsumer<K, V> consumer) {
		commitOffsets(consumer);
		return true;
	}

	private void commitOffsets(KafkaConsumer<K, V> consumer) {
		
		if(consumer != null && !partitionToUncommittedOffsetMap.isEmpty()) {
			
			Map<TopicPartition, OffsetAndMetadata> partitionToMetadataMap = new HashMap<>();
			for (Entry<TopicPartition, Long> e : partitionToUncommittedOffsetMap.entrySet()) {
				partitionToMetadataMap.put(e.getKey(), new OffsetAndMetadata(e.getValue() + 1));
			}
			
			logger.info("C : {}, committing the offsets : {}", clientId, partitionToMetadataMap);
			consumer.commitAsync(partitionToMetadataMap, callback);
			partitionToUncommittedOffsetMap.clear();
		}
	}
	
	private class DummyOffsetCommitCallback implements OffsetCommitCallback {

		private final String clientId;
		
		public DummyOffsetCommitCallback(String clientId) {
			this.clientId = clientId;
		}
		
		@Override
		public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {

			if(exception != null) {
				logger.error("C : {}, Error while committing offsets : {}", clientId, offsets);
			} else {
				logger.debug("C : {}, committed offsets : {} successfully", clientId, offsets);
			}
		}
	}

}


/**
 * $Log$
 *  
 */
