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

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncRecordProcessor<K, V> implements RecordProcessor<K, V> {

	private final Logger logger = LoggerFactory.getLogger(SyncRecordProcessor.class);
	private final String clientId;
	private final Map<TopicPartition, Long> partitionToUncommittedOffsetMap = new HashMap<TopicPartition, Long>();
	
	public SyncRecordProcessor(String clientId) {
		this.clientId = clientId;
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
			
			// commit offset of processed messages
			if((System.currentTimeMillis() - lastCommitTimeInMs) > 1000) {
				commit(consumer);
				lastCommitTimeInMs = System.currentTimeMillis();
			}
		}
		commit(consumer); // [OR] consumer.commitSync();
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
			doCommitSync(consumer, partitionToMetadataMap);
			partitionToUncommittedOffsetMap.clear();
		}
	}
	
	private void doCommitSync(KafkaConsumer<K, V> consumer, Map<TopicPartition, OffsetAndMetadata> offsets) {
		try {
			consumer.commitSync(offsets);
		} catch (WakeupException e) {
			// we're shutting down, but finish the commit first and then
		    // re-throw the exception so that the main loop can exit
			doCommitSync(consumer, offsets);
			throw e;
		} catch (CommitFailedException e) {
			logger.debug("C : {}, commit failed", e);
		}
	}
}


/**
 * $Log$
 *  
 */
