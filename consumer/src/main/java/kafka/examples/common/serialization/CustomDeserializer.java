/** CustomDeserializer.java -
* @version      $Name$
* @module       kafka.examples.common.serialization
* 
* @purpose
* @see
*
* @author   Kamal (kamal@nmsworks.co.in)
*
* @created  Mar 10, 2016
* $Id$
*
* @bugs
*
* Copyright 2014-2015 NMSWorks Software Pvt Ltd. All rights reserved.
* NMSWorks PROPRIETARY/CONFIDENTIAL. Use is subject to licence terms.
*/ 

package kafka.examples.common.serialization;

import java.io.Serializable;
import java.util.Map;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;

public class CustomDeserializer<T extends Serializable> implements Deserializer<T> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@SuppressWarnings("unchecked")
	@Override
	public T deserialize(String topic, byte[] objectData) {
		return (objectData == null) ? null : (T) SerializationUtils.deserialize(objectData);
	}

	@Override
	public void close() {
	}

}


/**
 * $Log$
 *  
 */
