/** CustomSerializer.java -
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
import org.apache.kafka.common.serialization.Serializer;

public class CustomSerializer<T extends Serializable> implements Serializer<T> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public byte[] serialize(String topic, T data) {
		return SerializationUtils.serialize(data);
	}

	@Override
	public void close() {
	}

}


/**
 * $Log$
 *  
 */
