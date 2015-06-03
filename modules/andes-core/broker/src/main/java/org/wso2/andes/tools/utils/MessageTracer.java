/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.tools.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;

/**
 * Purpose of this class is to log message activities
 */
public class MessageTracer {

    private static Logger log = LoggerFactory.getLogger(MessageTracer.class);

    public static final String REACHED_ANDES_CORE = "reached andes core";
    public static final String PUBLISHED_TO_DISRUPTOR = "published to inbound disruptor";
    public static final String MESSAGE_ID_MAPPED = "mapped to andes message";
    public static final String CONTENT_WRITTEN_TO_DB = "content written to database";
    public static final String SLOT_INFO_UPDATED = "slot information updated";
    public static final String METADATA_READ_FROM_DB = "metadata read from database";
    public static final String METADATA_BUFFERED_FOR_DELIVERY = "metadata buffered for delivery";
    public static final String CONTENT_READ = "content read from database";
    public static final String DISPATCHED_TO_PROTOCOL ="dispatched to protocol level for delivery";

	/**
	 * This method will print debug logs for message activities. This will accept andes message as
	 * a parameter
	 * @param message Andesmessage
	 * @param content Message activity
	 */
    public static void trace(AndesMessage message, String content) {
        if(log.isTraceEnabled()) {
            String messageContent = "Message {";
            messageContent += " Destination: " + message.getMetadata().getDestination();
            if (message.getMetadata().getMessageID() > 0) {
                messageContent += " , Id: " + message.getMetadata().getMessageID();
            }
            messageContent += " } ";
            log.trace(messageContent + content);
        }
    }

	/**
	 * This method will print debug logs for message activities. This will accept metadata as
	 * a parameter
	 * @param metadata andes metadata object
	 * @param content message activity
	 */
    public static void trace(AndesMessageMetadata metadata, String content) {
        if(log.isTraceEnabled()) {
            String messageContent = "Message {";
            messageContent += " Destination: " + metadata.getDestination();
            if (metadata.getMessageID() > 0) {
                messageContent += " , Id: " + metadata.getMessageID();
            }
            messageContent += " } ";
            log.trace(messageContent + content);
        }
    }

}