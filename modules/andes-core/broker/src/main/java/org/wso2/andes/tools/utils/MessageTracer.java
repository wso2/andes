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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesChannel;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;

/**
 * Purpose of this class is to log message activities
 */
public class MessageTracer {

    private static Log log = LogFactory.getLog(MessageTracer.class);

    public static final String REACHED_ANDES_CORE = "reached andes core";
    public static final String PUBLISHED_TO_INBOUND_DISRUPTOR = "messaging event submitted to inbound disruptor";
    public static final String ENQUEUE_EVENT_PUBLISHED_TO_INBOUND_DISRUPTOR = "enqueue event submitted to inbound "
                                                                              + "disruptor";
    public static final String MESSAGE_ID_MAPPED = "mapped to andes message";
    public static final String MESSAGE_CLONED = "cloned with message id";
    public static final String TRANSACTION_COMMIT_EVENT_PUBLISHED_TO_INBOUND_DISRUPTOR = "transaction commit event "
                                                                                         + "submitted to inbound "
                                                                                         + "disruptor";
    public static final String TRANSACTION_ROLLBACK_EVENT_PUBLISHED_TO_INBOUND_DISRUPTOR = "transaction rollback "
                                                                                           + "event submitted to inbound "
                                                                                           + "disruptor";
    public static final String TRANSACTION_CLOSE_EVENT_PUBLISHED_TO_INBOUND_DISRUPTOR = "transaction close "
                                                                                           + "event submitted to inbound "
                                                                                           + "disruptor";
    public static final String CONTENT_WRITTEN_TO_DB = "content written to database";
    public static final String SLOT_INFO_UPDATED = "slot information updated";
    public static final String PUBLISHED_TO_OUTBOUND_DISRUPTOR = "submitted to outbound disruptor";
    public static final String METADATA_READ_FROM_DB = "metadata read from database";
    public static final String METADATA_BUFFERED_FOR_DELIVERY = "metadata buffered for delivery";
    public static final String CONTENT_READ = "content read from database";
    public static final String DISPATCHED_TO_PROTOCOL ="dispatched to protocol level for delivery";
    public static final String MESSAGE_REJECTED = "message rejected";
    public static final String MESSAGE_REQUEUED_SUBSCRIBER = "message re-queued to subscriber";
    public static final String MOVED_TO_DLC = "message moved to DLC";
    public static final String MESSAGE_DELETED = "message deleted";
    public static final String ACK_RECEIVED_FROM_PROTOCOL = "ACK received from protocol";
    public static final String ACK_PUBLISHED_TO_DISRUPTOR = "ACK event submitted to disruptor";
    public static final String ACK_MESSAGE_REFERENCE_SET_BY_DISRUPTOR = "ACK metadata reference set by disruptor";
    public static final String MESSAGE_REQUEUED_BUFFER = "message re-queued to buffer";
    public static final String CONTENT_DECOMPRESSION_HANDLER_MESSAGE = "Content decompressed";
    public static final String MESSAGE_BEYOND_LAST_ROLLBACK = "Message is beyond the last rollback point. Therefore " +
            "deliveryCount is not increased.";
    public static final String DISCARD_STALE_MESSAGE = "discarding delivery as message is stale";
    public static final String EXPIRED_MESSAGE_DETECTED_AND_QUEUED = "expired message detected in the flusher and "
            + "queued for batch delete";
    public static final String EXPIRED_MESSAGE_DETECTED_FROM_DATABASE = "expired messages detected from "
            + "the database";
    public static final String EXPIRED_MESSAGE_DETECTED_FROM_DLC = "expired message detected from"
            + "the DLC";

    /**
     * This method will print debug logs for message activities. This will accept parameters for
     * message id, destination name and activity message
     * @param messageId Andes message id
     * @param destination destination name
     * @param description message activity
     */
    public static void trace(long messageId, String destination, String description) {
        if (log.isTraceEnabled()) {
	        StringBuilder messageContent = new StringBuilder();
	        messageContent.append("Message { Destination: ");
	        messageContent.append(destination);
            if (messageId > 0) { // Check if andes message id is assigned, else ignore
                messageContent.append(" , Id: ");
	            messageContent.append(messageId);
            }
            messageContent.append(" } ");
	        messageContent.append(description);
            log.trace(messageContent.toString());
        }
    }

    /**
     * Trace log on message activity. Accepts message Id and a description
     *
     * @param messageId Id of the message
     * @param description Description to place in log on message activity
     */
    public static void trace(long messageId, String description) {
        if (log.isTraceEnabled()) {
            StringBuilder messageContent = new StringBuilder();
            if (messageId > 0) { // Check if andes message id is assigned, else ignore
                messageContent.append("Id: ");
                messageContent.append(messageId);
                messageContent.append(", ");
            }
            messageContent.append(description);
            log.trace(messageContent.toString());
        }
    }

    /**
	 * This method will print debug logs for message activities. This will accept andes message as
	 * a parameter
	 * @param message Andes message
	 * @param description Message activity
	 */
    public static void trace(AndesMessage message, String description) {
        trace(message.getMetadata().getMessageID(), message.getMetadata().getDestination(), description);
    }

	/**
	 * This method will print debug logs for message activities. This will accept metadata as
	 * a parameter
	 * @param metadata andes metadata object
	 * @param description message activity
	 */
    public static void trace(AndesMessageMetadata metadata, String description) {
        trace(metadata.getMessageID(), metadata.getDestination(), description);
    }

    /**
	 * This method will check if trace logs are enabled. This method can be used when performing
	 * operations inside trace() method parameters
	 * @return Status of MessageTracer
	 */
    public static boolean isEnabled() {
        return log.isTraceEnabled();
    }

    /**
     * This method will trace a message in a transaction with the given details and a predefined description.
     *
     * @param messageId   the id of the message
     * @param destination the destination for which the message is bound
     * @param channelId   the identifier of the channel from the message was originated. Used to track the transaction
     * @param description     messaging activity
     */
    private static void traceTransaction(long messageId, String destination, String channelId, String description) {
        if (log.isTraceEnabled()) {
            StringBuilder messageContent = new StringBuilder();
            messageContent.append("Message { Destination: ");
            messageContent.append(destination);
            // Check if andes message id is assigned, else ignore
            if (messageId > 0) {
                messageContent.append(" , Id: ");
                messageContent.append(messageId);
            }
            messageContent.append(" , ChannelId: ");
            messageContent.append(channelId);

            messageContent.append(" } ");
            messageContent.append(description);
            log.trace(messageContent.toString());
        }
    }

    /**
     * This method will trace a transaction given the channel for which the transaction belongs to, the
     * number of messages in the transaction and a predefined description of the operation that is being done.
     *
     * @param channel the channel from the message was originated. Used to track the transaction
     * @param size    number of messages in the transaction
     * @param description messaging activity
     */
    public static void traceTransaction(AndesChannel channel, int size, String description) {
        if (log.isTraceEnabled()) {
            String messageContent = "Transaction { "
                                    + " ChannelID: " + channel.getIdentifier()
                                    + " , BatchSize: " + size
                                    + "} " + description;
            log.trace(messageContent);
        }
    }

    /**
     * This method will trace a message in a transaction given the message and the channel from which the
     * message is received and a predefined message.
     *
     * @param message the andes message to trace
     * @param channel the channel from the message was originated. Used to track the transaction
     * @param description messaging activity
     */
    public static void traceTransaction(AndesMessage message, AndesChannel channel, String description) {
        traceTransaction(message.getMetadata().getMessageID(), message.getMetadata().getDestination(),
                channel.getIdentifier(), description);
    }

}