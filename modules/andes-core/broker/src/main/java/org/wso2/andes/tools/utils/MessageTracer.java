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
import org.wso2.andes.kernel.AndesAckData;
import org.wso2.andes.kernel.AndesChannel;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.DeliverableAndesMetadata;
import org.wso2.andes.kernel.dtx.AndesPreparedMessageMetadata;
import org.wso2.andes.kernel.dtx.DtxBranch;

import javax.transaction.xa.Xid;
import java.util.UUID;
import org.wso2.andes.kernel.slot.Slot;

/**
 * Purpose of this class is to log message activities
 */
public class MessageTracer {

    private static Log log = LogFactory.getLog(MessageTracer.class);
    public static final String REACHED_ANDES_CORE = "reached Andes core";
    public static final String ENQUEUED_DTX_MESSAGE = "dtx message enqueued";
    public static final String PUBLISHING_DTX_PREPARE_ENQUEUE_RECORDS_TO_DISRUPTOR =
            "submitting dtx prepare enqueue records to inbound disruptor";
    public static final String PUBLISHING_DTX_PREPARE_DEQUEUE_RECORDS_TO_DISRUPTOR =
            "submitting dtx prepare dequeue records to inbound disruptor";
    public static final String DTX_MESSAGE_WRITTEN_TO_DB = "message written to database on dtx.commit";
    public static final String DTX_ROLLBACK_DEQUEUED_RECORDS = "message restored on dtx.rollback with a new message id";
    public static final String STORED_PREPARED_ENQUEUE_RECORDS_TO_DB = "stored prepared incoming messages to dtx store";
    public static final String MOVE_PREPARED_DEQUEUE_RECORDS_TO_DB =
            "moved acknowledged messages to dtx store from message store";
    public static final String SENDING_MESSAGE_TO_SUBSCRIBER = "sending message to subscriber";
    public static final String DTX_RECORD_METADATA_COUNT_IN_SLOT = "slot information updated for message on dtx.commit";
    public static final String MESSAGE_RECEIVED_TO_AMQ_CHANNEL = " message received to AMQChannel";
    public static final String DTX_MESSAGE_RECEIVED_TO_AMQ_CHANNEL = "dtx message received to AMQChannel";
    public static final String ACK_RECEIVED_TO_AMQ_CHANNEL = "acknowledgment received to AMQChannel";

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
            TraceBuilder traceBuilder = new TraceBuilder(TraceBuilder.MESSAGE_TRACE);
            traceBuilder.setDestination(destination);
            if (messageId > 0) { // Check if andes message id is assigned, else ignore
                traceBuilder.setMessageId(messageId);
            }
            log.trace(traceBuilder.toString(description));
        }
    }

    /**
     * Trace Acknowledgment messages
     *
     * @param ackData {@link AndesAckData}
     * @param description Description to place in log on message activity
     */
    public static void traceAck(AndesAckData ackData, String description) {
        if (log.isTraceEnabled()) {
            TraceBuilder traceBuilder = new TraceBuilder(TraceBuilder.ACKNOWLEDGMENT_TRACE);
            traceBuilder.setMessageId(ackData.getMessageId())
                    .setChannelId(ackData.getChannelId());
            log.trace(traceBuilder.toString(description));
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
            TraceBuilder traceBuilder = new TraceBuilder(TraceBuilder.MESSAGE_TRACE);
            traceBuilder.setDestination(destination);
            // Check if andes message id is assigned, else ignore
            if (messageId > 0) {
                traceBuilder.setMessageId(messageId);
            }
            traceBuilder.setChannelIdentifier(channelId);
            log.trace(traceBuilder.toString(description));
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
            TraceBuilder traceBuilder = new TraceBuilder(TraceBuilder.TX_TRACE);
            traceBuilder.setChannelIdentifier(channel.getIdentifier()).setTxBatchSize(size);
            log.trace(traceBuilder.toString(description));
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

    /**
     * Trace dtx messages coming into the {@link org.wso2.andes.server.AMQChannel}
     *
     * @param xid {{@link Xid}}
     * @param channelId {@link org.wso2.andes.server.AMQChannel} Id
     * @param description description of the trace incident
     */
    public static void trace(Xid xid, UUID channelId, String description) {
        if (log.isTraceEnabled()) {
            TraceBuilder traceBuilder = new TraceBuilder(TraceBuilder.DTX_TRACE);
            traceBuilder.setXid(xid).setChannelId(channelId);
            log.trace(traceBuilder.toString(description));
        }
    }

    /**
     * Trace {@link AndesMessage} for a distributed transaction

     * @param metadata {@link AndesMessageMetadata} relevant to the message
     * @param xid {@link Xid}
     * @param state {@link org.wso2.andes.kernel.dtx.DtxBranch.State} of the {@link DtxBranch}
     * @param description description of the message trace incident
     */
    public static void trace(AndesMessageMetadata metadata, Xid xid, DtxBranch.State state, String description) {
        if (log.isTraceEnabled()) {
            TraceBuilder traceBuilder = new TraceBuilder(TraceBuilder.MESSAGE_TRACE);
            traceBuilder.setMessageId(metadata.getMessageID())
                    .setDestination(metadata.getDestination())
                    .setXid(xid)
                    .setBranchState(state);
            log.trace(traceBuilder.toString(description));
        }
    }

    /**
     * Trace acknowledgment event for a distributed transaction
     *
     * @param ackData {@link AndesAckData}
     * @param xid {@link Xid}
     * @param state {@link org.wso2.andes.kernel.dtx.DtxBranch.State} of the {@link DtxBranch}
     * @param description description of the message trace incident
     */
    public static void trace(AndesAckData ackData, Xid xid, DtxBranch.State state, String description) {
        if (log.isTraceEnabled()) {
            TraceBuilder traceBuilder = new TraceBuilder(TraceBuilder.ACKNOWLEDGMENT_TRACE);
            traceBuilder.setMessageId(ackData.getMessageId())
                    .setBranchState(state)
                    .setChannelId(ackData.getChannelId())
                    .setXid(xid);
            log.trace(traceBuilder.toString(description));
        }
    }

    /**
     * Trace {@link AndesPreparedMessageMetadata} which is used to rollback acknowledged messages
     *
     * @param preparedMessageMetadata {@link AndesPreparedMessageMetadata}
     * @param xid {@link Xid}
     * @param state {@link org.wso2.andes.kernel.dtx.DtxBranch.State} of the {@link DtxBranch}
     * @param description description of the message trace incident
     */
    public static void trace(AndesPreparedMessageMetadata preparedMessageMetadata, Xid xid, DtxBranch.State state,
                             String description) {
        if (log.isTraceEnabled()) {
            TraceBuilder traceBuilder = new TraceBuilder(TraceBuilder.MESSAGE_TRACE);
            traceBuilder.setMessageId(preparedMessageMetadata.getMessageID())
                    .setOldMessageId(preparedMessageMetadata.getOldMessageId())
                    .setXid(xid)
                    .setDestination(preparedMessageMetadata.getDestination())
                    .setBranchState(state);
            log.trace(traceBuilder.toString(description));
        }
    }

    /**
     * General purpose message trace with a simple description
     * @param description description of the message trace incident
     */
    public static void trace(String description) {
        log.trace(description);
    }

    /**
     * Trace message with routing key and channel id
     *
     * @param messageId message id
     * @param routingKey routing key of the message
     * @param channelId {@link UUID} channel id
     * @param description description of the message trace incident
     */
    public static void trace(long messageId, String routingKey, UUID channelId, String description) {
        if (log.isTraceEnabled()) {
            TraceBuilder traceBuilder = new TraceBuilder(TraceBuilder.MESSAGE_TRACE);
            traceBuilder.setMessageId(messageId).setDestination(routingKey).setChannelId(channelId);
            log.trace(traceBuilder.toString(description));
        }
    }

    /**
     * Trace a message without message id.
     * @param destination destination of the message. (Routing key)
     * @param channelId {@link org.wso2.andes.server.AMQChannel} id
     * @param channelIdentifier {@link AndesChannel} identifier
     * @param description description of the message trace incident
     */
    public static void trace(String destination, UUID channelId, String channelIdentifier, String description) {
        if (log.isTraceEnabled()) {
            TraceBuilder traceBuilder = new TraceBuilder(TraceBuilder.MESSAGE_TRACE);
            traceBuilder.setDestination(destination).setChannelId(channelId).setChannelIdentifier(channelIdentifier);
            log.trace(traceBuilder.toString(description));
        }
    }

    /**
     * Trace a message in delivery
     *
     * @param messageId message id
     * @param deliveryTag delivery tag of the message
     * @param description description of the message trace incident
     */
    public static void trace(long messageId, long deliveryTag, String description) {
        if (log.isTraceEnabled()) {
            TraceBuilder traceBuilder = new TraceBuilder(TraceBuilder.MESSAGE_TRACE);
            traceBuilder.setMessageId(messageId).setDeliveryTag(deliveryTag);
            log.trace(traceBuilder.toString(description));
        }
    }

    /**
     * Trace acknowledgment
     *
     * @param destination routing key of the acknowledgment
     * @param channelId Channel id as a {@link UUID}
     * @param channelIdentifier channel identifier. IP and port
     * @param isDtx true if distributed transaction
     * @param deliveryTag delivery tag
     * @param isMultiple Ack multiple messages or not (AMQP specific)
     * @param description description of the message trace incident
     */
    public static void trace(String destination, UUID channelId, String channelIdentifier,
                             boolean isDtx, long deliveryTag, boolean isMultiple, String description) {
        if (log.isTraceEnabled()) {
            TraceBuilder traceBuilder = new TraceBuilder(TraceBuilder.ACKNOWLEDGMENT_TRACE);
            traceBuilder.setDestination(destination)
                    .setDeliveryTag(deliveryTag)
                    .setChannelId(channelId)
                    .setChannelIdentifier(channelIdentifier)
                    .setDeliveryTag(deliveryTag)
                    .setAckMultiple(isMultiple)
                    .setIsDtx(isDtx);
            log.trace(traceBuilder.toString(description));
        }
    }

    /**
     * Trace deliverable message
     * @param metadata {@link DeliverableAndesMetadata}
     * @param slot {@link Slot}
     * @param description description of the message trace incident
     */
    public static void trace(DeliverableAndesMetadata metadata, Slot slot, String description) {
        if (log.isTraceEnabled()) {
            TraceBuilder traceBuilder = new TraceBuilder(TraceBuilder.MESSAGE_TRACE);
            traceBuilder.setDestination(metadata.getDestination())
                    .setMessageId(metadata.getMessageID())
                    .setSlotId(slot.getId());
            log.trace(traceBuilder.toString(description));
        }
    }

    /**
     * Builder to create a standard trace message
     */
    private static class TraceBuilder {

        private static final String TX_BATCH_SIZE = "txBatchSize" ;
        private static final String MESSAGE_ID = "id: ";
        private static final String OLD_MESSAGE_ID = "oldId: ";
        private static final String DESTINATION = "destination: ";
        private static final String DELIVERY_TAG = "deliveryTag: ";
        private static final String CHANNEL_ID = "channelId: ";
        private static final String CHANNEL_IDENTIFIER = "channelIdentifier: ";
        private static final String XID = "xid: ";
        private static final String DTX_BRANCH_STATE = "branchState: ";
        private static final String ACK_MULTIPLE = "ackMultiple: ";
        private static final String IS_DTX = "isDtx: ";
        private static final String SLOT_ID = "slotId: ";

        static final String MESSAGE_TRACE = "Message { ";
        static final String ACKNOWLEDGMENT_TRACE = "Ack { ";
        static final String DTX_TRACE = "Dtx { ";
        static final String TX_TRACE = "Transaction { ";

        private final String traceHeader;

        private StringBuilder messageContent;

        /**
         * Create a trace builder class
         * @param traceHeader type of trace message
         */
        TraceBuilder(String traceHeader) {
            this.messageContent = new StringBuilder();
            this.traceHeader = traceHeader;
        }

        /**
         * Check whether the trace header is added
         * @return true if trace header is already present
         */
        private boolean containTraceHeader() {
            return messageContent.length() != 0;
        }

        /**
         * Add the trace header
         */
        private void addTraceHeader() {
            messageContent.append(traceHeader);
        }

        /**
         * Check if the trace header is added. If not added, add the
         * trace header
         */
        private void checkAndAddTraceHeader() {
            if (containTraceHeader()) {
                messageContent.append(" , ");
            } else {
                addTraceHeader();
            }
        }

        /**
         * Set message id to the trace
         * @param id Message id
         * @return TraceBuilder
         */
        TraceBuilder setMessageId(long id) {
            checkAndAddTraceHeader();
            messageContent.append(MESSAGE_ID);
            messageContent.append(id);
            return this;
        }

        /**
         * Set the previous message id of the message. Needed when restoring message
         * @param oldId Old message id
         * @return TraceBuilder
         */
        TraceBuilder setOldMessageId(long oldId) {
            checkAndAddTraceHeader();
            messageContent.append(OLD_MESSAGE_ID);
            messageContent.append(oldId);
            return this;
        }

        /**
         * Set destination queue or topic of the message
         *
         * @param destination destination (Routing key)
         * @return TraceBuilder
         */
        TraceBuilder setDestination(String destination) {
            checkAndAddTraceHeader();
            messageContent.append(DESTINATION);
            messageContent.append(destination);
            return this;
        }

        /**
         * Set delivery tag of the delivery message or acknowledgement
         * @param deliveryTag delivery tag
         * @return TraceBuilder
         */
        TraceBuilder setDeliveryTag(long deliveryTag) {
            checkAndAddTraceHeader();
            messageContent.append(DELIVERY_TAG);
            messageContent.append(deliveryTag);
            return this;
        }

        /**
         * Set Channel Id
         * @param channelId Channel id as a {@link UUID}
         * @return TraceBuilder
         */
        TraceBuilder setChannelId(UUID channelId) {
            checkAndAddTraceHeader();
            messageContent.append(CHANNEL_ID);
            messageContent.append(channelId);
            return this;
        }

        /**
         * Set channel identifier, which contains IP and port of the channel
         * @param channelIdentifier String containing the IP and port. {@link AndesChannel} identifier
         * @return TraceBuilder
         */
        TraceBuilder setChannelIdentifier(String channelIdentifier) {
            checkAndAddTraceHeader();
            messageContent.append(CHANNEL_IDENTIFIER);
            messageContent.append(channelIdentifier);
            return this;
        }

        /**
         * Set Dtx {@link Xid}
         * @param xid {@link Xid}
         * @return TraceBuilder
         */
        TraceBuilder setXid(Xid xid) {
            checkAndAddTraceHeader();
            messageContent.append(XID);
            messageContent.append(xid);
            return this;
        }

        /**
         * Set the dtx branch state
         * @param state {@link org.wso2.andes.kernel.dtx.DtxBranch.State}
         * @return TraceBuilder
         */
        TraceBuilder setBranchState(DtxBranch.State state) {
            checkAndAddTraceHeader();
            messageContent.append(DTX_BRANCH_STATE);
            messageContent.append(state);
            return this;
        }

        /**
         * Set the boolean of the received acknowledgement mode. Ack multiple messages or not (AMQP specific)
         * @param ackMultiple acknowledge multiple messages
         * @return TraceBuilder
         */
        TraceBuilder setAckMultiple(boolean ackMultiple) {
            checkAndAddTraceHeader();
            messageContent.append(ACK_MULTIPLE);
            messageContent.append(ackMultiple);
            return this;
        }

        /**
         * Set whether the this is specific to dtx or not
         * @param isDtx true if this is related to dtx
         * @return TraceBuilder
         */
        TraceBuilder setIsDtx(boolean isDtx) {
            checkAndAddTraceHeader();
            messageContent.append(IS_DTX);
            messageContent.append(isDtx);
            return this;
        }

        /**
         * set local transaction batch size
         * @param txBatchSize transaction batch size
         * @return TraceBuilder
         */
        TraceBuilder setTxBatchSize(int txBatchSize) {
            checkAndAddTraceHeader();
            messageContent.append(TX_BATCH_SIZE);
            messageContent.append(txBatchSize);
            return this;
        }

        /**
         * Set the slot id
         * @param slotId slot id
         * @return TraceBuilder
         */
        TraceBuilder setSlotId(String slotId) {
            checkAndAddTraceHeader();
            messageContent.append(SLOT_ID);
            messageContent.append(slotId);
            return this;
        }

        public String toString() {
            return messageContent.toString();
        }

        /**
         * Get the {@link String} representation of the trace with the description
         * @param description description of the trace incident
         * @return String
         */
        String toString(String description) {
            messageContent.append(" } ");
            messageContent.append(description);
            return messageContent.toString();
        }
    }
}
