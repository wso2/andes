/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.andes.amqp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.kernel.AndesContent;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.AndesUtils;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.kernel.ProtocolMessage;
import org.wso2.andes.kernel.disruptor.inbound.InboundBindingEvent;
import org.wso2.andes.kernel.disruptor.inbound.InboundExchangeEvent;
import org.wso2.andes.kernel.disruptor.inbound.InboundQueueEvent;
import org.wso2.andes.kernel.disruptor.inbound.QueueInfo;
import org.wso2.andes.server.exchange.DirectExchange;
import org.wso2.andes.server.exchange.Exchange;
import org.wso2.andes.server.exchange.TopicExchange;
import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.server.message.MessageMetaData;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.andes.server.queue.SimpleQueueEntryList;
import org.wso2.andes.server.store.MessageMetaDataType;
import org.wso2.andes.server.store.StorableMessageMetaData;
import org.wso2.andes.server.store.StoredMessage;
import org.wso2.andes.store.StoredAMQPMessage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class defines AMQP related miscellaneous util methods.
 */
public class AMQPUtils {

    public static final String TOPIC_AND_CHILDREN_WILDCARD = "#";

    public static final String IMMEDIATE_CHILDREN_WILDCARD = "*";

    public static String DIRECT_EXCHANGE_NAME = "amq.direct";

    public static String TOPIC_EXCHANGE_NAME = "amq.topic";

    public static String DEFAULT_EXCHANGE_NAME = "<<default>>";

    public static String DLC_EXCHANGE_NAME = "amq.dlc";

    public static String DEFAULT_ANDES_CHANNEL_IDENTIFIER = "AMQP-Unknown";

    /**
     * Max chunk size of the stored content in Andes;
     */
    public static int DEFAULT_CONTENT_CHUNK_SIZE;

    private static Log log = LogFactory.getLog(AMQPUtils.class);

    /**
     *  Store previously queried message part as a cache so when the same
     *  part was required for the next chunk it can be retrieved without
     *  accessing the database
     */
    private static Map<Long, AndesMessagePart> messagePartCache = new HashMap<>();


    /**
     * convert andes metadata to qpid AMQMessage
     *
     * @param metadata andes metadata
     * @return AMQMessage
     */
    public static AMQMessage getAMQMessageFromAndesMetaData(AndesMessageMetadata metadata) {
        long messageId = metadata.getMessageID();
        StorableMessageMetaData metaData = convertAndesMetadataToAMQMetadata(metadata);
        //create message with meta data. This has access to message content
        StoredAMQPMessage message = new StoredAMQPMessage(messageId, metaData);
        AMQMessage amqMessage = new AMQMessage(message);
        return amqMessage;
    }

    /**
     * Create a new AMQMessage out of StoredMessage and content
     * @param storedMessage StoredMessage object
     * @return instance of AMQMessage
     */
    public static AMQMessage getQueueEntryFromStoredMessage(StoredMessage<MessageMetaData> storedMessage,
                                                            AndesContent content) {
        QpidStoredMessage<MessageMetaData> message = new QpidStoredMessage<>(
                storedMessage, content);
        return new AMQMessage(message);
    }

    /**
     * Convert andes metadata to Qpid AMQMessage. Returned message is aware of the disruptor memory cache.
     *
     * @param metadata
     *         Meta object which holds information about the message
     * @param content
     *         Content object which has access to the message content
     * @return AMQMessage
     */
    public static AMQMessage getAMQMessageForDelivery(ProtocolMessage metadata, AndesContent content) {
        long messageId = metadata.getMessageID();
        //create message with meta data. This has access to message content
        StorableMessageMetaData metaData = convertAndesMetadataToAMQMetadata(metadata.getMessage());
        QpidStoredMessage<MessageMetaData> message = new QpidStoredMessage<>(
                new StoredAMQPMessage(messageId, metaData), content);
        AMQMessage amqMessage = new AMQMessage(message);
        amqMessage.setAndesMetadataReference(metadata);
        return amqMessage;
    }

    /**
     * convert a AMQMessage to a queue entry
     *
     * @param message AMQMessage
     * @param queue   qpid queue
     * @return queue entry
     */
    public static QueueEntry convertAMQMessageToQueueEntry(AMQMessage message, AMQQueue queue) {
        SimpleQueueEntryList list = new SimpleQueueEntryList(queue);
        return list.add(message);
    }

    /**
     * convert Andes metadata to StorableMessageMetaData
     *
     * @param andesMessageMetadata andes metadata
     * @return StorableMessageMetaData
     */
    public static StorableMessageMetaData convertAndesMetadataToAMQMetadata(AndesMessageMetadata andesMessageMetadata) {
        byte[] dataAsBytes = andesMessageMetadata.getMetadata();
        ByteBuffer buf = ByteBuffer.wrap(dataAsBytes);
        buf.position(1);
        buf = buf.slice();
        MessageMetaDataType type = MessageMetaDataType.values()[dataAsBytes[0]];
        return type.getFactory().createMetaData(buf);
    }

    /**
     * convert an AMQMessage to andes metadata
     *
     * @param amqMessage qpid amqMessage
     * @return andes message metadata
     * @throws AndesException
     */
    public static AndesMessageMetadata convertAMQMessageToAndesMetadata(AMQMessage amqMessage) {
        MessageMetaData amqMetadata = amqMessage.getMessageMetaData();
        String queue = amqMetadata.getMessagePublishInfo().getRoutingKey().toString();

        final int bodySize = 1 + amqMetadata.getStorableSize();
        byte[] underlying = new byte[bodySize];
        underlying[0] = (byte) amqMetadata.getType().ordinal();
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(underlying);
        buf.position(1);
        buf = buf.slice();
        amqMetadata.writeToBuffer(0, buf);
        AndesMessageMetadata metadata = new AndesMessageMetadata(amqMessage.getMessageId(),underlying,true);
        metadata.setArrivalTime(amqMessage.getArrivalTime());
        metadata.setMessageContentLength(amqMetadata.getContentSize());

        return metadata;
    }

    /**
     * Read message content from message store and fill the input buffer.
     *
     * @param messageId   The message Id
     * @param offsetValue Chunk offset to read
     * @param dst         Buffer to fill bytes of content
     * @return Written byte count
     * @throws AndesException
     */
    public static int fillBufferFromContent(long messageId, int offsetValue, ByteBuffer dst) throws AndesException {
        int written = 0;
        int initialBufferSize = dst.remaining();
        int currentOffsetValue = offsetValue;

        while (initialBufferSize > written) {
            int chunkIndex = currentOffsetValue / DEFAULT_CONTENT_CHUNK_SIZE;
            int indexToQuery = chunkIndex * DEFAULT_CONTENT_CHUNK_SIZE;
            int positionToReadFromChunk = currentOffsetValue - (chunkIndex * DEFAULT_CONTENT_CHUNK_SIZE);

            AndesMessagePart messagePart = resolveCacheAndRetrieveMessagePart(messageId, indexToQuery);

            int messagePartSize = messagePart.getDataLength();
            int remainingSizeOfBuffer = initialBufferSize - dst.position();
            int numOfBytesAvailableToRead = messagePartSize - positionToReadFromChunk;
            int numOfBytesToRead;

            if (remainingSizeOfBuffer > numOfBytesAvailableToRead) {
                numOfBytesToRead = numOfBytesAvailableToRead;

                // The complete message part has been read and no longer needed in the cache
                messagePartCache.remove(messageId);
            } else {
                numOfBytesToRead = initialBufferSize - written;
            }

            // message content can be returned as null if a sudden queue purge occurs and clears all message content in store.
            // This has to be handled.
            if (messagePart.getData() != null) {
                dst.put(messagePart.getData(), positionToReadFromChunk, numOfBytesToRead);
            }

            written += numOfBytesToRead;

            if (messagePartSize < DEFAULT_CONTENT_CHUNK_SIZE) { // Last message chunk has been received
                break;
            }
            currentOffsetValue += numOfBytesToRead;
        }
        return written;
    }

    /**
     * Retrieve the message part from the cache if available, otherwise retrieve it from the database and cache
     * the retrieved value for use.
     *
     * @param messageId The message Id
     * @param index The offset index value of the message part
     * @return Message Part
     * @throws AndesException
     */
    private static AndesMessagePart resolveCacheAndRetrieveMessagePart(long messageId, int index) throws AndesException {
        AndesMessagePart messagePart = null;
        boolean needToCache = true;

        AndesMessagePart cachedMessagePart =  messagePartCache.get(messageId);

        if (cachedMessagePart != null && cachedMessagePart.getOffset() == index) {
                messagePart = cachedMessagePart;
                needToCache = false;
        } else {
            messagePart = MessagingEngine.getInstance().getMessageContentChunk(messageId, index);

            if(messagePart == null) {
                throw new AndesException("Empty message part received while retrieving message content.");
            }
        }

        // The last part of the message content. The cache of this message part will not be useful.
        if (messagePart.getDataLength() < DEFAULT_CONTENT_CHUNK_SIZE) {
            needToCache = false;
        }

        /* If Message Part is not found in cache then it needs to be retrieved from the database and needs to be cached.
           Only the last retrieved message part is required in the cache since all the other parts up to it is sent.
        */
        if(needToCache) {
            messagePartCache.put(messageId, messagePart);
        }

        return messagePart;
    }

    /**
     * convert qpid queue to Andes queue
     *
     * @param amqQueue qpid queue
     * @return andes queue
     */
    public static InboundQueueEvent createInboundQueueEvent(AMQQueue amqQueue) {

        String queueName = amqQueue.getName();
        String queueOwner = (amqQueue.getOwner() != null) ? amqQueue.getOwner().toString() : "null";

        return new InboundQueueEvent(queueName,
                amqQueue.isDurable(),
                true,
                queueOwner,
                amqQueue.isExclusive());

    }

    /**
     * convert qpid exchange to andes exchange
     *
     * @param exchange qpid exchange
     * @return andes exchange
     */
    public static InboundExchangeEvent createAndesExchange(Exchange exchange) {
        return new InboundExchangeEvent(exchange.getName(), exchange.getType().getName().toString(),
                exchange.isAutoDelete());
    }

    /**
     * create andes binding from qpid binding
     *
     * @param exchange   qpid exchange binding points
     * @param queue      qpid queue binding points
     * @param routingKey routing key
     * @return InboundBindingEvent binding event that wrap AndesBinding
     */
    public static InboundBindingEvent createAndesBinding(Exchange exchange, AMQQueue queue, AMQShortString routingKey) {


        /**
         * we are checking the type of exchange to avoid the confusion
         * <<default>> exchange is actually a direct exchange. No need to keep two bindings
         */
        //TODO: extend to other types of exchanges
        String exchangeName = "";
        if (exchange.getType().equals(DirectExchange.TYPE)) {
            exchangeName = DirectExchange.TYPE.getDefaultExchangeName().toString();
        } else if (exchange.getType().equals(TopicExchange.TYPE)) {
            exchangeName = TopicExchange.TYPE.getDefaultExchangeName().toString();
        }

        String storageQueueName = AndesUtils.getStorageQueueForDestination(routingKey.toString(),
                exchangeName,queue.getName(),queue.isDurable());

        boolean isQueueShared = isQueueShared(queue.isDurable(), exchangeName);
        String queueOwner = "null";
        if(queue.getOwner() != null) {
            queueOwner = queue.getOwner().toString();
        }
        QueueInfo queueInformation = new QueueInfo(storageQueueName,
                queue.isDurable(),
                isQueueShared,
                queueOwner,
                queue.isExclusive());

        return new InboundBindingEvent(queueInformation,exchangeName,routingKey.toString());
    }

    /**
     * Get if a copy of a message in the queue should be sent to
     * bound subscribers in a round robin way
     * @param isDurable true is queue is durable
     * @param exchangeName name of exchange queue is bound to
     * @return true if message is shared between bound subscribers
     */
    private static boolean isQueueShared(boolean isDurable, String exchangeName) {

        boolean isQueueShared = true;

        /*
          for non durable topic queue, a copy of each message should be
          sent to each subscriber. Thus queue is not shared
         */
        if((AMQPUtils.TOPIC_EXCHANGE_NAME).equals(exchangeName)) {
            if (!isDurable) {
                isQueueShared = false;
            }
        }
        return isQueueShared;
    }

    public static boolean isTargetQueueBoundByMatchingToRoutingKey(String queueBoundRoutingKey,
                                                                   String messageRoutingKey) {
        boolean isMatching = false;
        if (queueBoundRoutingKey.equals(messageRoutingKey)) {
            isMatching = true;
        } else if (queueBoundRoutingKey.indexOf(TOPIC_AND_CHILDREN_WILDCARD) > 1) {
            int wildcardIndex = queueBoundRoutingKey.indexOf(TOPIC_AND_CHILDREN_WILDCARD);
            int partitionIndex;

            if (0 == wildcardIndex) {
                partitionIndex = 0;
            } else { // reduce one char for the constituent delimiter of topics
                partitionIndex = wildcardIndex - 1;
            }

            // Extract destination part before wild card character
            String wildcardPrefix = queueBoundRoutingKey.substring(0, partitionIndex);

            Pattern pattern = Pattern.compile(wildcardPrefix + ".*");
            Matcher matcher = pattern.matcher(messageRoutingKey);
            isMatching = matcher.matches();
        } else if (queueBoundRoutingKey.indexOf(IMMEDIATE_CHILDREN_WILDCARD) > 1) {
            int wildcardIndex = queueBoundRoutingKey.indexOf(IMMEDIATE_CHILDREN_WILDCARD);
            int partitionIndex;

            if (0 == wildcardIndex) {
                partitionIndex = 0;
            } else { // reduce one char for the constituent delimiter of topics
                partitionIndex = wildcardIndex - 1;
            }

            // Extract destination part before wild card character
            String wildcardPrefix = queueBoundRoutingKey.substring(0, partitionIndex);

            Pattern pattern = Pattern.compile("^" + wildcardPrefix + "[.][^.]+$");
            Matcher matcher = pattern.matcher(messageRoutingKey);
            isMatching = matcher.matches();
        }
        return isMatching;
    }

    /**
     * Checks whether a given subscription is a wildcard subscription.
     *
     * @param destination The destination string subscriber subscribed to
     * @return is this a wild card subscription
     */
    public static boolean isWildCardDestination(String destination) {
        boolean isWildCard = false;

        if (destination.contains(TOPIC_AND_CHILDREN_WILDCARD) || destination.contains(IMMEDIATE_CHILDREN_WILDCARD)) {
            isWildCard = true;
        }

        return isWildCard;
    }
}
