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
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.binding.Binding;
import org.wso2.andes.server.exchange.DirectExchange;
import org.wso2.andes.server.exchange.Exchange;
import org.wso2.andes.server.exchange.TopicExchange;
import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.server.message.MessageMetaData;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.andes.server.queue.SimpleQueueEntryList;
import org.wso2.andes.kernel.slot.Slot;
import org.wso2.andes.server.store.MessageMetaDataType;
import org.wso2.andes.server.store.StorableMessageMetaData;
import org.wso2.andes.server.subscription.Subscription;
import org.wso2.andes.store.StoredAMQPMessage;
import org.wso2.andes.subscription.AMQPLocalSubscription;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AMQPUtils {

    public static String DIRECT_EXCHANGE_NAME = "amq.direct";

    public static String TOPIC_EXCHANGE_NAME = "amq.topic";

    public static String DEFAULT_EXCHANGE_NAME = "<<default>>";

    public static final int DEFAULT_CONTENT_CHUNK_SIZE = 65534;

    private static Log log = LogFactory.getLog(AMQPUtils.class);

    /* Store previously queried message part as a cache so when the same part was required
     for the next chunk it can be retrieved without accessing the database */
    private static Map<Long, AndesMessagePart> messagePartCache = new HashMap<Long, AndesMessagePart>();

    /**
     * convert Andes metadata list to qpid queue entry list
     *
     * @param queue        qpid queue
     * @param metadataList Andes metadata list
     * @return Queue Entry list
     */
    public static List<QueueEntry> getQueueEntryListFromAndesMetaDataList(AMQQueue queue, List<AndesMessageMetadata> metadataList) {
        List<QueueEntry> messages = new ArrayList<QueueEntry>();
        SimpleQueueEntryList list = new SimpleQueueEntryList(queue);
        List<AMQMessage> amqMessageList = getEntryAMQMessageListFromAndesMetaDataList(metadataList);

        for (AMQMessage message : amqMessageList) {
            message.getStoredMessage().setExchange("amq.direct");
            messages.add(list.add(message));
        }
        return messages;
    }

    /**
     * convert Andes metadata list to qpid AMQMessages
     *
     * @param metadataList andes metadata list
     * @return AMQ message list
     */
    public static List<AMQMessage> getEntryAMQMessageListFromAndesMetaDataList(List<AndesMessageMetadata> metadataList) {
        List<AMQMessage> messages = new ArrayList<AMQMessage>();

        for (AndesMessageMetadata metadata : metadataList) {
            AMQMessage amqMessage = getAMQMessageFromAndesMetaData(metadata);
            messages.add(amqMessage);
        }
        return messages;
    }

    /**
     * convert andes metadata to qpid AMQMessage
     *
     * @param metadata andes metadata
     * @return AMQMessage
     */
    public static AMQMessage getAMQMessageFromAndesMetaData(AndesMessageMetadata metadata) {
        long messageId = metadata.getMessageID();
        Slot slot = metadata.getSlot();

        StorableMessageMetaData metaData = convertAndesMetadataToAMQMetadata(metadata);
        //create message with meta data. This has access to message content
        StoredAMQPMessage message = new StoredAMQPMessage(messageId, metaData);
        message.setSlot(slot);
        AMQMessage amqMessage = new AMQMessage(message);
        return amqMessage;
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
    public static AMQMessage getAMQMessageForDelivery(AndesMessageMetadata metadata, AndesContent content) {
        long messageId = metadata.getMessageID();
        Slot slot = metadata.getSlot();

        //create message with meta data. This has access to message content
        StorableMessageMetaData metaData = convertAndesMetadataToAMQMetadata(metadata);
        QpidStoredMessage<MessageMetaData> message = new QpidStoredMessage<MessageMetaData>(
                new StoredAMQPMessage(messageId, metaData), content);
        message.setSlot(slot);
        return new AMQMessage(message);
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
    public static AndesMessageMetadata convertAMQMessageToAndesMetadata(AMQMessage amqMessage, UUID channelID) throws AndesException {
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
        metadata.setChannelId(channelID);
        metadata.setSlot(amqMessage.getSlot());
        metadata.setExpirationTime(amqMessage.getExpiration());
        metadata.setArrivalTime(amqMessage.getArrivalTime());

        return metadata;
    }

    /**
     * create AMQP local subscription from qpid subscription
     *
     * @param queue        qpid queue
     * @param subscription qpid subscription
     * @param b            qpid binding
     * @return AMQP local subscription
     * @throws AndesException
     */
    //in order to tell if this is a queue subscription or a topic subscription, binding is needed
    public static LocalSubscription createAMQPLocalSubscription(AMQQueue queue, Subscription subscription, Binding b) throws AndesException {

        String subscriptionID = String.valueOf(subscription.getSubscriptionID());
        Exchange exchange = b.getExchange();
        String destination = b.getBindingKey();
        String subscribedNode = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();
        long subscribeTime = System.currentTimeMillis();
        String queueOwner = (queue.getOwner() == null) ? null : queue.getOwner().toString();
        String queueBoundExchangeName = "";
        String queueBoundExchangeType = exchange.getType().toString();
        Short isqueueBoundExchangeAutoDeletable = Short.parseShort(exchange.isAutoDelete() ? Integer.toString(1) : Integer.toString(0));
        boolean isBoundToTopic = false;

        /**
         *we are checking the type to avoid the confusion
         *<<default>> exchange is actually a direct exchange. No need to keep two bindings
         */
        //TODO: extend to other types of exchanges
        if (exchange.getType().equals(DirectExchange.TYPE)) {
            queueBoundExchangeName = DirectExchange.TYPE.getDefaultExchangeName().toString();
            isBoundToTopic = false;
        } else if (exchange.getType().equals(TopicExchange.TYPE)) {
            queueBoundExchangeName = TopicExchange.TYPE.getDefaultExchangeName().toString();
            isBoundToTopic = true;
        }

        /**
         * For durable topic subscriptions subscription ID should be unique for a client ID.
         * Thus we are redefining the subscription id to client ID
         */
        if(queue.isDurable() && isBoundToTopic) {
            subscriptionID = queue.getName();
        }

        AMQPLocalSubscription localSubscription = new AMQPLocalSubscription(queue,
                subscription, subscriptionID, destination, isBoundToTopic, queue.isExclusive(), queue.isDurable(),
                subscribedNode, subscribeTime, queue.getName(), queueOwner, queueBoundExchangeName, queueBoundExchangeType, isqueueBoundExchangeAutoDeletable, subscription.isActive());

        return localSubscription;
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
    public static int getMessageContentChunkConvertedCorrectly(long messageId, int offsetValue, ByteBuffer dst) throws AndesException {
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

        if (cachedMessagePart != null && cachedMessagePart.getOffSet() == index) {
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
     * create andes ack data message
     * @param channelID id of the connection message was received
     * @param messageID id of the message
     * @param destination  destination subscription who sent this ack is bound
     * @param storageDestination store destination of subscriber from which ack came from
     * @param isTopic is ack comes from a topic subscriber
     * @return Andes Ack Data
     */
    public static AndesAckData generateAndesAckMessage(UUID channelID, long messageID, String destination, String storageDestination, boolean isTopic) {
        return new AndesAckData(channelID, messageID,destination,storageDestination,isTopic);
    }

    /**
     * convert qpid queue to Andes queue
     *
     * @param amqQueue qpid queue
     * @return andes queue
     */
    public static AndesQueue createAndesQueue(AMQQueue amqQueue) {
        return new AndesQueue(amqQueue.getName(),
                (amqQueue.getOwner() != null) ? amqQueue.getOwner().toString() : "null",
                amqQueue.isExclusive(), amqQueue.isDurable());
    }

    /**
     * convert qpid exchange to andes exchange
     *
     * @param exchange qpid exchange
     * @return andes exchange
     */
    public static AndesExchange createAndesExchange(Exchange exchange) {
        return new AndesExchange(exchange.getName(), exchange.getType().getName().toString(),
                exchange.isAutoDelete());
    }

    /**
     * create andes binding from qpid binding
     *
     * @param exchange   qpid exchange binding points
     * @param queue      qpid queue binding points
     * @param routingKey routing key
     * @return Andes binding
     */
    public static AndesBinding createAndesBinding(Exchange exchange, AMQQueue queue, AMQShortString routingKey) {
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
        return new AndesBinding(exchangeName, AMQPUtils.createAndesQueue(queue), routingKey.toString());
    }

    public static String generateQueueName() {
        return "tmp_" + ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID() + UUID.randomUUID();
    }

    public static boolean isTargetQueueBoundByMatchingToRoutingKey(String queueBoundRoutingKey, String messageRoutingKey) {
        boolean isMatching = false;
        if (queueBoundRoutingKey.equals(messageRoutingKey)) {
            isMatching = true;
        } else if (queueBoundRoutingKey.indexOf(".#") > 1) {
            String p = queueBoundRoutingKey.substring(0, queueBoundRoutingKey.indexOf(".#"));
            Pattern pattern = Pattern.compile(p + ".*");
            Matcher matcher = pattern.matcher(messageRoutingKey);
            isMatching = matcher.matches();
        } else if (queueBoundRoutingKey.indexOf(".*") > 1) {
            String p = queueBoundRoutingKey.substring(0, queueBoundRoutingKey.indexOf(".*"));
            Pattern pattern = Pattern.compile("^" + p + "[.][^.]+$");
            Matcher matcher = pattern.matcher(messageRoutingKey);
            isMatching = matcher.matches();
        }
        return isMatching;
    }
}
