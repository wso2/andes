/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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
import org.wso2.andes.store.StoredAMQPMessage;
import org.wso2.andes.server.binding.Binding;
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
import org.wso2.andes.server.subscription.Subscription;
import org.wso2.andes.server.util.AndesUtils;
import org.wso2.andes.subscription.AMQPLocalSubscription;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class AMQPUtils {

    public static String DIRECT_EXCHANGE_NAME = "amq.direct";

    public static String TOPIC_EXCHANGE_NAME = "amq.topic";

    public static final int DEFAULT_CONTENT_CHUNK_SIZE = 65534;

    private static Log log = LogFactory.getLog(AMQPUtils.class);

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
        StorableMessageMetaData metaData = convertAndesMetadataToAMQMetadata(metadata);
        //create message with meta data. This has access to message content
        StoredAMQPMessage message = new StoredAMQPMessage(messageId, metaData);
        AMQMessage amqMessage = new AMQMessage(message);
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
        StorableMessageMetaData metaData = type.getFactory().createMetaData(buf);
        return metaData;
    }

    /**
     * convert an AMQMessage to andes metadata
     *
     * @param amqMessage qpid amqMessage
     * @return andes message metadata
     * @throws AndesException
     */
    public static AndesMessageMetadata convertAMQMessageToAndesMetadata(AMQMessage amqMessage) throws AndesException {
        MessageMetaData amqMetadata = amqMessage.getMessageMetaData();
        String queue = amqMetadata.getMessagePublishInfo().getRoutingKey().toString();

        final int bodySize = 1 + amqMetadata.getStorableSize();
        byte[] underlying = new byte[bodySize];
        underlying[0] = (byte) amqMetadata.getType().ordinal();
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(underlying);
        buf.position(1);
        buf = buf.slice();
        amqMetadata.writeToBuffer(0, buf);

        AndesMessageMetadata metadata = new AndesMessageMetadata();

        metadata.setMessageID(amqMessage.getMessageId());
        metadata.setMetadata(underlying);
        metadata.setDestination(queue);
        metadata.setPersistent(amqMetadata.isPersistent());
        metadata.setTopic(amqMetadata.getMessagePublishInfo().getExchange().equals("amq.topic"));

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
        String queueOwner = (queue.getOwner() == null) ? null : queue.getOwner().toString();
        String nodeQueueName = "";
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
            nodeQueueName = MessagingEngine.getMyNodeQueueName();
            isBoundToTopic = false;
        } else if (exchange.getType().equals(TopicExchange.TYPE)) {
            nodeQueueName = AndesUtils.getTopicNodeQueueName();
            queueBoundExchangeName = TopicExchange.TYPE.getDefaultExchangeName().toString();
            isBoundToTopic = true;
        }

        AMQPLocalSubscription localSubscription = new AMQPLocalSubscription(queue,
                subscription, subscriptionID, destination, isBoundToTopic, queue.isExclusive(), queue.isDurable(),
                nodeQueueName, queue.getName(), queueOwner, queueBoundExchangeName, queueBoundExchangeType, isqueueBoundExchangeAutoDeletable, subscription.isActive());

        return localSubscription;
    }

    /**
     * read a message content chunk
     *
     * @param messageId   id of the message
     * @param offsetValue chunk offset to read
     * @param dst         buffet to fill bytes of content
     * @return written byte count
     * @throws AndesException
     */
    public static int getMessageContentChunkConvertedCorrectly(long messageId, int offsetValue, ByteBuffer dst) throws AndesException {
        int written = 0;
        int initialBufferSize = dst.remaining();

        double chunkIndex = (float) offsetValue / DEFAULT_CONTENT_CHUNK_SIZE;
        int firstChunkIndex = (int) Math.floor(chunkIndex);
        int secondChunkIndex = (int) Math.ceil(chunkIndex);

        int firstIndexToQuery = firstChunkIndex * DEFAULT_CONTENT_CHUNK_SIZE;
        int secondIndexToQuery = secondChunkIndex * DEFAULT_CONTENT_CHUNK_SIZE;

        int positionToReadFromFirstChunk = offsetValue - (firstChunkIndex * DEFAULT_CONTENT_CHUNK_SIZE);

        try {

            byte[] content = null;

            //first chunk might not have DEFAULT_CONTENT_CHUNK_SIZE
            if (offsetValue == 0) {
                AndesMessagePart messagePart = MessagingEngine.getInstance().getMessageContentChunk(messageId, offsetValue);
                int messagePartSize = messagePart.getDataLength();
                if (initialBufferSize > messagePartSize) {

                    dst.put(messagePart.getData());
                    written += messagePart.getDataLength();
                } else {
                    dst.put(messagePart.getData(), 0, initialBufferSize);
                    written += initialBufferSize;
                }
                //here there will be chunks of DEFAULT_CONTENT_CHUNK_SIZE
            } else {
                //first read from first chunk from where we stopped
                AndesMessagePart firstPart = MessagingEngine.getInstance().getMessageContentChunk(messageId, firstIndexToQuery);
                int firstMessagePartSize = firstPart.getDataLength();
                int numOfBytesToRead = firstMessagePartSize - positionToReadFromFirstChunk;
                if (initialBufferSize > numOfBytesToRead) {
                    dst.put(firstPart.getData(), positionToReadFromFirstChunk, numOfBytesToRead);
                    written += numOfBytesToRead;

                    //if we have additional size in buffer read from next chunk as well
/*                    int remainingSizeOfBuffer = initialBufferSize - dst.position();
                    if(remainingSizeOfBuffer > 0 ) {
                        AndesMessagePart secondPart = MessagingEngine.getInstance().getMessageContentChunk(messageId,secondIndexToQuery);
                        dst.put(secondPart.getData(),0,remainingSizeOfBuffer);
                        written += remainingSizeOfBuffer;
                    }*/
                } else {
                    dst.put(firstPart.getData(), positionToReadFromFirstChunk, initialBufferSize);
                    written += initialBufferSize;
                }
            }

        } catch (Exception e) {
            log.error("Error in reading content for message id " + messageId, e);
        }
        return written;
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
}
