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
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.LocalSubscription;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.messageStore.StoredAMQPMessage;
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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public  class AMQPUtils {

    public static String DIRECT_EXCHANGE_NAME = "amq.direct";

    public static String TOPIC_EXCHANGE_NAME = "amq.topic";

    public static final int DEFAULT_CONTENT_CHUNK_SIZE = 65534;

    private static Log log = LogFactory.getLog(AMQPUtils.class);

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

    public static List<AMQMessage> getEntryAMQMessageListFromAndesMetaDataList(List<AndesMessageMetadata> metadataList) {
        List<AMQMessage> messages = new ArrayList<AMQMessage>();

        for (AndesMessageMetadata metadata : metadataList) {
            AMQMessage amqMessage = getAMQMessageFromAndesMetaData(metadata);
            messages.add(amqMessage);
        }
        return messages;
    }

    public static AMQMessage getAMQMessageFromAndesMetaData(AndesMessageMetadata metadata) {
        long messageId = metadata.getMessageID();
        StorableMessageMetaData metaData = convertAndesMetadataToAMQMetadata(metadata);
        //create message with meta data. This has access to message content
        StoredAMQPMessage message = new StoredAMQPMessage(messageId, metaData);
        AMQMessage amqMessage = new AMQMessage(message);
        return amqMessage;
    }

    public static QueueEntry convertAMQMessageToQueueEntry(AMQMessage message, AMQQueue queue) {
        SimpleQueueEntryList list = new SimpleQueueEntryList(queue);
        return list.add(message);
    }

    public static StorableMessageMetaData convertAndesMetadataToAMQMetadata(AndesMessageMetadata andesMessageMetadata) {
        byte[] dataAsBytes = andesMessageMetadata.getMetadata();
        ByteBuffer buf = ByteBuffer.wrap(dataAsBytes);
        buf.position(1);
        buf = buf.slice();
        MessageMetaDataType type = MessageMetaDataType.values()[dataAsBytes[0]];
        StorableMessageMetaData metaData = type.getFactory().createMetaData(buf);
        return metaData;
    }

    public static AndesMessageMetadata convertAMQMessageToAndesMetadata(AMQMessage amqMessage) throws AndesException{
        MessageMetaData amqMetadata =  amqMessage.getMessageMetaData();
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


    public static LocalSubscription createAMQPLocalSubscription(AMQQueue queue, Subscription subscription, Binding b) throws AndesException{

        Exchange exchange = b.getExchange();

        String subscriptionID = String.valueOf(subscription.getSubscriptionID());
        String destination = b.getBindingKey();
        String queueOwner = (queue.getOwner() == null) ? null : queue.getOwner().toString();
        String nodeQueueName = "";
        String queueBoundExchangeName = "";
        String queueBoundExchangeType = exchange.getType().toString();
        Short isqueueBoundExchangeAutoDeletable = Short.parseShort(exchange.isAutoDelete() ? Integer.toString(1) : Integer.toString(0));
        boolean isBoundToTopic = false;

        if (exchange.getType().equals(DirectExchange.TYPE) && queue.isDurable()) {
            queueBoundExchangeName = DirectExchange.TYPE.getDefaultExchangeName().toString();
            nodeQueueName = MessagingEngine.getMyNodeQueueName();
            isBoundToTopic = false;
        } else if(exchange.getType().equals(TopicExchange.TYPE)) {
            nodeQueueName = AndesUtils.getTopicNodeQueueName();
            queueBoundExchangeName = TopicExchange.TYPE.getDefaultExchangeName().toString();
            isBoundToTopic = true;
        }

        AMQPLocalSubscription localSubscription = new AMQPLocalSubscription(queue,
                subscription, subscriptionID, destination, isBoundToTopic, queue.isExclusive(), queue.isDurable(),
                nodeQueueName, queue.getName(), queueOwner, queueBoundExchangeName, queueBoundExchangeType, isqueueBoundExchangeAutoDeletable,subscription.isActive());

        return localSubscription;
    }

    public static LocalSubscription createInactiveLocalSubscriberRepresentingQueue(AMQQueue queue) {
           return new AMQPLocalSubscription(queue,null,"0", queue.getName(), false,queue.isExclusive(), queue.isDurable(),
                   MessagingEngine.getMyNodeQueueName(), queue.getName(), (queue.getOwner() == null) ? null : queue.getOwner().toString(),
                   AMQPUtils.DIRECT_EXCHANGE_NAME, DirectExchange.TYPE.toString(), Short.parseShort("0"),false);
    }

    public static LocalSubscription createInactiveLocalSubscriberRepresentingExchange(Exchange exchange) {
        return new AMQPLocalSubscription(null,null,"0", null, false,false,true,
                null, null, null,
                exchange.getName(), exchange.getType().toString(), exchange.isAutoDelete() ? Short.parseShort("1") : Short.parseShort("0"), false);
    }

    public static LocalSubscription createAMQPLocalSubscriptionRepresentingBinding(Exchange exchange, AMQQueue queue, AMQShortString routingKey) {


        String subscriptionID = "0";
        String destination = routingKey.toString();
        String queueOwner = (queue.getOwner() == null) ? null : queue.getOwner().toString();
        String nodeQueueName = "";
        String queueBoundExchangeName = "";
        String queueBoundExchangeType = exchange.getType().toString();
        Short isqueueBoundExchangeAutoDeletable = Short.parseShort(exchange.isAutoDelete() ? Integer.toString(1) : Integer.toString(0));
        boolean isBoundToTopic = false;

        if (exchange.getType().equals(DirectExchange.TYPE) && queue.isDurable()) {
            queueBoundExchangeName = DirectExchange.TYPE.getDefaultExchangeName().toString();
            nodeQueueName = MessagingEngine.getMyNodeQueueName();
            isBoundToTopic = false;
        } else if(exchange.getType().equals(TopicExchange.TYPE)) {
            nodeQueueName = AndesUtils.getTopicNodeQueueName();
            queueBoundExchangeName = TopicExchange.TYPE.getDefaultExchangeName().toString();
            isBoundToTopic = true;
        }

        AMQPLocalSubscription localSubscription = new AMQPLocalSubscription(queue,
                null , subscriptionID, destination, isBoundToTopic, queue.isExclusive(), queue.isDurable(),
                nodeQueueName, queue.getName(), queueOwner, queueBoundExchangeName, queueBoundExchangeType, isqueueBoundExchangeAutoDeletable,true);

        return localSubscription;
    }

    public static int getMessageContentChunkConvertedCorrectly(long messageId, int offsetValue, ByteBuffer dst) throws AndesException {
        int written = 0;
        int initialBufferSize = dst.remaining();

        double chunkIndex = (float)offsetValue / DEFAULT_CONTENT_CHUNK_SIZE;
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
                AndesMessagePart firstPart = MessagingEngine.getInstance().getMessageContentChunk(messageId,firstIndexToQuery);
                int firstMessagePartSize = firstPart.getDataLength();
                int numOfBytesToRead = firstMessagePartSize - positionToReadFromFirstChunk;
                if(initialBufferSize > numOfBytesToRead) {
                    dst.put(firstPart.getData(),positionToReadFromFirstChunk,numOfBytesToRead);
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
}
