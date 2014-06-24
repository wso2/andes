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
import org.wso2.andes.AMQException;
import org.wso2.andes.kernel.*;
import org.wso2.andes.messageStore.StoredAMQPMessage;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.binding.Binding;
import org.wso2.andes.server.cassandra.AndesSubscriptionManager;
import org.wso2.andes.server.cassandra.QueueBrowserDeliveryWorker;
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
import org.wso2.andes.server.subscription.SubscriptionImpl;
import org.wso2.andes.server.util.AndesUtils;
import org.wso2.andes.subscription.AMQPLocalSubscription;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public  class AMQPUtils {

    public static String DIRECT_EXCHANGE_NAME = "amq.direct";

    public static String TOPIC_EXCHANGE_NAME = "amq.topic";

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

    public static AndesMessageMetadata convertAMQMetaDataToAndesMetadata(StorableMessageMetaData amqMetadata) {
        MessageMetaData mmd = (MessageMetaData) amqMetadata;
        return convertAMQMetaDataToAndesMetadata(amqMetadata);
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
        metadata.setTopic(amqMetadata.getMessagePublishInfo().getExchange().equals("amq.topic")?true:false );

        return metadata;
    }

    /**
     * create local subscriptions and add for every binding of the queue
     * @param queue AMQ queue
     * @param subscription subscription
     * @throws AndesException
     * @throws AMQQueue.ExistingExclusiveSubscription
     */
    public static void addLocalSubscriptionsForAllBindingsOfQueue(AMQQueue queue, Subscription subscription) throws AndesException {
        AndesSubscriptionManager subscriptionManager = ClusterResourceHolder.
                getInstance().getSubscriptionManager();
        List<Binding> bindingList = queue.getBindings();
        if (bindingList != null && !bindingList.isEmpty())

        /**
         * Iterate bindings of the queue and add subscription entries. We need to do this because of the flat
         * subscription model we have
         */
        for (Binding b : bindingList) {
            Exchange exchange = b.getExchange();

            //register subscription
            String subscriptionID = String.valueOf(subscription.getSubscriptionID());
            String destination = b.getBindingKey();
            String queueOwner = (queue.getOwner() == null) ? null : queue.getOwner().toString();
            String nodeQueueName = "";
            boolean isBoundToTopic = false;

            //we sync and track only durable queues
            //when a topic subscription is created (Even for a durable topic) a non durable queue will be created. We do not track them
            if (exchange.getType().equals(DirectExchange.TYPE) && queue.isDurable()) {
                nodeQueueName = MessagingEngine.getMyNodeQueueName();
                isBoundToTopic = false;

                AMQPLocalSubscription localSubscription = new AMQPLocalSubscription(queue,
                        subscription, subscriptionID, destination, isBoundToTopic, queue.isExclusive(), queue.isDurable(),
                        nodeQueueName, queue.getName(), queueOwner, DirectExchange.TYPE.getDefaultExchangeName().toString(), true);

                subscriptionManager.addSubscription(localSubscription);

            //we track durable/non-durable queues for topics
            //durable ones for durable topics and non-durable ones for just statistics
            } else if(exchange.getType().equals(TopicExchange.TYPE)) {
                nodeQueueName = AndesUtils.getTopicNodeQueueName();
                isBoundToTopic = true;

                AMQPLocalSubscription localSubscription = new AMQPLocalSubscription(queue,
                        subscription, subscriptionID, destination, isBoundToTopic, queue.isExclusive(), queue.isDurable(),
                        nodeQueueName, queue.getName(), queueOwner, TopicExchange.TYPE.getDefaultExchangeName().toString(), true);

                subscriptionManager.addSubscription(localSubscription);
            }

        }

        if (subscription instanceof SubscriptionImpl.BrowserSubscription) {
            boolean isInMemoryMode = ClusterResourceHolder.getInstance().getClusterConfiguration().isInMemoryMode();
            QueueBrowserDeliveryWorker deliveryWorker = new QueueBrowserDeliveryWorker(subscription,queue,((SubscriptionImpl.BrowserSubscription) subscription).getProtocolSession(),isInMemoryMode);
            deliveryWorker.send();
        } else {
            log.info("Binding Subscription "+subscription.getSubscriptionID()+" to queue "+queue.getName());
        }
    }

    public static void closeLocalSubscriptionsForAllBindingsOfQueue(AMQQueue queue, Subscription subscription) throws AndesException {
        AndesSubscriptionManager subscriptionManager = ClusterResourceHolder.
                getInstance().getSubscriptionManager();
        List<Binding> bindingList = queue.getBindings();
        if (bindingList != null && !bindingList.isEmpty())

        /**
         * Iterate bindings of the queue and add subscription entries. We need to do this because of the flat
         * subscription model we have
         */
        for (Binding b : bindingList) {
            Exchange exchange = b.getExchange();

            //register subscription
            String subscriptionID = String.valueOf(subscription.getSubscriptionID());
            String destination = b.getBindingKey();
            String queueOwner = (queue.getOwner() == null) ? null : queue.getOwner().toString();
            String nodeQueueName = "";
            boolean isBoundToTopic = false;

            //we sync and track only durable queues
            //when a topic subscription is created (Even for a durable topic) a non durable queue will be created. We do not track them
            if (exchange.getType().equals(DirectExchange.TYPE) && queue.isDurable()) {
                nodeQueueName = MessagingEngine.getMyNodeQueueName();
                isBoundToTopic = false;

                AMQPLocalSubscription localSubscription = new AMQPLocalSubscription(queue,
                        subscription, subscriptionID, destination, isBoundToTopic, queue.isExclusive(), queue.isDurable(),
                        nodeQueueName, queue.getName(), queueOwner, DirectExchange.TYPE.getDefaultExchangeName().toString(), false);

                subscriptionManager.closeSubscription(localSubscription);

                //we track durable/non-durable queues for topics
                //durable ones for durable topics and non-durable ones for just statistics
            } else if(exchange.getType().equals(TopicExchange.TYPE)) {
                nodeQueueName = AndesUtils.getTopicNodeQueueName();
                isBoundToTopic = true;

                AMQPLocalSubscription localSubscription = new AMQPLocalSubscription(queue,
                        subscription, subscriptionID, destination, isBoundToTopic, queue.isExclusive(), queue.isDurable(),
                        nodeQueueName, queue.getName(), queueOwner, TopicExchange.TYPE.getDefaultExchangeName().toString(), false);

                subscriptionManager.closeSubscription(localSubscription);
            }

        }
    }

    public static LocalSubscription createInactiveLocalSubscriber(AMQQueue queue) {
           return new AMQPLocalSubscription(queue,null,"0", queue.getName(), false,queue.isExclusive(), queue.isDurable(),
                   MessagingEngine.getMyNodeQueueName(), queue.getName(), (queue.getOwner() == null) ? null : queue.getOwner().toString(),
                   AMQPUtils.DIRECT_EXCHANGE_NAME, false);
    }
}
