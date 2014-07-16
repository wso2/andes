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
import org.wso2.andes.AMQInternalException;
import org.wso2.andes.exchange.ExchangeDefaults;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.FieldTable;
import org.wso2.andes.kernel.*;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.binding.Binding;
import org.wso2.andes.server.cassandra.AndesSubscriptionManager;
import org.wso2.andes.server.cassandra.QueueBrowserDeliveryWorker;
import org.wso2.andes.server.exchange.Exchange;
import org.wso2.andes.server.exchange.ExchangeRegistry;
import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.IncomingMessage;
import org.wso2.andes.server.stats.PerformanceCounter;
import org.wso2.andes.server.subscription.Subscription;
import org.wso2.andes.server.subscription.SubscriptionImpl;
import org.wso2.andes.server.virtualhost.VirtualHost;
import org.wso2.andes.subscription.SubscriptionStore;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class QpidAMQPBridge {

    private static Log log = LogFactory.getLog(QpidAMQPBridge.class);
    private static QpidAMQPBridge qpidAMQPBridge = null;
    /**
     * Following used by a performance counter
     */
    private static AtomicLong receivedMessageCounter = new AtomicLong();
    private static long last10kMessageReceivedTimestamp = System.currentTimeMillis();

    public static synchronized QpidAMQPBridge getInstance() {
        if(qpidAMQPBridge == null){
            qpidAMQPBridge = new QpidAMQPBridge();
        }
        return qpidAMQPBridge;
    }

    public void messageMetaDataReceived(IncomingMessage incomingMessage, int channelID) throws AMQException {
        try {
            AMQMessage message = new AMQMessage(incomingMessage.getStoredMessage());
            AndesMessageMetadata metadata = AMQPUtils.convertAMQMessageToAndesMetadata(message);
            String queue = message.getRoutingKey();

            if (queue == null) {
                log.error("Queue cannot be null, for " + incomingMessage.getMessageNumber());
            }

            MessagingEngine.getInstance().messageReceived(metadata, channelID);

            PerformanceCounter.recordMessageReceived(queue, incomingMessage.getReceivedChunkCount());
        } catch (AndesException e) {
            throw new AMQException(AMQConstant.INTERNAL_ERROR, "Error while storing incoming message metadata", e);
        }

                /*
                 * Following code is only a performance counter
                 */
        Long localCount = receivedMessageCounter.incrementAndGet();
        if(localCount%10000 == 0){
            long timetook = System.currentTimeMillis() - last10kMessageReceivedTimestamp;
            log.info("Received " + localCount + ", throughput = " + (10000*1000/timetook) + " msg/sec, " + timetook);
            last10kMessageReceivedTimestamp = System.currentTimeMillis();
        }
                /*
                 * End of performance counter
                 */
    }

    public void messageContentChunkReceived(long messageID,  int offsetInMessage, ByteBuffer src) {

        AndesMessagePart part = new AndesMessagePart();
        src = src.slice();
        final byte[] chunkData = new byte[src.limit()];

        src.duplicate().get(chunkData);

        part.setData(chunkData);
        part.setMessageID(messageID);
        part.setOffSet(offsetInMessage);
        part.setDataLength(chunkData.length);

        MessagingEngine.getInstance().messageContentReceived(part);
    }

    public void createAMQPSubscription(Subscription subscription, AMQQueue queue) throws AMQException {
        try {
            log.info("===Andes Bridge: createAMQPSubscription subID " + subscription.getSubscriptionID() + " from queue " + queue.getName());
            addLocalSubscriptionsForAllBindingsOfQueue(queue, subscription);

            if (subscription instanceof SubscriptionImpl.BrowserSubscription) {
                boolean isInMemoryMode = ClusterResourceHolder.getInstance().getClusterConfiguration().isInMemoryMode();
                QueueBrowserDeliveryWorker deliveryWorker = new QueueBrowserDeliveryWorker(subscription, queue, ((SubscriptionImpl.BrowserSubscription) subscription).getProtocolSession(), isInMemoryMode);
                deliveryWorker.send();
            } else {
                log.info("Binding Subscription " + subscription.getSubscriptionID() + " to queue " + queue.getName());
            }
        } catch (AndesException e) {
            log.error("Error while adding the subscription", e);
            throw new AMQException(AMQConstant.INTERNAL_ERROR, "Error while registering subscription", e);
        }
    }

    public void closeAMQPSubscription(AMQQueue queue, Subscription subscription) throws AndesException{
        log.info("===Andes Bridge: closeAMQPSubscription subID " + subscription.getSubscriptionID() + " from queue " + queue.getName());
        closeLocalSubscriptionsForAllBindingsOfQueue(queue, subscription);
    }

    public void createExchange(Exchange exchange) throws AMQException{
        log.info("===Andes Bridge: createExchange" + exchange.getName());
/*        try {
            AndesSubscriptionManager subManager = ClusterResourceHolder.getInstance().getSubscriptionManager();
            LocalSubscription sub = AMQPUtils.createInactiveLocalSubscriberRepresentingExchange(exchange);
            subManager.addSubscription(sub);
        } catch (AndesException e) {
            log.error("error while creating exchange", e);
            throw new AMQException(AMQConstant.INTERNAL_ERROR,"error while creating exchange",e);
        }*/
    }

    public void deleteExchange(Exchange exchange) throws AMQException{
        log.info("===Andes Bridge: deleteExchange " + exchange.getName());
        try {
            SubscriptionStore subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
            AndesExchange andesExchange = new AndesExchange(exchange.getName(), exchange.getType().getName().toString(),
                    exchange.isAutoDelete() ? Short.parseShort("1") : Short.parseShort("0"));
            subscriptionStore.deleteExchange(andesExchange);
        } catch (AndesException e) {
            log.error("error while deleting exchange", e);
            throw new AMQException(AMQConstant.INTERNAL_ERROR,"error while deleting exchange",e);
        }
    }

    public void createQueue(AMQQueue queue) throws AMQException {
        log.info("===Andes Bridge create queue: " + queue.getName());
        try {
            AndesContext.getInstance().getSubscriptionStore().addLocalSubscription(AMQPUtils.createInactiveLocalSubscriberRepresentingQueue(queue));
        } catch (AndesException e) {
            log.error("error while creating queue", e);
            throw new AMQException(AMQConstant.INTERNAL_ERROR,"error while creating queue",e);
        }
    }

    public void deleteQueue(AMQQueue queue) throws AMQException {
        log.info("Andes Bridge delete queue : " + queue.getName());
        try {
            SubscriptionStore subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
            subscriptionStore.removeQueue(queue.getName(), queue.isExclusive());
            //caller should remove messages from global queue
            ClusterResourceHolder.getInstance().getSubscriptionManager().handleMessageRemovalFromGlobalQueue(queue.getName());
        } catch (AndesException e) {
            log.error("error while removing queue", e);
            throw new AMQException(AMQConstant.INTERNAL_ERROR,"error while removing queue",e);
        }
    }

    public void createBinding(Exchange exchange, AMQShortString routingKey,
                              AMQQueue queue, FieldTable args) throws AMQInternalException {
        log.info("===Andes Bridge createBinding exchange=" + exchange.getName() + " routingKey=" + routingKey + " queue=" + queue.getName());
        //todo:we create bindings for the whole queue's bindings at once. This is not needed
/*        try {
            LocalSubscription sub = AMQPUtils.createAMQPLocalSubscriptionRepresentingBinding(exchange, queue, routingKey);
            ClusterResourceHolder.getInstance().getSubscriptionManager().addSubscription(sub);
        } catch (AndesException e) {
            log.error("error while creating binding", e);
        }*/
    }

    public void removeBinding(Binding b, VirtualHost virtualHost) throws AndesException {
        log.info("===Andes Bridge removeBinding binding key: " + b.getBindingKey() + " queue: " + b.getQueue().getName());
        SubscriptionStore subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
        //todo - hasitha - why queue bindings not removed??
        if(b.getExchange().getName().equalsIgnoreCase("amq.topic")) {

            //we remove bindings irrespective of temp or durable
            subscriptionStore.removeAllSubscriptionsRepresentingBinding(b.getBindingKey(), b.getQueue().getName());

            //stop Topic Delivery Worker If Having No actual normal (not durable) topic subscriptions on this node
            if(ClusterResourceHolder.getInstance().getTopicDeliveryWorker() != null) {
                ExchangeRegistry exchangeRegistry = virtualHost.getExchangeRegistry();
                Exchange exchange = exchangeRegistry.getExchange(ExchangeDefaults.TOPIC_EXCHANGE_NAME);
                boolean havingNormalTopicSubscriptions = false;
                for(Binding binding: exchange.getBindings()){
                    if(!binding.isDurable()) {
                        havingNormalTopicSubscriptions = true;
                        break;
                    }
                }
                if(!havingNormalTopicSubscriptions) {
                    ClusterResourceHolder.getInstance().getTopicDeliveryWorker().stopWorking();
                }
            }
        }
        System.out.println("Removed Local Binding:  binding key: " + b.getBindingKey() + " queue:" + b.getQueue().getName());

    }

    /**
     * create local subscriptions and add for every binding of the queue
     * @param queue AMQ queue
     * @param subscription subscription
     * @throws AndesException
     */
    private void addLocalSubscriptionsForAllBindingsOfQueue(AMQQueue queue, Subscription subscription) throws AndesException {
        AndesSubscriptionManager subscriptionManager = ClusterResourceHolder.
                getInstance().getSubscriptionManager();
        List<Binding> bindingList = queue.getBindings();
        if (bindingList != null && !bindingList.isEmpty())

        /**
         * Iterate bindings of the queue and add subscription entries. We need to do this because of the flat
         * subscription model we have
         */
            for (Binding b : bindingList) {

                LocalSubscription localSubscription = AMQPUtils.createAMQPLocalSubscription(queue, subscription, b);
                subscriptionManager.addSubscription(localSubscription);

            }
    }

    private void closeLocalSubscriptionsForAllBindingsOfQueue(AMQQueue queue, Subscription subscription) throws AndesException {
        AndesSubscriptionManager subscriptionManager = ClusterResourceHolder.
                getInstance().getSubscriptionManager();
        List<Binding> bindingList = queue.getBindings();
        if (bindingList != null && !bindingList.isEmpty())

        /**
         * Iterate bindings of the queue and add subscription entries. We need to do this because of the flat
         * subscription model we have
         */
            for (Binding b : bindingList) {
                LocalSubscription localSubscription = AMQPUtils.createAMQPLocalSubscription(queue, subscription, b);
                subscriptionManager.closeSubscription(localSubscription);
            }
    }



}
