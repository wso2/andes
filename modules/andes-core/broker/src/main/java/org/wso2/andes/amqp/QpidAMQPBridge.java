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
import org.wso2.andes.AMQException;
import org.wso2.andes.AMQInternalException;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.FieldTable;
import org.wso2.andes.kernel.*;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.binding.Binding;
import org.wso2.andes.server.cassandra.AndesSubscriptionManager;
import org.wso2.andes.server.cassandra.QueueBrowserDeliveryWorker;
import org.wso2.andes.server.cluster.coordination.ClusterCoordinationHandler;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;
import org.wso2.andes.server.slot.SlotDeliveryWorkerManager;
import org.wso2.andes.server.exchange.Exchange;
import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.IncomingMessage;
import org.wso2.andes.server.stats.PerformanceCounter;
import org.wso2.andes.server.store.StorableMessageMetaData;
import org.wso2.andes.server.subscription.Subscription;
import org.wso2.andes.server.subscription.SubscriptionImpl;
import org.wso2.andes.server.virtualhost.VirtualHost;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class QpidAMQPBridge {

    private static Log log = LogFactory.getLog(QpidAMQPBridge.class);
    private static QpidAMQPBridge qpidAMQPBridge = null;
    /**
     * Following used by a performance counter
     */
    private static AtomicLong receivedMessageCounter = new AtomicLong();
    private static long last10kMessageReceivedTimestamp = System.currentTimeMillis();

    //keep listeners that should be triggered when constructs are updated
    private List<QueueListener> queueListeners = new ArrayList<QueueListener>();
    private List<ExchangeListener> exchangeListeners = new ArrayList<ExchangeListener>();
    private List<BindingListener> bindingListeners = new ArrayList<BindingListener>();

    /**
     * get QpidAMQPBridge instance
     *
     * @return QpidAMQPBridge instance
     */
    public static synchronized QpidAMQPBridge getInstance() {
        if (qpidAMQPBridge == null) {
            qpidAMQPBridge = new QpidAMQPBridge();
        }
        return qpidAMQPBridge;
    }

    private QpidAMQPBridge() {

        //register listeners for queue changes
        addQueueListener(new MessagePurgeHandler());
        addQueueListener(new ClusterCoordinationHandler(HazelcastAgent.getInstance()));

        //register listeners for exchange changes
        addExchangeListener(new ClusterCoordinationHandler(HazelcastAgent.getInstance()));

        //register listeners for binding changes
        addBindingListener(new ClusterCoordinationHandler(HazelcastAgent.getInstance()));
    }

    /**
     * Register a listener interested in local binding changes
     *
     * @param listener listener to register
     */
    public void addBindingListener(BindingListener listener) {
        bindingListeners.add(listener);
    }

    /**
     * Register a listener interested on queue changes
     *
     * @param listener listener to be registered
     */
    public void addQueueListener(QueueListener listener) {
        queueListeners.add(listener);
    }

    /**
     * Register a listener interested on exchange changes
     *
     * @param listener listener to be registered
     */
    public void addExchangeListener(ExchangeListener listener) {
        exchangeListeners.add(listener);
    }

    /**
     * message metadata received from AMQP transport.
     * This should happen after all content chunks are received
     *
     * @param incomingMessage message coming in
     * @param channelID       id of the channel
     * @throws AMQException
     */
    public void messageMetaDataReceived(IncomingMessage incomingMessage, UUID channelID) throws AMQException {
        try {
            if(log.isDebugEnabled()){
            log.debug("AMQP BRIDGE: Message id= " + incomingMessage.getMessageNumber() + "received");
            }
            AMQMessage message = new AMQMessage(incomingMessage.getStoredMessage());
            AndesMessageMetadata metadata = AMQPUtils.convertAMQMessageToAndesMetadata(message, channelID);
            String queue = message.getRoutingKey();

            if (queue == null) {
                log.error("Queue cannot be null, for " + incomingMessage.getMessageNumber());
            }

            MessagingEngine.getInstance().messageReceived(metadata);

            PerformanceCounter.recordMessageReceived(queue, incomingMessage.getReceivedChunkCount());
        } catch (AndesException e) {
            throw new AMQException(AMQConstant.INTERNAL_ERROR, "Error while storing incoming message metadata", e);
        }

        //Following code is only a performance counter
        Long localCount = receivedMessageCounter.incrementAndGet();
        if (localCount % 10000 == 0) {
            long timetook = System.currentTimeMillis() - last10kMessageReceivedTimestamp;
            log.info("Received " + localCount + ", throughput = " + (10000 * 1000 / timetook) + " msg/sec, " + timetook);
            last10kMessageReceivedTimestamp = System.currentTimeMillis();
        }

    }

    /**
     * read metadata of a message from store
     *
     * @param messageID id of the message
     * @return StorableMessageMetaData
     * @throws AMQException
     */
    public StorableMessageMetaData getMessageMetaData(long messageID) throws AMQException {
        StorableMessageMetaData metaData;
        try {
            metaData = AMQPUtils.convertAndesMetadataToAMQMetadata
                    (MessagingEngine.getInstance().getMessageMetaData(messageID));
        } catch (AndesException e) {
            log.error("Error in getting meta data for messageID", e);
            throw new AMQException(AMQConstant.INTERNAL_ERROR, "Error in getting meta data for messageID " + messageID, e);
        }
        return metaData;
    }

    /**
     * message content chunk received to the server
     *
     * @param messageID       id of message to which content belongs
     * @param offsetInMessage chunk offset
     * @param src             Bytebuffer with content bytes
     */
    public void messageContentChunkReceived(long messageID, int offsetInMessage, ByteBuffer src) {

        if(log.isDebugEnabled()){
        log.debug("AMQP BRIDGE: Content Part Received id= " + messageID + " offset= " + offsetInMessage);
        }
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

    /**
     * Read a message content chunk form store
     *
     * @param messageID       id of the message
     * @param offsetInMessage offset to be read
     * @param dst             buffer to be filled by content bytes
     * @return written content length
     * @throws AMQException
     */
    public int getMessageContentChunk(long messageID, int offsetInMessage, ByteBuffer dst) throws AMQException {
        int contentLenWritten;
        try {
            contentLenWritten = AMQPUtils.getMessageContentChunkConvertedCorrectly(messageID, offsetInMessage, dst);
        } catch (AndesException e) {
            log.error("Error in getting message content", e);
            throw new AMQException(AMQConstant.INTERNAL_ERROR, "Error in getting message content chunk messageID " + messageID + " offset=" + offsetInMessage, e);
        }
        return contentLenWritten;
    }

    public void ackReceived(UUID channelID, long messageID, String queueName, boolean isTopic) throws AMQException{
        try {
        if(log.isDebugEnabled()){
        log.debug("AMQP BRIDGE: ack received for message id= " + messageID + " channelId= " + channelID);
        }
        AndesAckData andesAckData = AMQPUtils.generateAndesAckMessage(channelID, messageID,queueName,isTopic);
        MessagingEngine.getInstance().ackReceived(andesAckData);
        } catch (AndesException e) {
            log.error("Exception occurred while handling ack", e);
            throw new AMQException(AMQConstant.INTERNAL_ERROR, "Error in getting handling ack for " + messageID, e);
        }
    }

    public void rejectMessage(AndesMessageMetadata metadata) throws AMQException {
        try {
            log.debug("AMQP BRIDGE: rejected message id= " + metadata.getMessageID() + " channel = " + metadata.getChannelId());
            MessagingEngine.getInstance().messageRejected(metadata);
        } catch (AndesException e) {
            throw new AMQException(AMQConstant.INTERNAL_ERROR, "Error while handling rejected message", e);
        }
    }


    /**
     * create a AMQP subscription in andes kernel
     *
     * @param subscription qpid subscription
     * @param queue        qpid queue
     * @throws AMQException
     */
    public void createAMQPSubscription(Subscription subscription, AMQQueue queue) throws AMQException {
        try {
            if(log.isDebugEnabled()){
            log.debug("AMQP BRIDGE: create AMQP Subscription subID " + subscription.getSubscriptionID() + " from queue "
                    + queue.getName());
            }

            if (subscription instanceof SubscriptionImpl.BrowserSubscription) {
                boolean isInMemoryMode = ClusterResourceHolder.getInstance().getClusterConfiguration().isInMemoryMode();
                QueueBrowserDeliveryWorker deliveryWorker = new QueueBrowserDeliveryWorker(subscription, queue, ((SubscriptionImpl.BrowserSubscription) subscription).getProtocolSession(), isInMemoryMode);
                deliveryWorker.send();
            } else {
                if(log.isDebugEnabled()){
                log.debug("Adding Subscription " + subscription.getSubscriptionID() + " to queue " + queue.getName());
                }
                addLocalSubscriptionsForAllBindingsOfQueue(queue, subscription);
            }

            SlotDeliveryWorkerManager slotDeliveryWorkerManager = SlotDeliveryWorkerManager.getInstance();
            slotDeliveryWorkerManager.startSlotDeliveryWorker(queue.getName());
        } catch (AndesException e) {
            log.error("Error while adding the subscription", e);
            throw new AMQException(AMQConstant.INTERNAL_ERROR, "Error while registering subscription", e);
        }
    }

    /**
     * Close AMQP subscription in Andes kernel
     *
     * @param queue        qpid queue
     * @param subscription qpid subscription
     * @throws AndesException
     */
    public void closeAMQPSubscription(AMQQueue queue, Subscription subscription) throws AndesException {
        if(log.isDebugEnabled()){
        log.debug("AMQP BRIDGE: close AMQP Subscription subID " + subscription.getSubscriptionID() + " from queue " +
                queue.getName());
        }

        // Browser subscriptions are not registered and hence not needed to be closed.
        if (!(subscription instanceof SubscriptionImpl.BrowserSubscription)) {
            closeLocalSubscriptionsForAllBindingsOfQueue(queue, subscription);
        }
    }

    /**
     * Create an exchange in andes kernel
     *
     * @param exchange qpid exchange
     * @throws AMQException
     */
    public void createExchange(Exchange exchange) throws AMQException {
        if(log.isDebugEnabled()){
        log.debug("AMQP BRIDGE: create Exchange" + exchange.getName());
        }
        try {
            /*AndesSubscriptionManager subManager = ClusterResourceHolder.getInstance().getSubscriptionManager();
            LocalSubscription sub = AMQPUtils.createInactiveLocalSubscriberRepresentingExchange(exchange);
            subManager.addSubscription(sub);*/
            AndesContext.getInstance().getAMQPConstructStore().addExchange(AMQPUtils.createAndesExchange(exchange), true);
            for (ExchangeListener listener : exchangeListeners) {
                listener.handleLocalExchangesChanged(AMQPUtils.createAndesExchange(exchange), ExchangeListener.ExchangeChange.Added);
            }

        } catch (AndesException e) {
            log.error("error while creating exchange", e);
            throw new AMQException(AMQConstant.INTERNAL_ERROR, "error while creating exchange", e);
        }
    }

    /**
     * Delete exchange from andes kernel
     *
     * @param exchange qpid exchange
     * @throws AMQException
     */
    public void deleteExchange(Exchange exchange) throws AMQException {
        if(log.isDebugEnabled()){
        log.debug("AMQP BRIDGE: delete Exchange " + exchange.getName());
        }
        try {
            AndesContext.getInstance().getAMQPConstructStore().removeExchange(exchange.getName(), true);
            for (ExchangeListener listener : exchangeListeners) {
                listener.handleLocalExchangesChanged(AMQPUtils.createAndesExchange(exchange), ExchangeListener.ExchangeChange.Deleted);
            }
        } catch (AndesException e) {
            log.error("error while deleting exchange", e);
            throw new AMQException(AMQConstant.INTERNAL_ERROR, "error while deleting exchange", e);
        }
    }

    /**
     * Create queue in andes kernel
     *
     * @param queue qpid queue
     * @throws AMQException
     */
    public void createQueue(AMQQueue queue) throws AMQException {
        if(log.isDebugEnabled()){
        log.debug("AMQP BRIDGE: create queue: " + queue.getName());
        }
        try {
            AndesContext.getInstance().getAMQPConstructStore().addQueue(AMQPUtils.createAndesQueue(queue), true);
            for (QueueListener queueListener : queueListeners) {
                queueListener.handleLocalQueuesChanged(AMQPUtils.createAndesQueue(queue), QueueListener.QueueChange.Added);
            }
            //AndesSubscriptionManager subManager = ClusterResourceHolder.getInstance().getSubscriptionManager();
            //.subManager.addSubscription(AMQPUtils.createInactiveLocalSubscriberRepresentingQueue(queue));
        } catch (AndesException e) {
            log.error("error while creating queue", e);
            throw new AMQException(AMQConstant.INTERNAL_ERROR, "error while creating queue", e);
        }
    }

    /**
     * Delete queue from andes kernel
     *
     * @param queue qpid queue
     * @throws AMQException
     */
    public void deleteQueue(AMQQueue queue) throws AMQException {
        if(log.isDebugEnabled()){
        log.debug("AMQP BRIDGE:  delete queue : " + queue.getName());
        }
        try {
            AndesContext.getInstance().getAMQPConstructStore().removeQueue(queue.getName(), true);
            for (QueueListener queueListener : queueListeners) {
                queueListener.handleLocalQueuesChanged(AMQPUtils.createAndesQueue(queue), QueueListener.QueueChange.Deleted);
            }
            //ClusterResourceHolder.getInstance().getSubscriptionManager().removeSubscriptionsRepresentingQueue(queue.getName(),queue.isExclusive());

        } catch (AndesException e) {
            log.error("error while removing queue", e);
            throw new AMQException(AMQConstant.INTERNAL_ERROR, "error while removing queue", e);
        }
    }

    /**
     * Create binding in andes kernel
     *
     * @param exchange   qpid exchange
     * @param routingKey routing key
     * @param queue      qpid queue
     * @param args       qpid queue arguments
     * @throws AMQInternalException
     */
    public void createBinding(Exchange exchange, AMQShortString routingKey,
                              AMQQueue queue, FieldTable args) throws AMQInternalException {
        if(log.isDebugEnabled()){
        log.debug("AMQP BRIDGE: createBinding exchange=" + exchange.getName() + " routingKey=" + routingKey + " queue=" + queue.getName());
        }
        try {
            /**
             * durable topic case is handled inside qpid itself.
             * So we do not check for it here
             */
            AndesBinding binding = AMQPUtils.createAndesBinding(exchange, queue, routingKey);

            AndesContext.getInstance().getAMQPConstructStore().addBinding(binding, true);
            for (BindingListener bindingListener : bindingListeners) {
                bindingListener.handleLocalBindingsChanged(binding, BindingListener.BindingChange.Added);
            }
        } catch (AndesException e) {
            log.error("error while creating binding", e);
            throw new AMQInternalException("error while removing queue", e);
        }
    }

    /**
     * remove binding from andes kernel
     *
     * @param b           qpid binding
     * @param virtualHost virtualhost binding belongs
     * @throws AndesException
     */
    public void removeBinding(Binding b, VirtualHost virtualHost) throws AndesException {
        if(log.isDebugEnabled()){
        log.debug("AMQP BRIDGE: removeBinding binding key: " + b.getBindingKey() + " queue: " + b.getQueue().getName());
        }
        AndesBinding binding = AMQPUtils.createAndesBinding(b.getExchange(), b.getQueue(), new AMQShortString(b.getBindingKey()));
        AndesContext.getInstance().getAMQPConstructStore().removeBinding(binding.boundExchangeName, binding.boundQueue.queueName, true);
        for (BindingListener bindingListener : bindingListeners) {
            bindingListener.handleLocalBindingsChanged(binding, BindingListener.BindingChange.Deleted);
        }
    }

    /**
     * create local subscriptions and add for every unique binding of the queue
     *
     * @param queue        AMQ queue
     * @param subscription subscription
     * @throws AndesException
     */
    private void addLocalSubscriptionsForAllBindingsOfQueue(AMQQueue queue, Subscription subscription) throws AndesException {
        AndesSubscriptionManager subscriptionManager = ClusterResourceHolder.
                getInstance().getSubscriptionManager();

        List<Binding> bindingList = queue.getBindings();
        if (bindingList != null && !bindingList.isEmpty()) {
            Set<AndesBinding> uniqueBindings = new HashSet<AndesBinding>();
            /**
             * Iterate unique bindings of the queue and add subscription entries.
             */
            try {
                for (Binding b : bindingList) {
                    AndesBinding andesBinding = AMQPUtils.createAndesBinding(b.getExchange(), b.getQueue(), new AMQShortString(b.getBindingKey()));
                    if (uniqueBindings.add(andesBinding)) {
                        LocalSubscription localSubscription = AMQPUtils.createAMQPLocalSubscription(queue, subscription, b);
                        subscriptionManager.addSubscription(localSubscription);
                    }
                }
            } catch (AndesException e) {
                log.warn("Reverting already created subscription entries for this subscription", e);
                for (AndesBinding b : uniqueBindings) {
                    for (Binding qpidBinding : bindingList) {
                        if (qpidBinding.getQueue().getName().equals(b.boundQueue.queueName) &&
                                qpidBinding.getBindingKey().equals(b.routingKey) &&
                                qpidBinding.getExchange().getName().equals(b.boundExchangeName)) {
                            LocalSubscription localSubscription = AMQPUtils.createAMQPLocalSubscription(queue, subscription, qpidBinding);
                            subscriptionManager.closeLocalSubscription(localSubscription);
                        }
                    }
                }
                throw new AndesException("error while adding the local subscription", e);
            }
        }
    }

    /**
     * remove local subscriptions and add for every unique binding of the queue
     *
     * @param queue        AMQ queue
     * @param subscription subscription to remove
     * @throws AndesException
     */
    private void closeLocalSubscriptionsForAllBindingsOfQueue(AMQQueue queue, Subscription subscription) throws AndesException {
        AndesSubscriptionManager subscriptionManager = ClusterResourceHolder.
                getInstance().getSubscriptionManager();
        List<Binding> bindingList = queue.getBindings();
        if (bindingList != null && !bindingList.isEmpty()) {
            Set<AndesBinding> uniqueBindings = new HashSet<AndesBinding>();
            /**
             * Iterate unique bindings of the queue and remove subscription entries.
             */
            for (Binding b : bindingList) {
                AndesBinding andesBinding = AMQPUtils.createAndesBinding(b.getExchange(), b.getQueue(), new AMQShortString(b.getBindingKey()));
                if (uniqueBindings.add(andesBinding)) {
                    LocalSubscription localSubscription = AMQPUtils.createAMQPLocalSubscription(queue, subscription, b);
                    subscriptionManager.closeLocalSubscription(localSubscription);
                }
            }
        }
    }

    /**
     * Channel closed. Clear status
     * @param channelID id of the closed channel
     */
    public void channelIsClosing(UUID channelID) {
        MessagingEngine.getInstance().clientConnectionClosed(channelID);
    }
}
