/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.wso2.andes.exchange.ExchangeDefaults;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.FieldTable;
import org.wso2.andes.framing.abstraction.ContentChunk;
import org.wso2.andes.kernel.*;
import org.wso2.andes.kernel.distruptor.inbound.InboundBindingEvent;
import org.wso2.andes.kernel.distruptor.inbound.InboundExchangeEvent;
import org.wso2.andes.kernel.distruptor.inbound.InboundQueueEvent;
import org.wso2.andes.kernel.distruptor.inbound.InboundSubscriptionEvent;
import org.wso2.andes.kernel.distruptor.inbound.PubAckHandler;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.binding.Binding;
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

    /**
     * Ignore pub acknowledgements in AMQP. AMQP doesn't have pub acks
     */
    private PubAckHandler pubAckHandler;

    static {
        qpidAMQPBridge = new QpidAMQPBridge();
    }
    /**
     * get QpidAMQPBridge instance
     *
     * @return QpidAMQPBridge instance
     */
    public static QpidAMQPBridge getInstance() {
        return qpidAMQPBridge;
    }

    /**
     * Create a QpidAMQPBridge instance with disabled publisher ack implementation
     */
    private QpidAMQPBridge() {
        pubAckHandler = new DisablePubAckImpl();
    }

    /**
     * message metadata received from AMQP transport.
     * This should happen after all content chunks are received
     *
     * @param incomingMessage message coming in
     * @param channelID       id of the channel
     * @param andesChannel    AndesChannel
     * @throws AMQException
     */
    public void messageReceived(IncomingMessage incomingMessage, UUID channelID, AndesChannel andesChannel) throws AMQException {

        long receivedTime = System.currentTimeMillis();
        try {
            if (log.isDebugEnabled()) {
                log.debug("Message id " + incomingMessage.getMessageNumber() + " received");
            }
            AMQMessage message = new AMQMessage(incomingMessage.getStoredMessage());

            // message arrival time set to mb node's system time without using
            // message published time by publisher.
            message.getMessageMetaData().setArrivalTime(receivedTime);

            AndesMessageMetadata metadata = AMQPUtils.convertAMQMessageToAndesMetadata(message, channelID);
            String queue = message.getRoutingKey();

            if (queue == null) {
                log.error("Queue cannot be null, for " + incomingMessage.getMessageNumber());
                return;
            }

            AndesMessage andesMessage = new AndesMessage(metadata);

            // Update Andes message with all the chunk details
            int contentChunks = incomingMessage.getBodyCount();
            int offset = 0;
            for (int i = 0; i < contentChunks; i++) {
                ContentChunk chunk = incomingMessage.getContentChunk(i);
                AndesMessagePart messagePart = messageContentChunkReceived(
                                                        metadata.getMessageID(), offset, chunk.getData().buf());
                offset = offset + chunk.getSize();
                andesMessage.addMessagePart(messagePart);
            }

            // Handover message to Andes
            Andes.getInstance().messageReceived(andesMessage, andesChannel, pubAckHandler);

            if(log.isDebugEnabled()) {
                PerformanceCounter.recordMessageReceived(queue, incomingMessage.getReceivedChunkCount());
            }
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
    public AndesMessagePart messageContentChunkReceived(long messageID, int offsetInMessage, ByteBuffer src) {

        if (log.isDebugEnabled()) {
            log.debug("Content Part Received id " + messageID + ", offset " + offsetInMessage);
        }
        AndesMessagePart part = new AndesMessagePart();
        src = src.slice();
        final byte[] chunkData = new byte[src.limit()];
        src.duplicate().get(chunkData);

        part.setData(chunkData);
        part.setMessageID(messageID);
        part.setOffSet(offsetInMessage);
        part.setDataLength(chunkData.length);

        return part;
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

    public void ackReceived(UUID channelID, long messageID, String routingKey, boolean isTopic)
            throws AMQException {
        try {
            if (log.isDebugEnabled()) {
                log.debug("ack received for message id= " + messageID + " channelId= " + channelID);
            }
            AndesSubscription ackSentSubscription = AndesContext.getInstance().
                    getSubscriptionStore().getLocalSubscriptionForChannelId(channelID);
            if (ackSentSubscription == null) {
                //TODO : if an ack came here after subscription is closed, should we discard message?
                log.error("Cannot handle Ack. Subscription is null for channel= " + channelID + "Message Destination= "
                        + routingKey);
                return;
            }
            //This can be different from routing key in hierarchical topic case
            String subscriptionBoundDestination = ackSentSubscription.getSubscribedDestination();
            String storageQueueNameOfSubscription = ackSentSubscription.getStorageQueueName();
            AndesAckData andesAckData = AMQPUtils
                    .generateAndesAckMessage(channelID, messageID, subscriptionBoundDestination,
                            storageQueueNameOfSubscription, isTopic);
            Andes.getInstance().ackReceived(andesAckData);
        } catch (AndesException e) {
            log.error("Exception occurred while handling ack", e);
            throw new AMQException(AMQConstant.INTERNAL_ERROR, "Error in getting handling ack for " + messageID, e);
        }
    }

    public void rejectMessage(AMQMessage message, AMQChannel channel) throws AMQException {
        try {
            AndesMessageMetadata rejectedMessage = AMQPUtils.convertAMQMessageToAndesMetadata(message, channel.getId());
            log.debug("AMQP BRIDGE: rejected message id= " + rejectedMessage.getMessageID() + " channel = " + rejectedMessage.getChannelId());
            MessagingEngine.getInstance().messageRejected(rejectedMessage);
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
            if (log.isDebugEnabled()) {
                log.debug("AMQP BRIDGE: create AMQP Subscription subID " + subscription.getSubscriptionID() + " from queue "
                        + queue.getName());
            }

            if (subscription instanceof SubscriptionImpl.BrowserSubscription) {
                QueueBrowserDeliveryWorker deliveryWorker = new QueueBrowserDeliveryWorker(subscription, queue,
                        ((SubscriptionImpl.BrowserSubscription) subscription).getProtocolSession());
                deliveryWorker.send();
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Adding Subscription " + subscription.getSubscriptionID() + " to queue " + queue.getName());
                }
                addLocalSubscriptionsForAllBindingsOfQueue(queue, subscription);
            }

        } catch (AndesException e) {
            log.error("Error while adding the subscription", e);
            throw new AMQException(AMQConstant.INTERNAL_ERROR, "Error while registering subscription", e);
        } catch (SubscriptionAlreadyExistsException e) {
            log.error("Error occurred while adding an already existing subscription", e);
            throw new AMQQueue.ExistingExclusiveSubscription();
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
        if (log.isDebugEnabled()) {
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
        if (log.isDebugEnabled()) {
            log.debug("AMQP BRIDGE: create Exchange" + exchange.getName());
        }
        try {
            InboundExchangeEvent exchangeEvent = AMQPUtils.createAndesExchange(exchange);
            Andes.getInstance().createExchange(exchangeEvent);
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
        if (log.isDebugEnabled()) {
            log.debug("AMQP BRIDGE: delete Exchange " + exchange.getName());
        }
        try {
            Andes.getInstance().deleteExchange(AMQPUtils.createAndesExchange(exchange));
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
        if (log.isDebugEnabled()) {
            log.debug("AMQP BRIDGE: create queue: " + queue.getName());
        }
        try {
            Andes.getInstance().createQueue(AMQPUtils.createAndesQueue(queue));
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
        if (log.isDebugEnabled()) {
            log.debug("AMQP BRIDGE:  delete queue : " + queue.getName());
        }
        try {
            InboundQueueEvent queueEvent = AMQPUtils.createAndesQueue(queue);
            Andes.getInstance().deleteQueue(queueEvent);

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

        // We ignore default exchange events. Andes doesn't honor creation of AMQ default exchange bindings
        if (exchange.getNameShortString().equals(ExchangeDefaults.DEFAULT_EXCHANGE_NAME)) {
            if (log.isDebugEnabled()) {
                log.debug("Ignored binding for default exchange " + exchange.getNameShortString());
            }
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("AMQP BRIDGE: addBinding exchange=" + exchange.getName() + " routingKey=" + routingKey + " queue=" + queue.getName());
        }
        try {
            /**
             * durable topic case is handled inside qpid itself.
             * So we do not check for it here
             */
            InboundBindingEvent binding = AMQPUtils.createAndesBinding(exchange, queue, routingKey);
            Andes.getInstance().addBinding(binding);
        } catch (AndesException e) {
            log.error("error while creating binding", e);
            throw new AMQInternalException("error while removing queue", e);
        }
    }

    /**
     * remove binding from andes kernel
     *
     * @param binding           qpid binding
     * @param virtualHost virtualhost binding belongs
     * @throws AndesException
     */
    public void removeBinding(Binding binding, VirtualHost virtualHost) throws AndesException {

        Exchange exchange = binding.getExchange();

        // We ignore default exchange events. Andes doesn't honor creation of AMQ default exchange bindings
        if (exchange.getNameShortString().equals(ExchangeDefaults.DEFAULT_EXCHANGE_NAME)) {
            if (log.isDebugEnabled()) {
                log.debug("Ignored binding for default exchange " + exchange.getNameShortString());
            }
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("AMQP BRIDGE: removeBinding binding key: " + binding.getBindingKey() + " exchange: "
                    + binding.getExchange().getName() + " queue: " + binding.getQueue().getName());
        }
        InboundBindingEvent inboundBindingEvent = AMQPUtils.createAndesBinding(binding.getExchange(),
                binding.getQueue(), new AMQShortString(binding.getBindingKey()));
        Andes.getInstance().removeBinding(inboundBindingEvent);
    }

    /**
     * create local subscriptions and add for every unique binding of the queue
     *
     * @param queue        AMQ queue
     * @param subscription subscription
     * @throws AndesException
     */
    private void addLocalSubscriptionsForAllBindingsOfQueue(AMQQueue queue, Subscription subscription) throws AndesException, SubscriptionAlreadyExistsException {

        List<Binding> bindingList = queue.getBindings();
        if (bindingList != null && !bindingList.isEmpty()) {
            Set<AndesBinding> uniqueBindings = new HashSet<AndesBinding>();
            List<InboundSubscriptionEvent> alreadyAddedSubscriptions = new ArrayList<InboundSubscriptionEvent>();

            // Iterate unique bindings of the queue and add subscription entries.
            try {
                for (Binding b : bindingList) {
                    // We ignore default exchange events. Andes doesn't honor creation of AMQ default exchange bindings
                    if (b.getExchange().getNameShortString().equals(ExchangeDefaults.DEFAULT_EXCHANGE_NAME.toString())) {
                        continue;
                    }

                    AndesBinding andesBinding = AMQPUtils.createAndesBinding(b.getExchange(), b.getQueue(), new AMQShortString(b.getBindingKey()));
                    if (uniqueBindings.add(andesBinding)) {
                        InboundSubscriptionEvent localSubscription = AMQPUtils.createAMQPLocalSubscription(queue, subscription, b);
                        Andes.getInstance().openLocalSubscription(localSubscription);
                        alreadyAddedSubscriptions.add(localSubscription);
                    }
                }
            } catch (AndesException e) {
                log.warn("Reverting already created subscription entries for subscription " + subscription, e);
                for (InboundSubscriptionEvent alreadyAddedSub : alreadyAddedSubscriptions) {
                    Andes.getInstance().closeLocalSubscription(alreadyAddedSub);
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
        List<Binding> bindingList = queue.getBindings();
        if (bindingList != null && !bindingList.isEmpty()) {
            Set<AndesBinding> uniqueBindings = new HashSet<AndesBinding>();

            // Iterate unique bindings of the queue and remove subscription entries.
            for (Binding b : bindingList) {
                // We ignore default exchange events. Andes doesn't honor creation of AMQ default exchange bindings
                if (b.getExchange().getNameShortString().equals(ExchangeDefaults.DEFAULT_EXCHANGE_NAME.toString())) {
                    continue;
                }

                AndesBinding andesBinding = AMQPUtils.createAndesBinding(b.getExchange(), b.getQueue(), new AMQShortString(b.getBindingKey()));
                if (uniqueBindings.add(andesBinding)) {
                    InboundSubscriptionEvent localSubscription = AMQPUtils.createAMQPLocalSubscription(queue, subscription, b);
                    Andes.getInstance().closeLocalSubscription(localSubscription);
                }
            }
        }
    }

    /**
     * Channel closed. Clear status
     *
     * @param channelID id of the closed channel
     */
    public void channelIsClosing(UUID channelID) {
        Andes.getInstance().clientConnectionClosed(channelID);
    }

}
