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
import org.wso2.andes.framing.abstraction.ContentChunk;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.QueueBrowserDeliveryWorker;
import org.wso2.andes.server.binding.Binding;
import org.wso2.andes.server.exchange.Exchange;
import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.server.message.ServerMessage;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.IncomingMessage;
import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.andes.server.store.StorableMessageMetaData;
import org.wso2.andes.server.subscription.Subscription;
import org.wso2.andes.server.subscription.SubscriptionImpl;
import org.wso2.carbon.andes.core.Andes;
import org.wso2.carbon.andes.core.AndesAckData;
import org.wso2.carbon.andes.core.AndesBinding;
import org.wso2.carbon.andes.core.AndesChannel;
import org.wso2.carbon.andes.core.AndesException;
import org.wso2.carbon.andes.core.AndesMessage;
import org.wso2.carbon.andes.core.AndesMessageMetadata;
import org.wso2.carbon.andes.core.AndesMessagePart;
import org.wso2.carbon.andes.core.AndesUtils;
import org.wso2.carbon.andes.core.DeliverableAndesMetadata;
import org.wso2.carbon.andes.core.DisablePubAckImpl;
import org.wso2.carbon.andes.core.MessagingEngine;
import org.wso2.carbon.andes.core.ProtocolType;
import org.wso2.carbon.andes.core.SubscriptionAlreadyExistsException;
import org.wso2.carbon.andes.core.internal.AndesContext;
import org.wso2.carbon.andes.core.internal.inbound.InboundBindingEvent;
import org.wso2.carbon.andes.core.internal.inbound.InboundExchangeEvent;
import org.wso2.carbon.andes.core.internal.inbound.InboundQueueEvent;
import org.wso2.carbon.andes.core.internal.inbound.InboundSubscriptionEvent;
import org.wso2.carbon.andes.core.internal.inbound.InboundTransactionEvent;
import org.wso2.carbon.andes.core.internal.inbound.PubAckHandler;
import org.wso2.carbon.andes.core.subscription.LocalSubscription;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Class is not instantiable from outside. This is used as a bridge between Qpid and
 * Andes. And the class doesn't store any state information hence the methods are made static
 */
public class QpidAndesBridge {

    private static Log log = LogFactory.getLog(QpidAndesBridge.class);

    /**
     * Following used by a performance counter
     */
    private static AtomicLong receivedMessageCounter = new AtomicLong();
    private static long last10kMessageReceivedTimestamp = System.currentTimeMillis();

    /**
     * Ignore pub acknowledgements in AMQP. AMQP doesn't have pub acks
     */
    private static PubAckHandler pubAckHandler;

    /**
     * Recover messages
     *
     * @param messages
     *         messages subjected to recover
     * @param channel
     * @throws AMQException
     */
    public static void recover(Collection<QueueEntry> messages, AMQChannel channel) throws AMQException {
        try {
            UUID channelID = channel.getId();
            LocalSubscription localSubscription = AndesContext.getInstance().
                    getSubscriptionEngine().getLocalSubscriptionForChannelId(channel.getId());

            if (null == localSubscription) {
                log.warn("Cannot handle recover. Subscription not found for channel " + channelID);
                return;
            }

            ArrayList<DeliverableAndesMetadata> recoveredMetadataList = new ArrayList<>(messages.size());

            for (QueueEntry message : messages) {
                ServerMessage serverMessage = message.getMessage();
                if (serverMessage instanceof AMQMessage) {
                    DeliverableAndesMetadata messageMetadata = getDeliverableAndesMetadata(localSubscription,
                                                                                           (AMQMessage) serverMessage);

                    messageMetadata.markAsNackedByClient(channel.getId());
                    recoveredMetadataList.add(messageMetadata);

                    if (log.isDebugEnabled()) {
                        log.debug("AMQP BRIDGE: recovered message id= " + messageMetadata.getMessageId() + " channel = "
                                  + channelID);
                    }

                } else {
                    log.warn("Cannot recover message since server message is not AMQMessage type. Message ID: "
                             + serverMessage.getMessageNumber());
                }
            }

            Andes.getInstance().recoverMessage(recoveredMetadataList, localSubscription);
        } catch (AndesException e) {
            throw new AMQException(AMQConstant.INTERNAL_ERROR, "Error while handling recovered message", e);
        }
    }

    /**
     * Get matching DeliverableAndesMetadata for the serverMessage
     *
     * @param localSubscription
     *         subscription used to send the message
     * @param message
     *         message
     * @return DeliverableAndesMetadata
     */
    private static DeliverableAndesMetadata getDeliverableAndesMetadata(LocalSubscription localSubscription,
                                                                        AMQMessage message) {
        return localSubscription.getMessageByMessageID(message.getMessageId());
    }

    static {
        pubAckHandler = new DisablePubAckImpl();
    }

    /**
     * Class is not instantiable from outside. This is a used as a bridge between Qpid and
     * Andes. And the class doesn't store any state information
     */
    private QpidAndesBridge() {
    }

    /**
     * message metadata received from AMQP transport.
     * This should happen after all content chunks are received
     *
     * @param incomingMessage  message coming in
     * @param channelID        id of the channel message came in
     * @param andesChannel     AndesChannel
     * @param transactionEvent not null if this is a message in a transaction, null otherwise
     * @param protocolType {@link ProtocolType}
     * @throws AMQException
     */
    public static void messageReceived(IncomingMessage incomingMessage,
                                       UUID channelID,
                                       AndesChannel andesChannel,
                                       InboundTransactionEvent transactionEvent,
                                       ProtocolType protocolType) throws AMQException {

        long receivedTime = System.currentTimeMillis();
        try {
            if (log.isDebugEnabled()) {
                log.debug("Message id " + incomingMessage.getMessageNumber() + " received from channel " + channelID);
            }
            AMQMessage message = new AMQMessage(incomingMessage.getStoredMessage());

            // message arrival time set to mb node's system time without using
            // message published time by publisher.
            message.getMessageMetaData().setArrivalTime(receivedTime);

            AndesMessageMetadata metadata = AMQPUtils.convertAMQMessageToAndesMetadata(message, protocolType);
            String queue = message.getRoutingKey();

            if (queue == null) {
                log.error("Queue cannot be null, for " + incomingMessage.getMessageNumber());
                return;
            }

            AndesMessage andesMessage = new AMQPMessage(metadata);

            // Update Andes message with all the chunk details
            int contentChunks = incomingMessage.getBodyCount();
            int offset = 0;
            for (int i = 0; i < contentChunks; i++) {
                ContentChunk chunk = incomingMessage.getContentChunk(i);
                AndesMessagePart messagePart = messageContentChunkReceived(
                                                        metadata.getMessageId(), offset, chunk.getData().buf());
                offset = offset + chunk.getSize();
                andesMessage.addMessagePart(messagePart);
            }

            // Handover message to Andes
            if(null == transactionEvent) { // not a transaction
                Andes.getInstance().messageReceived(andesMessage, andesChannel, pubAckHandler);
            } else { // transaction event
                transactionEvent.enqueue(andesMessage);
            }

        } catch (AndesException e) {
            throw new AMQException(AMQConstant.INTERNAL_ERROR, "Error while storing incoming message metadata", e);
        }

        //Following code is only a performance counter
        if(log.isDebugEnabled()) {
            Long localCount = receivedMessageCounter.incrementAndGet();
            if (localCount % 10000 == 0) {
                long timetook = System.currentTimeMillis() - last10kMessageReceivedTimestamp;
                log.debug("Received " + localCount + ", throughput = " + (10000 * 1000 / timetook) + " msg/sec, " + timetook);
                last10kMessageReceivedTimestamp = System.currentTimeMillis();
            }
        }

    }

    /**
     * read metadata of a message from store
     *
     * @param messageID id of the message
     * @return StorableMessageMetaData
     * @throws AMQException
     */
    public static StorableMessageMetaData getMessageMetaData(long messageID) throws AMQException {
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
    public static AndesMessagePart messageContentChunkReceived(long messageID, int offsetInMessage, ByteBuffer src) {

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
    public static int getMessageContentChunk(long messageID, int offsetInMessage, ByteBuffer dst) throws AMQException {
        int contentLenWritten;
        try {
            contentLenWritten = AMQPUtils.fillBufferFromContent(messageID, offsetInMessage, dst);
        } catch (AndesException e) {
            log.error("Error in getting message content", e);
            throw new AMQException(AMQConstant.INTERNAL_ERROR, "Error in getting message content chunk messageID " + messageID + " offset=" + offsetInMessage, e);
        }
        return contentLenWritten;
    }

    public static void ackReceived(UUID channelID, long messageID)
            throws AMQException {
        try {
            if (log.isDebugEnabled()) {
                log.debug("ack received for message id= " + messageID + " channelId= " + channelID);
            }
            //This can be different from routing key in hierarchical topic case
            AndesAckData andesAckData = AndesUtils.generateAndesAckMessage(channelID, messageID);
            if(null == andesAckData) {
                return;
            }
            Andes.getInstance().ackReceived(andesAckData);
        } catch (AndesException e) {
            log.error("Exception occurred while handling ack", e);
            throw new AMQException(AMQConstant.INTERNAL_ERROR, "Error in getting handling ack for " + messageID, e);
        }
    }

    /**
     * Reject message is received
     * @param message message subjected to rejection
     * @param channel channel by which reject message is received
     * @throws AMQException
     */
    public static void rejectMessage(AMQMessage message, AMQChannel channel) throws AMQException {
        try {
            LocalSubscription localSubscription = AndesContext.getInstance().
                    getSubscriptionEngine().getLocalSubscriptionForChannelId(channel.getId());
            DeliverableAndesMetadata rejectedMessage = localSubscription.getMessageByMessageID(message.getMessageId());

            channel.setLastRejectedMessageId(message.getMessageNumber());

            rejectedMessage.setIsBeyondLastRollbackedMessage(channel.isMessageBeyondLastRollback(message.getMessageId()));

            log.debug("AMQP BRIDGE: rejected message id= " + rejectedMessage.getMessageId()
                    + " channel = " + channel.getId());

            MessagingEngine.getInstance().messageRejected(rejectedMessage, channel.getId());

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
    public static void createAMQPSubscription(Subscription subscription, AMQQueue queue) throws AMQException {
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
    public static void closeAMQPSubscription(AMQQueue queue, Subscription subscription) throws AndesException {
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
    public static void createExchange(Exchange exchange) throws AMQException {
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
    public static void deleteExchange(Exchange exchange) throws AMQException {
        if (log.isDebugEnabled()) {
            log.debug("AMQP BRIDGE: delete Exchange " + exchange.getName());
        }
        try {
            Andes.getInstance().deleteExchange(AMQPUtils.createAndesExchange(exchange));
        } catch (AndesException e) {
            log.error("Error while deleting exchange", e);
            throw new AMQException(AMQConstant.INTERNAL_ERROR, "Error while deleting exchange", e);
        }
    }

    /**
     * Create queue in andes kernel
     *
     * @param queue qpid queue
     * @throws AMQException
     */
    public static void createQueue(AMQQueue queue) throws AMQException {
        if (log.isDebugEnabled()) {
            log.debug("AMQP BRIDGE: create queue: " + queue.getName());
        }
        try {
            List<String> queues = AndesContext.getInstance().getAmqpConstructStore().getQueueNames();
            for (String queueName : queues) {
                if (queueName.equalsIgnoreCase(queue.getName())) {
                    throw new AMQException("Cannot create already existing queue: " + queue.getName());
                }
            }
            Andes.getInstance().createQueue(AMQPUtils.createAndesQueue(queue));
        } catch (AndesException e) {
            log.error("Error while creating queue", e);
            throw new AMQException(AMQConstant.INTERNAL_ERROR, "Error while creating queue", e);
        }
    }

    /**
     * Delete queue from andes kernel
     *
     * @param queue qpid queue
     * @throws AMQException
     */
    public static void deleteQueue(AMQQueue queue) throws AMQException {
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
     * @throws AMQInternalException
     */
    public static void createBinding(Exchange exchange, AMQShortString routingKey,
                              AMQQueue queue) throws AMQInternalException {

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
     * @throws AndesException
     */
    public static void removeBinding(Binding binding) throws AndesException {

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
    private static void addLocalSubscriptionsForAllBindingsOfQueue(AMQQueue queue, Subscription subscription) throws AndesException, SubscriptionAlreadyExistsException {

        List<Binding> bindingList = queue.getBindings();
        if (bindingList != null && !bindingList.isEmpty()) {
            Set<AndesBinding> uniqueBindings = new HashSet<AndesBinding>();
            List<InboundSubscriptionEvent> alreadyAddedSubscriptions = new ArrayList<>();

            // Iterate unique bindings of the queue and add subscription entries.
            try {
                for (Binding b : bindingList) {
                    // We ignore default exchange events. Andes doesn't honor creation of AMQ default exchange bindings
                    if (b.getExchange().getNameShortString().equals(ExchangeDefaults.DEFAULT_EXCHANGE_NAME.toString())) {
                        continue;
                    }

                    AndesBinding andesBinding = AMQPUtils.createAndesBinding(b.getExchange(), b.getQueue(), new AMQShortString(b.getBindingKey()));
                    if (uniqueBindings.add(andesBinding)) {
                        LocalSubscription localSubscription = AMQPUtils.createAMQPLocalSubscription(queue, subscription, b);
                        InboundSubscriptionEvent subscriptionEvent = new InboundSubscriptionEvent(localSubscription);
                        Andes.getInstance().openLocalSubscription(subscriptionEvent);
                        alreadyAddedSubscriptions.add(subscriptionEvent);
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
    private static void closeLocalSubscriptionsForAllBindingsOfQueue(AMQQueue queue, Subscription subscription) throws AndesException {
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
                    UUID channelID = ((SubscriptionImpl) subscription).getChannel().getId();
                    LocalSubscription localSubscription = AndesContext.getInstance().getSubscriptionEngine()
                            .getLocalSubscriptionForChannelId(channelID);
                    localSubscription.setHasExternalSubscriptions(subscription.isActive());
                    localSubscription.setExclusive(((SubscriptionImpl) subscription).isExclusive());
                    InboundSubscriptionEvent subscriptionEvent = new InboundSubscriptionEvent(localSubscription);
                    Andes.getInstance().closeLocalSubscription(subscriptionEvent);
                }
            }
        }
    }

    /**
     * Channel closed. Clear status
     *
     * @param channelID id of the closed channel
     */
    public static void channelIsClosing(UUID channelID) {
        Andes.getInstance().clientConnectionClosed(channelID);
    }

}
