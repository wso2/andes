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
import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesAckData;
import org.wso2.andes.kernel.AndesContent;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesUtils;
import org.wso2.andes.kernel.ProtocolDeliveryFailureException;
import org.wso2.andes.kernel.ProtocolDeliveryRulesFailureException;
import org.wso2.andes.kernel.ProtocolMessage;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.binding.Binding;
import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.server.message.MessageMetaData;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.andes.server.store.StoredMessage;
import org.wso2.andes.server.subscription.Subscription;
import org.wso2.andes.server.subscription.SubscriptionImpl;
import org.wso2.andes.kernel.subscription.OutboundSubscription;
import org.wso2.andes.tools.utils.MessageTracer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


/**
 * This class represents a AMQP subscription locally created
 * This class has info and methods to deal with qpid AMQP transports and
 * send messages to the subscription
 */
public class AMQPLocalSubscription implements OutboundSubscription {

    private static Log log = LogFactory.getLog(AMQPLocalSubscription.class);

    //AMQP transport channel subscriber is dealing with
    private AMQChannel channel = null;

    //internal qpid queue subscription is bound to
    private AMQQueue amqQueue;

    //internal qpid subscription
    private Subscription amqpSubscription;

    //if this subscription is bound to a durable queue
    private boolean isDurable;

    //time when subscriber is created
    private long subscribeTime;

    /*
     * This map works as a cache for queue entries, preventing need to convert
     * DeliverableAndesMetadata to queue entries two times
     */
    private Map<Long, StoredMessage<MessageMetaData>> storedMessageCache;

    //List of Delivery Rules to evaluate
    private List<AMQPDeliveryRule> amqpDeliveryRuleList = new ArrayList<>();


    public AMQPLocalSubscription(Subscription amqpSubscription) {
        this.subscribeTime = System.currentTimeMillis();
        this.amqQueue = amqpSubscription.getQueue();
        this.isDurable = amqpSubscription.getQueue().isDurable();
        this.amqpSubscription = amqpSubscription;

        if (amqpSubscription != null && amqpSubscription instanceof SubscriptionImpl) {
            channel = ((SubscriptionImpl) amqpSubscription).getChannel();
            initializeDeliveryRules();
        }

        //We leave the default values for initialCapacity and progression factor
        //We re define concurrencyLevel as 2, since there will be only 2 threads which accesses it concurrently
        this.storedMessageCache = new ConcurrentHashMap<>(16,0.75f,2);
    }

    /**
     * Initializing Delivery Rules
     */
    private void initializeDeliveryRules() {

        List<Binding> bindings = amqpSubscription.getQueue().getBindings();
        boolean isBoundToTopic = false;
        for (Binding binding : bindings) {
            if(binding.getExchange().getName().equals(AMQPUtils.TOPIC_EXCHANGE_NAME)) {
                isBoundToTopic = true;
                break;
            }
        }
        //checking counting delivery rule
        if (  (! isBoundToTopic) || amqpSubscription.getQueue().isDurable()) { //evaluate this only for queues and
            // durable
            // subscriptions
            amqpDeliveryRuleList.add(new MaximumNumOfDeliveryRule());
        }

        //checking no local delivery rule
        amqpDeliveryRuleList.add(new AmqpNoLocalRule(amqpSubscription, channel));

    }

    public boolean isOutboundConnectionLive() {
        return amqpSubscription.isActive();
    }

    @Override
    public UUID getChannelID() {
        return channel.getId();
    }

    @Override
    public long getSubscribeTime() {
        return subscribeTime;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void forcefullyDisconnect() throws AndesException {
        try {
            channel.mgmtClose();
        } catch (Exception e) {
            throw new AndesException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isMessageAcceptedBySelector(AndesMessageMetadata messageMetadata)
            throws AndesException {

        AMQMessage amqMessage = AMQPUtils.getAMQMessageFromAndesMetaData(messageMetadata);
        QueueEntry message = AMQPUtils.convertAMQMessageToQueueEntry(amqMessage, amqQueue);

        if(amqpSubscription.hasInterest(message)) {
            storedMessageCache.put(message.getMessage().getMessageNumber(), amqMessage.getStoredMessage());
            return true;
        } else {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean sendMessageToSubscriber(ProtocolMessage messageMetadata, AndesContent content)
            throws AndesException {

        StoredMessage<MessageMetaData> cachedStoredMessage = storedMessageCache.get(messageMetadata.getMessageID());

        AMQMessage message;

        if(null != cachedStoredMessage) {
            message = AMQPUtils.getQueueEntryFromStoredMessage(cachedStoredMessage, content);
            storedMessageCache.remove(messageMetadata.getMessageID());
            message.setAndesMetadataReference(messageMetadata);
        } else {
            message = AMQPUtils.getAMQMessageForDelivery(messageMetadata, content);
        }

        QueueEntry messageToSend = AMQPUtils.convertAMQMessageToQueueEntry(message, amqQueue);

        if (evaluateDeliveryRules(messageToSend)) {
            //check if redelivered. If so, set the JMS header
            if(messageMetadata.isRedelivered()) {
                messageToSend.setRedelivered();
            }
            sendMessage(messageToSend);

        } else {
            throw new ProtocolDeliveryRulesFailureException("AMQP delivery rule evaluation failed");
        }

        return true;
    }

    /**
     * Evaluating Delivery rules before sending the messages
     *
     * @param message AMQ Message
     * @return IsOKToDelivery
     * @throws AndesException
     */
    private boolean evaluateDeliveryRules(QueueEntry message) throws AndesException {
        boolean isOKToDelivery = true;

        for (AMQPDeliveryRule element : amqpDeliveryRuleList) {
            if (!element.evaluate(message)) {
                isOKToDelivery = false;
                break;
            }
        }
        return isOKToDelivery;
    }


    /**
     * write message to channel
     *
     * @param queueEntry message to send
     * @throws AndesException
     */
    private void sendMessage(QueueEntry queueEntry) throws AndesException {

        String msgHeaderStringID = (String) queueEntry.getMessageHeader().getHeader("msgID");
        Long messageID = queueEntry.getMessage().getMessageNumber();

        try {

            if (amqpSubscription instanceof SubscriptionImpl.AckSubscription) {

                MessageTracer.trace(messageID, "",
                                    "Sending message " + msgHeaderStringID + " messageID-" + messageID + "-to channel "
                                    + getChannelID());

                amqpSubscription.send(queueEntry);
            } else if (amqpSubscription instanceof SubscriptionImpl.NoAckSubscription) {
                MessageTracer.trace(messageID, "",
                                    "Sending message " + msgHeaderStringID + " messageID-" + messageID + "-to channel "
                                    + getChannelID());

                amqpSubscription.send(queueEntry);

                // After sending message we simulate acknowledgment for NoAckSubscription
                UUID channelID = ((SubscriptionImpl.NoAckSubscription) amqpSubscription).getChannel().getId();
                AndesAckData andesAckData = AndesUtils.generateAndesAckMessage(channelID, messageID);

                Andes.getInstance().ackReceived(andesAckData);
            } else {
                throw new AndesException("Error occurred while delivering message. Unexpected Subscription type for "
                        + "message with ID : " + msgHeaderStringID);
            }
        } catch (AMQException e) {
            // The error is not logged here since this will be caught safely higher up in the execution plan :
            // MessageFlusher.deliverAsynchronously. If we have more context, its better to log here too,
            // but since this is a general explanation of many possible errors, no point in logging at this state.
            ProtocolMessage protocolMessage = ((AMQMessage) queueEntry.getMessage()).getAndesMetadataReference();
            log.error("AMQP Protocol Error while delivering message to the subscriber subID= "
                      + amqpSubscription.getSubscriptionID() + " message id= " + messageID + " slot= "
                      + protocolMessage.getMessage().getSlot().getId(), e);
            throw new ProtocolDeliveryFailureException(
                    "Error occurred while delivering message with ID : " + msgHeaderStringID, e);
        }
    }
}
