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

package org.wso2.andes.subscription;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.AMQException;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.kernel.AndesContent;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.DeliveryRule;
import org.wso2.andes.kernel.HasInterestRule;
import org.wso2.andes.kernel.LocalSubscription;
import org.wso2.andes.kernel.MaximumNumOfDeliveryRule;
import org.wso2.andes.kernel.MessageStatus;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.kernel.NoLocalRule;
import org.wso2.andes.kernel.OnflightMessageTracker;
import org.wso2.andes.kernel.disruptor.inbound.InboundSubscriptionEvent;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.exchange.DirectExchange;
import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.andes.server.subscription.Subscription;
import org.wso2.andes.server.subscription.SubscriptionImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * This class represents a AMQP subscription locally created
 * This class has info and methods to deal with qpid AMQP transports and
 * send messages to the subscription
 */
public class AMQPLocalSubscription extends InboundSubscriptionEvent {

    private static Log log = LogFactory.getLog(AMQPLocalSubscription.class);
    //AMQP transport channel subscriber is dealing with
    AMQChannel channel = null;
    //internal qpid queue subscription is bound to
    private AMQQueue amqQueue;
    //internal qpid subscription
    private Subscription amqpSubscription;
    /**
     * Whether subscription is bound to topic or not
     */
    private boolean isBoundToTopic;
    /**
     * Whether subscription is durable or not
     */
    private boolean isDurable;
    /**
     * OnflightMessageTracker stores message information, using it to get message information
     */
    private OnflightMessageTracker onflightMessageTracker;
    /**
     * List of Delivery Rules to evaluate
     */
    private List<DeliveryRule> deliveryRulesList = new ArrayList<DeliveryRule>();

    public AMQPLocalSubscription(AMQQueue amqQueue, Subscription amqpSubscription, String subscriptionID, String destination,
                                 boolean isBoundToTopic, boolean isExclusive, boolean isDurable,
                                 String subscribedNode, long subscribeTime, String targetQueue, String targetQueueOwner,
                                 String targetQueueBoundExchange, String targetQueueBoundExchangeType,
                                 Short isTargetQueueBoundExchangeAutoDeletable, boolean hasExternalSubscriptions) {

        super(subscriptionID, destination, isBoundToTopic, isExclusive, isDurable, subscribedNode, subscribeTime, targetQueue, targetQueueOwner,
                targetQueueBoundExchange, targetQueueBoundExchangeType, isTargetQueueBoundExchangeAutoDeletable, hasExternalSubscriptions);

        setSubscriptionType(SubscriptionType.AMQP);

        this.amqQueue = amqQueue;
        this.amqpSubscription = amqpSubscription;
        this.isBoundToTopic = isBoundToTopic;
        this.isDurable = isDurable;
        onflightMessageTracker = OnflightMessageTracker.getInstance();

        if (amqpSubscription != null && amqpSubscription instanceof SubscriptionImpl.AckSubscription) {
            channel = ((SubscriptionImpl.AckSubscription) amqpSubscription).getChannel();
            initializeDeliveryRules();
        }
    }

    /**
     * Initializing Delivery Rules
     */
    private void initializeDeliveryRules() {

        //checking counting delivery rule
        //evaluate this only for queues and durable subscriptions
        if (!isBoundToTopic || isDurable) {
            deliveryRulesList.add(new MaximumNumOfDeliveryRule(channel));
        }
        // NOTE: Feature Message Expiration moved to a future release
//        //checking message expiration deliver rule
//        deliveryRulesList.add(new MessageExpiredRule());

//        //checking message purged delivery rule
//        deliveryRulesList.add(new MessagePurgeRule());
        //checking has interest delivery rule
        deliveryRulesList.add(new HasInterestRule(amqpSubscription));
        //checking no local delivery rule
        deliveryRulesList.add(new NoLocalRule(amqpSubscription, channel));
    }

    public boolean isActive() {
        return amqpSubscription.isActive();
    }

    @Override
    public UUID getChannelID() {
        return channel.getId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendMessageToSubscriber(AndesMessageMetadata messageMetadata, AndesContent content)
            throws AndesException {
        AMQMessage message = AMQPUtils.getAMQMessageForDelivery(messageMetadata, content);
        QueueEntry messageToSend = AMQPUtils.convertAMQMessageToQueueEntry(message, amqQueue);
        if (messageMetadata.getRedelivered()) {
            messageToSend.setRedelivered();
        }
        if (evaluateDeliveryRules(messageToSend)) {
            sendAMQMessageToSubscriber(message, messageToSend);
        } else {
            rejectMessageAndMoveToDLC(messageMetadata);
        }
    }

    /**
     * Reject message and move to dead letter channel
     *
     * @param messageMetadata message metadata to be rejected
     */
    private void rejectMessageAndMoveToDLC(AndesMessageMetadata messageMetadata) throws AndesException {
        //Set message status to reject
        onflightMessageTracker.setMessageStatus(MessageStatus.DELIVERY_REJECT, messageMetadata.getMessageID());
        /**
         * Message tracker rejected this message from sending. Hence moving
         * to dead letter channel
         */
        // Move message to DLC
        // All the Queues and Durable Topics related messages are adding to DLC
        if (!isBoundToTopic || isDurable) {
            MessagingEngine.getInstance().moveMessageToDeadLetterChannel(messageMetadata, messageMetadata
                    .getDestination());
        }
    }

    /**
     * send message to the internal subscription
     *
     * @param message message to send
     * @throws AndesException
     */
    private void sendAMQMessageToSubscriber(AMQMessage message, QueueEntry messageToSend) throws AndesException {
        int numOfDeliveriesOfCurrentMsg =
                onflightMessageTracker.getNumOfMsgDeliveriesForChannel(message.getMessageId(), channel.getId());

        onflightMessageTracker.setMessageStatus(MessageStatus.DELIVERY_OK, message.getMessageId());
        if (numOfDeliveriesOfCurrentMsg == 1) {
            onflightMessageTracker.setMessageStatus(MessageStatus.SENT, message.getMessageId());
        } else if (numOfDeliveriesOfCurrentMsg > 1) {
            onflightMessageTracker.setMessageStatus(MessageStatus.RESENT, message.getMessageId());
        }
        sendQueueEntryToSubscriber(messageToSend);
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

        for (DeliveryRule element : deliveryRulesList) {
            if (!element.evaluate(message)) {
                isOKToDelivery = false;
                break;
            }
        }
        return isOKToDelivery;
    }

    /**
     * validate routing keys of messages and send
     *
     * @param message message to send
     * @throws AndesException
     */
    public void sendQueueEntryToSubscriber(QueueEntry message) throws AndesException {
        sendMessage(message);
    }

    /**
     * write message to channel
     *
     * @param queueEntry message to send
     * @throws AndesException
     */
    private void sendMessage(QueueEntry queueEntry) throws AndesException {

        String msgHeaderStringID = "";
        Long messageNumber = null;

        if (queueEntry != null) {
            msgHeaderStringID = (String) queueEntry.getMessageHeader().
                    getHeader("msgID");
            messageNumber = queueEntry.getMessage().getMessageNumber();
        }

        try {

            // TODO: We might have to carefully implement this in every new subscription type we implement
            // shall we move this up to LocalSubscription level?
            onflightMessageTracker.incrementNonAckedMessageCount(channel.getId());

            if (amqpSubscription instanceof SubscriptionImpl.AckSubscription) {
                //this check is needed to detect if subscription has suddenly closed
                if (log.isDebugEnabled()) {
                    log.debug("TRACING>> QDW- sent queue/durable topic message " +
                            msgHeaderStringID + " messageID-" + messageNumber + "-to " +
                            "subscription " + amqpSubscription);
                }
                amqpSubscription.send(queueEntry);
            } else {
                throw new AndesException("Unexpected Subscription type for message with ID : " + msgHeaderStringID);
            }
        } catch (AMQException e) {
            // The error is not logged here since this will be caught safely higher up in the execution plan :
            // MessageFlusher.deliverAsynchronously. If we have more context, its better to log here too,
            // but since this is a general explanation of many possible errors, no point in logging at this state.
            throw new AndesException("Error occurred while delivering message with ID : " + msgHeaderStringID, e);
        } catch (AndesException e) {
            throw new AndesException("Error occurred while delivering message with ID : " + msgHeaderStringID, e);
        }
    }


    public LocalSubscription createQueueToListentoTopic() {
        //todo:hasitha:verify passing null values
        String subscribedNode = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();
        return new AMQPLocalSubscription(amqQueue,
                amqpSubscription, subscriptionID, targetQueue, false, isExclusive, true, subscribedNode, System.currentTimeMillis(), amqQueue.getName(),
                amqQueue.getOwner().toString(), AMQPUtils.DIRECT_EXCHANGE_NAME, DirectExchange.TYPE.toString(), Short.parseShort("0"), true);
    }

    public boolean equals(Object o) {
        if (o instanceof AMQPLocalSubscription) {
            AMQPLocalSubscription c = (AMQPLocalSubscription) o;
            if (this.subscriptionID.equals(c.subscriptionID) &&
                    this.getSubscribedNode().equals(c.getSubscribedNode()) &&
                    this.targetQueue.equals(c.targetQueue) &&
                    this.targetQueueBoundExchange.equals(c.targetQueueBoundExchange)) {
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        return new HashCodeBuilder(17, 31).
                append(subscriptionID).
                append(getSubscribedNode()).
                append(targetQueue).
                append(targetQueueBoundExchange).
                toHashCode();
    }
}
