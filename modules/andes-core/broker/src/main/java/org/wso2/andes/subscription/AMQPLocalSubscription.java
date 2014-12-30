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
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesChannel;
import org.wso2.andes.kernel.AndesContent;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.DeliverableAndesMessageMetadata;
import org.wso2.andes.kernel.LocalSubscription;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.binding.Binding;
import org.wso2.andes.server.cassandra.MessageFlusher;
import org.wso2.andes.server.exchange.DirectExchange;
import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.andes.server.subscription.Subscription;
import org.wso2.andes.server.subscription.SubscriptionImpl;

import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class represents a AMQP subscription locally created
 * This class has info and methods to deal with qpid AMQP transports and
 * send messages to the subscription
 */
public class AMQPLocalSubscription extends BasicSubscription implements LocalSubscription {
    private static Log log = LogFactory.getLog(AMQPLocalSubscription.class);

    //internal qpid queue subscription is bound to
    private AMQQueue amqQueue;

    //internal qpid subscription
    private Subscription amqpSubscription;

    //AMQP transport channel subscriber is dealing with
    AMQChannel channel = null;

    private Integer maximumRedeliveryTimes = 1;

    public AMQPLocalSubscription(AMQQueue amqQueue, Subscription amqpSubscription, String subscriptionID, String destination,
                                 boolean isBoundToTopic, boolean isExclusive, boolean isDurable,
                                 String subscribedNode, long subscribeTime, String targetQueue, String targetQueueOwner,
                                 String targetQueueBoundExchange, String targetQueueBoundExchangeType,
                                 Short isTargetQueueBoundExchangeAutoDeletable, boolean hasExternalSubscriptions) {

        super(subscriptionID, destination, isBoundToTopic, isExclusive, isDurable, subscribedNode, subscribeTime, targetQueue, targetQueueOwner,
                targetQueueBoundExchange, targetQueueBoundExchangeType, isTargetQueueBoundExchangeAutoDeletable, hasExternalSubscriptions);

        this.amqQueue = amqQueue;
        this.amqpSubscription = amqpSubscription;

        if (amqpSubscription != null && amqpSubscription instanceof SubscriptionImpl.AckSubscription) {
            channel = ((SubscriptionImpl.AckSubscription) amqpSubscription).getChannel();
        }

        this.maximumRedeliveryTimes = AndesConfigurationManager.readValue
                        (AndesConfiguration.TRANSPORTS_AMQP_MAXIMUM_REDELIVERY_ATTEMPTS);
    }

    public boolean isActive() {
        return amqpSubscription.isActive();
    }

    @Override
    public long getChannelID() {
        return channel.getAndesChannel().getChannelID();
    }

    @Override
    public AndesChannel getAndesChannel() {
        return channel.getAndesChannel();
    }

    @Override
    public boolean isSuspended() {
        return channel.getAndesChannel().isChannelSuspended();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean sendMessageToSubscriber(DeliverableAndesMessageMetadata messageMetadata, AndesContent content)
            throws AndesException {
        boolean isToSend = evaluateDeliveryRules(messageMetadata);
        if(isToSend) {
            AMQMessage message = AMQPUtils.getAMQMessageForDelivery(messageMetadata, content);
            long IDOfChannel = channel.getAndesChannel().getChannelID();
            boolean isRedeliveryToChannel = messageMetadata.isRedeliveredToChannel(IDOfChannel);
            sendAMQMessageToSubscriber(message, isRedeliveryToChannel);
        }
        return isToSend;
    }

    /**
     * send message to the internal subscription
     *
     * @param message      message to send
     * @param isRedelivery is a redelivered message
     * @throws AndesException
     */
    private void sendAMQMessageToSubscriber(AMQMessage message, boolean isRedelivery) throws AndesException {
        QueueEntry messageToSend = AMQPUtils.convertAMQMessageToQueueEntry(message, amqQueue);
        if (isRedelivery) {
            //set redelivery header
            messageToSend.setRedelivered();
        }
        sendQueueEntryToSubscriber(messageToSend);
    }

    /**
     * validate routing keys of messages and send
     *
     * @param message message to send
     * @throws AndesException
     */
    public void sendQueueEntryToSubscriber(QueueEntry message) throws AndesException {
        if (message.getQueue().checkIfBoundToTopicExchange()) {
            //topic messages should be sent to all matching subscriptions
            String routingKey = message.getMessage().getRoutingKey();
            for (Binding bindingOfQueue : amqQueue.getBindings()) {
                if (isMatching(bindingOfQueue.getBindingKey(), routingKey)) {
                    sendMessage(message);
                    break;
                }
            }
        } else {
            sendMessage(message);
        }

    }

    /**
     * write message to channel
     *
     * @param queueEntry message to send
     * @throws AndesException
     */
    private void sendMessage(QueueEntry queueEntry) throws AndesException {

        String msgHeaderStringID = "";

        if (queueEntry != null) {
            msgHeaderStringID = (String) queueEntry.getMessageHeader().
                    getHeader("msgID");
        }

        try {
            AMQProtocolSession session = channel.getProtocolSession();
            ((AMQMessage) queueEntry.getMessage()).setClientIdentifier(session);

            if (amqpSubscription instanceof SubscriptionImpl.AckSubscription) {
                //this check is needed to detect if subscription has suddenly closed
                if (log.isDebugEnabled()) {
                    log.debug("TRACING>> QDW- sent queue/durable topic message " +
                            (msgHeaderStringID == null ? "" : msgHeaderStringID + " messageID-" +
                                    queueEntry.getMessage().getMessageNumber()) + "-to " +
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

    private boolean evaluateDeliveryRules(DeliverableAndesMessageMetadata messageToDeliver)
            throws AndesException {
        boolean isOKToDeliver = true;
        long messageId = messageToDeliver.getMessageID();
        String messageDestination = messageToDeliver.getDestination();
        //check if number of redelivery tries has breached.
        int numOfDeliveriesOfCurrentMsg = messageToDeliver.getNumOfDeliveires4Channel(getChannelID());

        // Get last purged timestamp of the destination queue.
        long lastPurgedTimestampOfQueue = MessageFlusher.getInstance().getMessageDeliveryInfo(
                messageDestination).getLastPurgedTimestamp();

        if (numOfDeliveriesOfCurrentMsg > maximumRedeliveryTimes) {

            log.warn("Number of Maximum Redelivery Tries Has Breached. Routing Message to DLC : id= " + messageId);
            isOKToDeliver = false;

            //check if destination entry has expired. Any expired message will not be delivered
        } else if (messageToDeliver.isExpired()) {

            log.warn("Message is expired. Routing Message to DLC : id= " + messageId);
            isOKToDeliver = false;

        } else if (messageToDeliver.getArrivalTime() <= lastPurgedTimestampOfQueue) {

            log.warn("Message was sent at " + messageToDeliver.getArrivalTime() + " before last purge event at "
                     + lastPurgedTimestampOfQueue + ". Will be skipped. id= " + messageId);
            messageToDeliver.setMessageStatus(DeliverableAndesMessageMetadata.MessageStatus.PURGED);
            isOKToDeliver = false;
        }
        if (isOKToDeliver) {
            messageToDeliver.setMessageStatus(
                    DeliverableAndesMessageMetadata.MessageStatus.DELIVERY_OK);
        } else {
            messageToDeliver.setMessageStatus(
                    DeliverableAndesMessageMetadata.MessageStatus.DELIVERY_REJECT);
        }
        return isOKToDeliver;
    }

    /**
     * perform AMQP specific routing key match
     *
     * @param binding binding key
     * @param topic   binding key to match
     * @return is matching
     */
    private boolean isMatching(String binding, String topic) {
        boolean isMatching = false;
        if (binding.equals(topic)) {
            isMatching = true;
        } else if (binding.indexOf(".#") > 1) {
            String p = binding.substring(0, binding.indexOf(".#"));
            Pattern pattern = Pattern.compile(p + ".*");
            Matcher matcher = pattern.matcher(topic);
            isMatching = matcher.matches();
        } else if (binding.indexOf(".*") > 1) {
            String p = binding.substring(0, binding.indexOf(".*"));
            Pattern pattern = Pattern.compile("^" + p + "[.][^.]+$");
            Matcher matcher = pattern.matcher(topic);
            isMatching = matcher.matches();
        }
        return isMatching;
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
