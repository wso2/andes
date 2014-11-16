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

package org.wso2.andes.subscription;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.AMQException;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.LocalSubscription;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.binding.Binding;
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

    public AMQPLocalSubscription(AMQQueue amqQueue, Subscription amqpSubscription, String subscriptionID, String destination,
                                 boolean isBoundToTopic, boolean isExclusive, boolean isDurable,
                                 String subscribedNode, String targetQueue, String targetQueueOwner,
                                 String targetQueueBoundExchange, String targetQueueBoundExchangeType,
                                 Short isTargetQueueBoundExchangeAutoDeletable, boolean hasExternalSubscriptions) {

        super(subscriptionID, destination, isBoundToTopic, isExclusive, isDurable, subscribedNode, targetQueue, targetQueueOwner,
                targetQueueBoundExchange, targetQueueBoundExchangeType, isTargetQueueBoundExchangeAutoDeletable, hasExternalSubscriptions);

        this.amqQueue = amqQueue;
        this.amqpSubscription = amqpSubscription;

        if (amqpSubscription != null && amqpSubscription instanceof SubscriptionImpl.AckSubscription) {
            channel = ((SubscriptionImpl.AckSubscription) amqpSubscription).getChannel();
        }
    }

    public int getnotAckedMsgCount() {
        return channel.getNotAckedMessageCount();
    }

    public boolean isActive() {
        return amqpSubscription.isActive();
    }

    @Override
    public UUID getChannelID() {
        return channel.getId();
    }

    @Override
    public void sendMessageToSubscriber(AndesMessageMetadata messageMetadata) throws AndesException {
        AMQMessage message = AMQPUtils.getAMQMessageFromAndesMetaData(messageMetadata);
        sendAMQMessageToSubscriber(message, messageMetadata.getRedelivered());
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
    private void sendQueueEntryToSubscriber(QueueEntry message) throws AndesException {
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
            channel.incrementNonAckedMessageCount();
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
                throw new AndesException("Unexpected Subscription type");
            }
        } catch (AMQException e) {
            throw new AndesException(e);
        } catch (AndesException e) {
            if (e.getErrorCode().equals(AndesException.MESSAGE_CONTENT_OBSOLETE)) {
                if (log.isDebugEnabled()) {
                    log.debug("Message content for message id : " + msgHeaderStringID + " has " +
                            "been removed from store due to a queue purge or a previous " +
                            "acknowledgement of the message. This message won't be retried.", e);
                }
            } else {
                throw new AndesException(e);
            }
        }
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


    public LocalSubscription createQueueToListentoTopic() {
        //todo:hasitha:verify passing null values
        String subscribedNode = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();
        return new AMQPLocalSubscription(amqQueue,
                amqpSubscription, subscriptionID, targetQueue, false, isExclusive, true, subscribedNode, amqQueue.getName(),
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
