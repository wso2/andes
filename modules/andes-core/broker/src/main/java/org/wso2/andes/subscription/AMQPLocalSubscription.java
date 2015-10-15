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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.AMQException;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.kernel.AMQPDeliveryRule;
import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesAckData;
import org.wso2.andes.kernel.AndesContent;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesUtils;
import org.wso2.andes.kernel.HasInterestRuleAMQP;
import org.wso2.andes.kernel.MaximumNumOfDeliveryRuleAMQP;
import org.wso2.andes.kernel.NoLocalRuleAMQP;
import org.wso2.andes.kernel.ProtocolDeliveryFailureException;
import org.wso2.andes.kernel.ProtocolDeliveryRulesFailureException;
import org.wso2.andes.kernel.ProtocolMessage;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.andes.server.subscription.Subscription;
import org.wso2.andes.server.subscription.SubscriptionImpl;
import org.wso2.andes.tools.utils.MessageTracer;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


/**
 * This class represents a AMQP subscription locally created
 * This class has info and methods to deal with qpid AMQP transports and
 * send messages to the subscription
 */
public class AMQPLocalSubscription implements OutboundSubscription {

    private static Log log = LogFactory.getLog(AMQPLocalSubscription.class);

    //AMQP transport channel subscriber is dealing with
    AMQChannel channel = null;

    //internal qpid queue subscription is bound to
    private AMQQueue amqQueue;

    //internal qpid subscription
    private Subscription amqpSubscription;

    //if this subscription is a durable one
    private boolean isDurable;

    //if this subscription represent a topic subscription
    private  boolean isBoundToTopic;

    //List of Delivery Rules to evaluate
    private List<AMQPDeliveryRule> AMQPDeliveryRulesList = new ArrayList<>();


    public AMQPLocalSubscription(AMQQueue amqQueue, Subscription amqpSubscription, boolean isDurable, boolean
            isBoundToTopic) {

        this.amqQueue = amqQueue;
        this.amqpSubscription = amqpSubscription;

        if (amqpSubscription != null && amqpSubscription instanceof SubscriptionImpl) {
            channel = ((SubscriptionImpl) amqpSubscription).getChannel();
            initializeDeliveryRules();
        }

        this.isDurable = isDurable;
        this.isBoundToTopic = isBoundToTopic;
    }

    /**
     * Initializing Delivery Rules
     */
    private void initializeDeliveryRules() {

        //checking counting delivery rule
        if (  (! isBoundToTopic) || isDurable){ //evaluate this only for queues and durable subscriptions
            AMQPDeliveryRulesList.add(new MaximumNumOfDeliveryRuleAMQP(channel));
        }
        //checking has interest delivery rule
        AMQPDeliveryRulesList.add(new HasInterestRuleAMQP(amqpSubscription));
        //checking no local delivery rule
        AMQPDeliveryRulesList.add(new NoLocalRuleAMQP(amqpSubscription, channel));
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
    public boolean sendMessageToSubscriber(ProtocolMessage messageMetadata, AndesContent content)
            throws AndesException {

        AMQMessage message = AMQPUtils.getAMQMessageForDelivery(messageMetadata, content);
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

        for (AMQPDeliveryRule element : AMQPDeliveryRulesList) {
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
                      + protocolMessage.getMessage().getSlot().toString(), e);
            throw new ProtocolDeliveryFailureException(
                    "Error occurred while delivering message with ID : " + msgHeaderStringID, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getStorageQueueName(String destination, String subscribedNode) {

        String storageQueueName;

        String targetQueue = amqQueue.getName();

        if (isBoundToTopic && !isDurable) {  // for normal topic subscriptions
            storageQueueName = AndesUtils.getStorageQueueForDestination(destination, subscribedNode, true);
        } else if (isBoundToTopic) {  //for durable topic subscriptions
            storageQueueName = AndesUtils.getStorageQueueForDestination(targetQueue, subscribedNode, false);
        } else { //For queue subscriptions. This is a must. Otherwise queue will not be shared among nodes
            storageQueueName = AndesUtils.getStorageQueueForDestination(targetQueue, subscribedNode, false);
        }

        return storageQueueName;
    }
}
