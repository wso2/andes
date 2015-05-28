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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel.distruptor.delivery;

import com.lmax.disruptor.EventHandler;
import org.apache.log4j.Logger;
import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesRemovableMetadata;
import org.wso2.andes.kernel.LocalSubscription;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.kernel.MessageFlusher;
import org.wso2.andes.kernel.OnflightMessageTracker;
import org.wso2.andes.matrics.MetricsConstants;
import org.wso2.carbon.metrics.manager.Level;
import org.wso2.carbon.metrics.manager.Meter;
import org.wso2.carbon.metrics.manager.MetricManager;

import java.util.ArrayList;
import java.util.List;

/**
 * Disruptor handler used to send the message. This the final event handler of the ring-buffer
 */
public class DeliveryEventHandler implements EventHandler<DeliveryEventData> {
    /**
     * Class logger
     */
    private static final Logger log = Logger.getLogger(DeliveryEventHandler.class);

    /**
     * Used to identify the subscribers that need to be processed by this handler
     */
    private final long ordinal;

    /**
     * Total number of DeliveryEventHandler
     */
    private final long numberOfConsumers;

    public DeliveryEventHandler(long ordinal, long numberOfHandlers) {
        this.ordinal = ordinal;
        this.numberOfConsumers = numberOfHandlers;
    }

    /**
     * Send message to subscriber
     *
     * @param deliveryEventData
     *         Event data holder
     * @param sequence
     *         Sequence number of the disruptor event
     * @param endOfBatch
     *         Indicate end of batch
     * @throws Exception
     */
    @Override
    public void onEvent(DeliveryEventData deliveryEventData, long sequence, boolean endOfBatch) throws Exception {
        LocalSubscription subscription = deliveryEventData.getLocalSubscription();
        
        // Taking the absolute value since hashCode can be a negative value
        long channelModulus = Math.abs(subscription.getChannelID().hashCode() % numberOfConsumers);

        // Filter tasks assigned to this handler
        if (channelModulus == ordinal) {
            AndesMessageMetadata message = deliveryEventData.getMetadata();

            try {
                //decrement number of schedule deliveries before send to subscriber to avoid parallel status update issues
                OnflightMessageTracker.getInstance().decrementNumberOfScheduledDeliveries(message.getMessageID());
                if (deliveryEventData.isErrorOccurred()) {
                    handleSendError(message);
                    return;
                }
                if (subscription.isActive()) {
                    subscription.sendMessageToSubscriber(message, deliveryEventData.getAndesContent());

                    //Adding metrics meter for ack rate
                    Meter msgMeter = MetricManager.meter(Level.INFO, this.getClass() + MetricsConstants.MSG_SENT_RATE);
                    msgMeter.mark();

                } else {
                    //destination would be target queue if it is durable topic, otherwise it is queue or non durable topic
                    if(subscription.isBoundToTopic() && subscription.isDurable()){
                        message.setDestination(subscription.getTargetQueue());
                    } else {
                        message.setDestination(subscription.getSubscribedDestination());
                    }
                    MessageFlusher.getInstance().reQueueUndeliveredMessagesDueToInactiveSubscriptions(message);
                }
            } catch (Throwable e) {
                log.error("Error while delivering message. Message id " + message.getMessageID(), e);
                //increment above schedule count because exception occurred while send message to subscriber
                OnflightMessageTracker.getInstance().incrementNumberOfScheduledDeliveries(message.getMessageID());
                handleSendError(message);
            } finally {
                deliveryEventData.clearData();
            }
        }
    }

    /**
     * When an error is occurred in message delivery, this method will move the message to dead letter channel.
     *
     * @param message
     *         Meta data for the message
     */
    private void handleSendError(AndesMessageMetadata message) {
        // If message is a queue message we move the message to the Dead Letter Channel
        // since topics doesn't have a Dead Letter Channel
        if (!message.isTopic()) {
            log.info("Moving message to Dead Letter Channel. Message ID " + message.getMessageID());
            AndesRemovableMetadata removableMessage = new AndesRemovableMetadata(message.getMessageID(),
                                                                                 message.getDestination(),
                                                                                 message.getStorageQueueName());
            List<AndesRemovableMetadata> messageToMoveToDLC = new ArrayList<AndesRemovableMetadata>();
            messageToMoveToDLC.add(removableMessage);
            try {
                Andes.getInstance().deleteMessages(messageToMoveToDLC, true);
            } catch (AndesException dlcException) {
                // If an exception occur in this level, it means that there is a message store level error.
                // There's a possibility that we might lose this message
                // If the message is not removed the slot will not get removed which will lead to an
                // inconsistency
                log.error("Error moving message " + message.getMessageID() + " to dead letter channel.", dlcException);
            }
        }
    }
}
