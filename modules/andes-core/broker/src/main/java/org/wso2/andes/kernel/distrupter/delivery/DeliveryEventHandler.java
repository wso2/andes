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

package org.wso2.andes.kernel.distrupter.delivery;

import com.lmax.disruptor.EventHandler;
import org.apache.log4j.Logger;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesRemovableMetadata;
import org.wso2.andes.kernel.DeliverableAndesMessageMetadata;
import org.wso2.andes.kernel.LocalSubscription;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.server.cassandra.MessageFlusher;

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
    public void onEvent(DeliveryEventData deliveryEventData, long sequence, boolean endOfBatch)
            throws Exception {
        LocalSubscription subscription = deliveryEventData.getLocalSubscription();

        // Taking the absolute value since hashCode can be a negative value
        long channelModulus = Math.abs(subscription.getChannelID() % numberOfConsumers);

        // Filter tasks assigned to this handler
        if (channelModulus == ordinal) {
            DeliverableAndesMessageMetadata message = deliveryEventData.getMetadata();
            long channelIDOfSubscription = subscription.getAndesChannel().getChannelID();
            try {

                if (!message.isDeliverableToChannel(channelIDOfSubscription)) {
                    log.warn(
                            "This is not a new message. It has been sent to this subscriber " +
                            "earlier. Expecting Acknowledge." +
                            "Rejecting delivery of message. Msg id = " + message
                                    .getMessageID() + " channel id= " + channelIDOfSubscription);
                    return;
                }

                //decrement number of schedule deliveries before send to subscriber to avoid
                // parallel status update issues. Also we save the reference before sending due to same reason
                message.recordDelivery(channelIDOfSubscription);
                subscription.getAndesChannel().registerMessageDelivery(message);

                //if an error occurred rollback delivery record and send message to DLC
                if (deliveryEventData.isErrorOccurred()) {
                    handleSendError(message);
                    message.rollBackDeliveryRecord(channelIDOfSubscription, true);
                    subscription.getAndesChannel().unRegisterMessageDelivery(message);
                    return;
                }

                //perform actual send if subscriber is active now
                if (subscription.isActive()) {
                    boolean deliverySuccess = subscription
                            .sendMessageToSubscriber(message, deliveryEventData.getAndesContent());
                    if (deliverySuccess) {
                        //we have already performed what needs to be done when successful before actual sending
                    } else {
                        //Move to dead letter channel
                        handleSendError(message);
                        message.rollBackDeliveryRecord(channelIDOfSubscription, true);
                        subscription.getAndesChannel().unRegisterMessageDelivery(message);
                    }
                } else {
                    message.rollBackDeliveryRecord(channelIDOfSubscription, false);
                    subscription.getAndesChannel().unRegisterMessageDelivery(message);
                    //requeue message again as subscriber is gone
                    MessageFlusher.getInstance()
                                  .reQueueUndeliveredMessagesDueToInactiveSubscriptions(message);
                }
            } catch (Throwable e) {
                log.error("Error while delivering message. Moving to Dead Letter Queue.", e);
                handleSendError(message);
                message.rollBackDeliveryRecord(channelIDOfSubscription, true);
                subscription.getAndesChannel().unRegisterMessageDelivery(message);
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
    private void handleSendError(DeliverableAndesMessageMetadata message) {
        // If message is a queue message we move the message to the Dead Letter Channel
        // since topics doesn't have a Dead Letter Channel
        log.warn("Moving to Dead Letter Queue.");
        if (!message.isTopic()) {
            AndesRemovableMetadata removableMessage = new AndesRemovableMetadata(message.getMessageID(),
                                                                                 message.getDestination(),
                                                                                 message.getStorageQueueName());
            List<AndesRemovableMetadata> messageToMoveToDLC = new ArrayList<AndesRemovableMetadata>();
            messageToMoveToDLC.add(removableMessage);
            try {
                MessagingEngine.getInstance().deleteMessages(messageToMoveToDLC, true);
                //TODO: move this to a single method in Messaging engine
                //TODO: should remove this message from all channels
                message.setMessageStatus(DeliverableAndesMessageMetadata.MessageStatus.DLC_MESSAGE);

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
