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

package org.wso2.andes.kernel.disruptor.delivery;

import com.lmax.disruptor.EventHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.Andes;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.DeliverableAndesMetadata;
import org.wso2.andes.kernel.MessageStatus;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.kernel.ProtocolDeliveryFailureException;
import org.wso2.andes.kernel.ProtocolDeliveryRulesFailureException;
import org.wso2.andes.kernel.ProtocolMessage;
import org.wso2.andes.kernel.SubscriptionAlreadyClosedException;
import org.wso2.andes.kernel.subscription.AndesSubscription;
import org.wso2.andes.kernel.subscription.StorageQueue;
import org.wso2.andes.metrics.MetricsConstants;
import org.wso2.andes.tools.utils.MessageTracer;
import org.wso2.carbon.metrics.manager.Counter;
import org.wso2.carbon.metrics.manager.Level;
import org.wso2.carbon.metrics.manager.Meter;
import org.wso2.carbon.metrics.manager.MetricManager;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


/**
 * Disruptor handler used to send the message. This the final event handler of the ring-buffer
 */
public class DeliveryEventHandler implements EventHandler<DeliveryEventData> {
    /**
     * Class logger
     */
    private static final Log log = LogFactory.getLog(DeliveryEventHandler.class);

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
     * @param deliveryEventData Event data holder
     * @param sequence          Sequence number of the disruptor event
     * @param endOfBatch        Indicate end of batch
     * @throws Exception
     */
    @Override
    public void onEvent(DeliveryEventData deliveryEventData, long sequence, boolean endOfBatch) throws Exception {
        AndesSubscription subscription = deliveryEventData.getLocalSubscription();

        // Taking the absolute value since hashCode can be a negative value
        UUID protocolChannelID = subscription.getSubscriberConnection().getProtocolChannelID();
        long channelModulus = Math.abs(protocolChannelID.hashCode() % numberOfConsumers);

        // Filter tasks assigned to this handler
        if (channelModulus == ordinal) {
            ProtocolMessage protocolMessage = deliveryEventData.getMetadata();
            DeliverableAndesMetadata message = protocolMessage.getMessage();

            try {
                if (deliveryEventData.isErrorOccurred()) {
                    onSendError(message, subscription);
                    subscription.getSubscriberConnection().onWriteToConnectionError(message.getMessageID());
                    routeMessageToDLC(message, subscription);
                    return;
                }
                if (!message.isStale()) {
                    if (subscription.isActive()) {
                        //Tracing Message
                        MessageTracer.trace(message, MessageTracer.DISPATCHED_TO_PROTOCOL);

                        //Adding metrics meter for ack rate
                        Meter messageMeter = MetricManager.meter(MetricsConstants.MSG_SENT_RATE, Level.INFO);
                        messageMeter.mark();
                        //Adding metrics counter for dequeue messages
                        Counter counter = MetricManager.counter(MetricsConstants.DEQUEUE_MESSAGES, Level.INFO);
                        counter.inc();

                        subscription.getSubscriberConnection().writeMessageToConnection(protocolMessage,
                                deliveryEventData.getAndesContent());

                    } else {
                        onSendError(message, subscription);
                        onSubscriptionAlreadyClosed(message, subscription);
                    }
                } else {
                    // Stale only happens when last subscription is closed and slot is returned. No need to re-queue
                    // here. Messages will be read again when slot is re-acquired and messages are read as new messages.
                    onSendError(message, subscription);
                    onSubscriptionAlreadyClosed(message, subscription);
                    // Tracing Message
                    MessageTracer.trace(message.getMessageID(), message.getDestination(),
                            MessageTracer.DISCARD_STALE_MESSAGE);

                }
            } catch (ProtocolDeliveryRulesFailureException e) {
                onSendError(message, subscription);
                subscription.getSubscriberConnection().onWriteToConnectionError(message.getMessageID());
                routeMessageToDLC(message, subscription);

            } catch (SubscriptionAlreadyClosedException ex) {
                //we do not log the error as subscriber is closing this is an expected exception.
                //subscriber is already closed while try to deliver
                onSendError(message, subscription);
                onSubscriptionAlreadyClosed(message, subscription);

            } catch (ProtocolDeliveryFailureException ex) {
                // we log the exception earlier. Hence logging is not required here. We increase delivery count so max
                // send count delivery rule is evaluated and message is sent to DLC if failure is consistent
                onDeliveryException(message, subscription);
                reQueueMessageIfDurable(message, subscription);

            } catch (Throwable e) {
                log.error("Unexpected error while delivering message. Message id " + message.getMessageID(), e);
                onDeliveryException(message, subscription);
                reQueueMessageIfDurable(message, subscription);

            }
        }
    }

    /**
     * Re-queue message for a durable subscriber.
     *
     * @param message      message metadata to re-queue
     * @param subscription subscription to check on
     * @throws AndesException on re-queue error
     */
    private void reQueueMessageIfDurable(DeliverableAndesMetadata message, AndesSubscription subscription)
            throws AndesException {
        if (subscription.isDurable()) {
            StorageQueue storageQueue = subscription.getStorageQueue();
            storageQueue.bufferMessageForDelivery(message);
        } else {
            if (!message.isOKToDispose()) {
                log.warn("Cannot send message id= " + message.getMessageID() + " as subscriber is closed");
            }
        }
    }

    /**
     * Called when a delivery failure happened due to channel is already closed
     *
     * @param message      message failed to deliver
     * @param subscription subscription already closed
     * @throws AndesException
     */
    private void onSubscriptionAlreadyClosed(DeliverableAndesMetadata message, AndesSubscription subscription) throws
            AndesException {
        UUID channelID = subscription.getSubscriberConnection().getProtocolChannelID();
        message.markDeliveredChannelAsClosed(channelID);
        //re-evaluate ACK if a topic subscriber has closed
        if (!subscription.isDurable()) {
            message.evaluateMessageAcknowledgement();
            if (message.isAknowledgedByAll()) {
                //try to delete message
                List<DeliverableAndesMetadata> messageToDelete = new ArrayList<>();
                messageToDelete.add(message);
                MessagingEngine.getInstance().deleteMessages(messageToDelete);
            }
        }
    }

    /**
     * This should be called whenever a delivery failure happens.
     * This will clear message status and subscriber status so that it will not
     * affect future message schedules
     *
     * @param messageMetadata message failed to be delivered
     * @param localSubscription subscription failed to deliver message
     */
    private void onSendError(DeliverableAndesMetadata messageMetadata, AndesSubscription localSubscription) {
        //Send failed. Rollback changes done that assumed send would be success
        UUID channelID = localSubscription.getSubscriberConnection().getProtocolChannelID();
        messageMetadata.markDeliveryFailureOfASentMessage(channelID);
    }

    /**
     * This should be called whenever a protocol delivery failure happens.
     * This will clear message status and subscriber status so that it will not
     * affect future message schedules. Also this will not decrement message delivery count
     * so that message delivery will not failure infinitely (will get caught by max delivery count rule).
     *
     * @param messageMetadata   message failed to be delivered by protocol
     * @param localSubscription subscription failed to deliver message
     */
    private void onDeliveryException(DeliverableAndesMetadata messageMetadata, AndesSubscription localSubscription) {
        UUID channelID = localSubscription.getSubscriberConnection().getProtocolChannelID();
        messageMetadata.markDeliveryFailureByProtocol(channelID);
        localSubscription.getSubscriberConnection().onWriteToConnectionError(messageMetadata.getMessageID());
    }

    /**
     * When an error is occurred in message delivery, this method will move the message to dead letter channel.
     *
     * @param message Meta data for the message
     */
    private void routeMessageToDLC(DeliverableAndesMetadata message,
                                   AndesSubscription subscription)
                                   throws AndesException {

        // If message is a queue message we move the message to the Dead Letter Channel
        // since topics doesn't have a Dead Letter Channel
        if (subscription.isDurable()) {
            log.warn("Moving message to Dead Letter Channel Due to Send Error. Message ID " + message.getMessageID());
            try {
                Andes.getInstance().moveMessageToDeadLetterChannel(message, message.getDestination());
            } catch (AndesException dlcException) {
                // If an exception occur in this level, it means that there is a message store level error.
                // There's a possibility that we might lose this message
                // If the message is not removed the slot will not get removed which will lead to an
                // inconsistency
                log.error("Error moving message " + message.getMessageID() + " to dead letter channel.", dlcException);
            }
        } else {
            //for non durable topic messages see if we can delete the message
            log.warn("Discarding topic message id = " + message.getMessageID() + " as delivery failed");
            message.markAsRejectedByClient(subscription.getSubscriberConnection().getProtocolChannelID());
            List<DeliverableAndesMetadata> messagesToRemove = new ArrayList<>();
            message.evaluateMessageAcknowledgement();
            if (message.getLatestState().equals(MessageStatus.ACKED_BY_ALL)) {
                messagesToRemove.add(message);
            }
            MessagingEngine.getInstance().deleteMessages(messagesToRemove);
        }
    }
}
