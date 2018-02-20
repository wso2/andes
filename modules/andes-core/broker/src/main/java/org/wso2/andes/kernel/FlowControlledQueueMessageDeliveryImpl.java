/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.kernel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.kernel.subscription.AndesSubscription;
import org.wso2.andes.kernel.subscription.StorageQueue;
import org.wso2.andes.tools.utils.MessageTracer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Strategy definition for queue message delivery
 */
public class FlowControlledQueueMessageDeliveryImpl implements MessageDeliveryStrategy {

    private static Log log = LogFactory.getLog(FlowControlledQueueMessageDeliveryImpl.class);

    public FlowControlledQueueMessageDeliveryImpl() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int deliverMessageToSubscriptions(StorageQueue storageQueue) throws AndesException {

        Collection<DeliverableAndesMetadata> messages = storageQueue.getMessagesForDelivery();
        int sentMessageCount = 0;
        Iterator<DeliverableAndesMetadata> iterator = messages.iterator();

        /*
         * get all relevant type of subscriptions.
         * For durable topic subscriptions this should return queue subscription
         * bound to unique queue based on subscription id
         */
        List<AndesSubscription> subscriptions4Queue = storageQueue.getBoundSubscriptions();

        List<AndesSubscription> currentSubscriptions = new ArrayList<>(subscriptions4Queue);

        int numOfConsumers = currentSubscriptions.size();
        int consumerIndexCounter = 0;

        while (iterator.hasNext()) {
            try {

                DeliverableAndesMetadata message = iterator.next();
                int numOfCurrentMsgDeliverySchedules = 0;
                boolean subscriberWithMatchingSelectorFound = true;
                boolean suspendedSubFound = false;

                /*
                 * if message is addressed to queues, only ONE subscriber should
                 * get the message. Otherwise, loop for every subscriber
                 */
                for (int j = 0; j < numOfConsumers; j++) {
                    int currentConsumerIndex = consumerIndexCounter % numOfConsumers;
                    AndesSubscription localSubscription = currentSubscriptions.get(currentConsumerIndex);
                    consumerIndexCounter = consumerIndexCounter + 1;
                    if (localSubscription.getSubscriberConnection().isSuspended()) {
                        suspendedSubFound = true;
                        continue;
                    }
                    if (localSubscription.getSubscriberConnection().hasRoomToAcceptMessages()
                            & localSubscription.getSubscriberConnection().isReadyToDeliver()) {

                        if (!localSubscription.getSubscriberConnection().
                                isMessageAcceptedByConnectionSelector(message)) {
                            // If this doesn't match a selector we skip sending the message
                            subscriberWithMatchingSelectorFound = false;
                            continue; // continue on to match selectors of other subscribers
                        }
                        if (log.isDebugEnabled()) {
                            log.debug("Scheduled to send message id = " + message.getMessageID() +
                                    " to subscription id= " + localSubscription.getSubscriptionId());
                        }

                        // In a re-queue for delivery scenario we need the correct destination. Hence setting
                        // it back correctly in AndesMetadata for durable subscription for topics
                        if (storageQueue.getMessageRouter().
                                getName().equals(AMQPUtils.TOPIC_EXCHANGE_NAME) && storageQueue.isDurable()) {

                            message.setDestination(storageQueue.getName());
                        }

                        message.markAsScheduledToDeliver(localSubscription);
                        iterator.remove();
                        MessageFlusher.getInstance().deliverMessageAsynchronously(localSubscription, message);
                        numOfCurrentMsgDeliverySchedules++;

                        //for queue messages and durable topic messages (as they are now queue messages)
                        // we only send to one selected subscriber if it is a queue message
                        break;
                    }
                }

                //if the message was delivered, it needs to be removed
                if (numOfCurrentMsgDeliverySchedules == 1) {
                    if (log.isDebugEnabled()) {
                        log.debug("Removing Scheduled to send message from buffer. MsgId= " + message.getMessageID());
                    }
                    sentMessageCount++;
                } else {

                    //if no subscriber has a matching selector, route message to DLC queue
                    if (!subscriberWithMatchingSelectorFound && !suspendedSubFound) {
                        message.addMessageStatus(MessageStatus.NO_MATCHING_CONSUMER);
                        Andes.getInstance().moveMessageToDeadLetterChannel(message, message.getDestination());
                        iterator.remove();
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("All subscriptions bounded for queue " + storageQueue.getName()
                                      + " have reached number of max unacked messages, or are in the suspended state "
                                      + "Skipping delivery of message id= " + message.getMessageID());
                        }
                        //if we continue message order will break
                        break;
                    }
                }

            } catch (NoSuchElementException ex) {
                // This exception can occur because the iterator of ConcurrentSkipListSet loads the at-the-time
                // snapshot.
                // Some records could be deleted by the time the iterator reaches them.
                // However, this can only happen at the tail of the collection, not in middle, and it would cause the
                // loop
                // to blindly check for a batch of deleted records.
                // Given this situation, this loop should break so the sendFlusher can re-trigger it.
                // for tracing purposes can use this : log.warn("NoSuchElementException thrown",ex);
                log.warn("NoSuchElementException thrown. ", ex);
                break;
            }
        }

        return sentMessageCount;
    }

}
