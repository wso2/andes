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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedMap;

/**
 * Strategy definition for queue message delivery.
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

        SortedMap<Long, DeliverableAndesMetadata> messages =
                (SortedMap<Long, DeliverableAndesMetadata>) storageQueue.getMessagesForDelivery();
        int sentMessageCount = 0;
        /*
         * get all relevant type of subscriptions.
         * For durable topic subscriptions this should return queue subscription
         * bound to unique queue based on subscription id
         */
        List<AndesSubscription> subscriptions4Queue = storageQueue.getBoundSubscriptions();

        List<AndesSubscription> currentSubscriptions = new ArrayList<>(subscriptions4Queue);

        int numOfConsumers = currentSubscriptions.size();
        int consumerIndexCounter = 0;
        long minCursor = Long.MAX_VALUE;

        try {

            int numOfCurrentMsgDeliverySchedules = 0;

                /*
                 * if message is addressed to queues, only ONE subscriber should
                 * get the message. Otherwise, loop for every subscriber
                 */
            consumerLoop:
            for (int j = 0; j < numOfConsumers; j++) {
                int currentConsumerIndex = consumerIndexCounter % numOfConsumers;
                AndesSubscription localSubscription = currentSubscriptions.get(currentConsumerIndex);
                long currentCursor = localSubscription.getCursor();
                consumerIndexCounter = consumerIndexCounter + 1;
                if (localSubscription.getSubscriberConnection().isSuspended()) {
                    continue;
                }
                Iterator<Map.Entry<Long, DeliverableAndesMetadata>> iterator = messages.tailMap(currentCursor)
                        .entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<Long, DeliverableAndesMetadata> entry = iterator.next();
                    DeliverableAndesMetadata message = entry.getValue();
                    currentCursor = entry.getKey();
                    if (localSubscription.getSubscriberConnection().hasRoomToAcceptMessages()) {
                        if (!localSubscription.getSubscriberConnection().
                                isMessageAcceptedByConnectionSelector(message)) {
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
                        //if the message was delivered, it needs to be removed
                        iterator.remove();
                        MessageFlusher.getInstance().deliverMessageAsynchronously(localSubscription, message);
                        numOfCurrentMsgDeliverySchedules++;

                        //for queue messages and durable topic messages (as they are now queue messages)
                        // we only send to one selected subscriber if it is a queue message
                        localSubscription.setCursor(currentCursor);
                        minCursor = Long.min(currentCursor, minCursor);
                        if (numOfCurrentMsgDeliverySchedules == 1) {
                            if (log.isDebugEnabled()) {
                                log.debug(
                                        "Removing Scheduled to send message from buffer. MsgId= " + message.getMessageID());
                            }
                            sentMessageCount++;
                        }
                        break consumerLoop;
                    }
                }
            }
            storageQueue.getMessageHandler().setLastBufferedMessageId(minCursor);
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
        }
        return sentMessageCount;
    }

}
