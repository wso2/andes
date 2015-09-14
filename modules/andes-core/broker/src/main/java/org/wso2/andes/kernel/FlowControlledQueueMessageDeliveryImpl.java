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
import org.wso2.andes.subscription.LocalSubscription;
import org.wso2.andes.subscription.SubscriptionStore;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Strategy definition for queue message delivery
 */
public class FlowControlledQueueMessageDeliveryImpl implements MessageDeliveryStrategy {

    private static Log log = LogFactory.getLog(FlowControlledQueueMessageDeliveryImpl.class);
    private SubscriptionStore subscriptionStore;

    public FlowControlledQueueMessageDeliveryImpl(SubscriptionStore subscriptionStore) {
        this.subscriptionStore = subscriptionStore;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int deliverMessageToSubscriptions(String destination, Set<DeliverableAndesMetadata> messages) throws
            AndesException {

        int sentMessageCount = 0;
        boolean noSubscribersForDestination = false;
        Iterator<DeliverableAndesMetadata> iterator = messages.iterator();


        while (iterator.hasNext()) {

            try {

                DeliverableAndesMetadata message = iterator.next();

                /**
                 * get all relevant type of subscriptions. This call does NOT
                 * return hierarchical subscriptions for the destination. There
                 * are duplicated messages for each different subscribed destination.
                 * For durable topic subscriptions this should return queue subscription
                 * bound to unique queue based on subscription id
                 */
                Collection<LocalSubscription> subscriptions4Queue =
                        subscriptionStore.getActiveLocalSubscribers(destination, message.isTopic());

                if (subscriptions4Queue.isEmpty()) {
                    // We don't have subscribers for this message
                    // Handle orphaned slot created with this no subscription scenario for queue
                    noSubscribersForDestination = true;
                    break; // break the loop
                }

                int numOfCurrentMsgDeliverySchedules = 0;

                /**
                 * if message is addressed to queues, only ONE subscriber should
                 * get the message. Otherwise, loop for every subscriber
                 */
                for (int j = 0; j < subscriptions4Queue.size(); j++) {
                    LocalSubscription localSubscription = MessageFlusher.getInstance().
                            findNextSubscriptionToSent(destination, subscriptions4Queue);
                    if (localSubscription.hasRoomToAcceptMessages()) {
                        if (log.isDebugEnabled()) {
                            log.debug("Scheduled to send id = " + message.getMessageID());
                        }

                        // In a re-queue for delivery scenario we need the correct destination. Hence setting
                        // it back correctly in AndesMetadata for durable subscription for topics
                        if (localSubscription.isBoundToTopic()) {
                            message.setDestination(localSubscription.getTargetQueue());
                        }

                        message.markAsScheduledToDeliver(localSubscription);
                        MessageFlusher.getInstance().deliverMessageAsynchronously(localSubscription, message);
                        numOfCurrentMsgDeliverySchedules++;

                        //for queue messages and durable topic messages (as they are now queue messages)
                        // we only send to one selected subscriber if it is a queue message
                        break;
                    }
                }

                if (numOfCurrentMsgDeliverySchedules == 1) {
                    iterator.remove();
                    if (log.isDebugEnabled()) {
                        log.debug("Removing Scheduled to send message from buffer. MsgId= " + message.getMessageID());
                    }
                    sentMessageCount++;
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("All subscriptions for destination " + destination + " have max unacked " +
                                "messages " + message.getDestination());
                    }
                    //if we continue message order will break
                    break;
                }

            } catch (NoSuchElementException ex) {
                // This exception can occur because the iterator of ConcurrentSkipListSet loads the at-the-time snapshot.
                // Some records could be deleted by the time the iterator reaches them.
                // However, this can only happen at the tail of the collection, not in middle, and it would cause the loop
                // to blindly check for a batch of deleted records.
                // Given this situation, this loop should break so the sendFlusher can re-trigger it.
                // for tracing purposes can use this : log.warn("NoSuchElementException thrown",ex);
                log.warn("NoSuchElementException thrown. ", ex);
                break;
            }
        }
        // clear all tracking when orphan slot situation
        if (noSubscribersForDestination) {
            messages.clear();
        }

        return sentMessageCount;
    }

}
