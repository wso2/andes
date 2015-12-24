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
import org.wso2.andes.server.store.MessageMetaDataType;
import org.wso2.andes.subscription.LocalSubscription;
import org.wso2.andes.subscription.SubscriptionStore;

import java.util.*;

/**
 * This class implements topic message delivery. This implementation will deliver every message
 * to every subscriber without any message loss or without considering overflowing memory.
 */
public class NoLossBurstTopicMessageDeliveryImpl implements MessageDeliveryStrategy {

    private static Log log = LogFactory.getLog(NoLossBurstTopicMessageDeliveryImpl.class);
    private SubscriptionStore subscriptionStore;

    public NoLossBurstTopicMessageDeliveryImpl(SubscriptionStore subscriptionStore) {
        this.subscriptionStore = subscriptionStore;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int deliverMessageToSubscriptions(String destination, SortedSet<DeliverableAndesMetadata> messages) throws
            AndesException {
        int sentMessageCount = 0;
        List<DeliverableAndesMetadata> droppedTopicMessagesList = new ArrayList<>();


        while (!messages.isEmpty()) {

            try {
                DeliverableAndesMetadata message = messages.first();

                /**
                 * get all relevant type of subscriptions. This call does NOT
                 * return hierarchical subscriptions for the destination. There
                 * are duplicated messages for each different subscribed destination.
                 * For durable topic subscriptions this should return queue subscription
                 * bound to unique queue based on subscription id
                 */
                Collection<LocalSubscription> subscriptions4Queue =
                        subscriptionStore.getActiveLocalSubscribers(destination, message.isTopic());

                //If this is a topic message, we remove all durable topic subscriptions here.
                //Because durable topic subscriptions will get messages via queue path.
                Iterator<LocalSubscription> subscriptionIterator = subscriptions4Queue.iterator();
                while (subscriptionIterator.hasNext()) {
                    LocalSubscription subscription = subscriptionIterator.next();
                    /**
                     * Here we need to consider the arrival time of the message. Only topic
                     * subscribers who appeared before publishing this message should receive it
                     */
                    if (subscription.isDurable() || (subscription.getSubscribeTime() > message.getArrivalTime())) {
                        subscriptionIterator.remove();
                    }

                    // Avoid sending if the subscriber is MQTT and message is not MQTT
                    if (AndesSubscription.SubscriptionType.MQTT == subscription.getSubscriptionType()
                            && MessageMetaDataType.META_DATA_MQTT != message.getMetaDataType()) {
                        subscriptionIterator.remove();
                        // Avoid sending if the subscriber is AMQP and message is MQTT
                    } else if (AndesSubscription.SubscriptionType.AMQP == subscription.getSubscriptionType()
                            && MessageMetaDataType.META_DATA_MQTT == message.getMetaDataType()) {
                        subscriptionIterator.remove();
                    }
                }

                if (subscriptions4Queue.size() == 0) {
                    messages.remove(message);
                    droppedTopicMessagesList.add(message);

                    continue; // skip this iteration if no subscriptions for the message
                }

                boolean allTopicSubscriptionsSaturated = true;
                for (LocalSubscription subscription : subscriptions4Queue) {
                    if (subscription.hasRoomToAcceptMessages()) {
                        allTopicSubscriptionsSaturated = false;
                        break;
                    }
                }

                /**
                 * if all topic subscriptions are saturated we skip sending. This is to prevent protocol buffers
                 * overfilling. These messages will be tried in order in next buff flush.
                 */
                if(allTopicSubscriptionsSaturated) {
                    break;
                }

                message.markAsScheduledToDeliver(subscriptions4Queue);

                for (LocalSubscription localSubscription : subscriptions4Queue) {
                    MessageFlusher.getInstance().deliverMessageAsynchronously(localSubscription, message);
                }
                messages.remove(message);
                if (log.isDebugEnabled()) {
                    log.debug("Removing Scheduled to send message from buffer. MsgId= " + message.getMessageID());
                }
                sentMessageCount++;


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


        /*
         * Here we do not need to have orphaned slot scenario (return slot). If there are no subscribers
         * slot will be consumed and metadata will be removed. We duplicate topic messages per node
         */


        /*
         * delete topic messages that were dropped due to no subscriptions
         * for the message and due to has no room to enqueue the message.
         */
        MessagingEngine.getInstance().deleteMessages(droppedTopicMessagesList);

        return sentMessageCount;
    }
}
