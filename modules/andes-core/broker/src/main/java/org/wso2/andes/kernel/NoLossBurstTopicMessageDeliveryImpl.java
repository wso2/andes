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
import org.wso2.andes.kernel.subscription.AndesSubscription;
import org.wso2.andes.kernel.subscription.StorageQueue;

import java.util.*;

/**
 * This class implements topic message delivery. This implementation will deliver every message
 * to every subscriber without any message loss or without considering overflowing memory.
 */
public class NoLossBurstTopicMessageDeliveryImpl implements MessageDeliveryStrategy {

    private static Log log = LogFactory.getLog(NoLossBurstTopicMessageDeliveryImpl.class);

    public NoLossBurstTopicMessageDeliveryImpl() {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int deliverMessageToSubscriptions(StorageQueue storageQueue) throws
            AndesException {

        Collection<DeliverableAndesMetadata> messages = storageQueue.getMessagesForDelivery().values();
        int sentMessageCount = 0;
        Iterator<DeliverableAndesMetadata> iterator = messages.iterator();
        List<DeliverableAndesMetadata> droppedTopicMessagesList = new ArrayList<>();

        /*
          Get all relevant types of subscriptions that are not suspended. This call does NOT return hierarchical
          subscriptions for the destination. There are duplicated messages for each different subscribed destination.
         */
        List<org.wso2.andes.kernel.subscription.AndesSubscription> subscriptions4Queue =
                storageQueue.getBoundSubscriptions();

        List<org.wso2.andes.kernel.subscription.AndesSubscription> currentSubscriptions =
                Collections.unmodifiableList(subscriptions4Queue);

        while (iterator.hasNext()) {

            try {
                DeliverableAndesMetadata message = iterator.next();

                List<AndesSubscription> subscriptionsToDeliver = new ArrayList<>();

                //All subscription filtering logic for topics goes here
                for (AndesSubscription subscription : currentSubscriptions) {

                    if (subscription.getSubscriberConnection().isSuspended()) {
                        continue;
                    }

                    /*
                     * Consider the arrival time of the message. Only topic
                     * subscribers which appeared before publishing this message should receive it
                     */
                    if ((subscription.getSubscriberConnection().getSubscribeTime() > message.getArrivalTime())) {
                        continue;
                    }

                    // Avoid sending if the selector of subscriber does not match
                    if (!subscription.getSubscriberConnection().isMessageAcceptedByConnectionSelector(message)) {
                        continue;
                    }

                    subscriptionsToDeliver.add(subscription);
                }

                if (subscriptionsToDeliver.size() == 0) {
                    iterator.remove(); // remove buffer
                    droppedTopicMessagesList.add(message);

                    continue; // skip this iteration if no subscriptions for the message
                }

                boolean allTopicSubscriptionsSaturated = true;
                for (AndesSubscription subscription : subscriptionsToDeliver) {
                    if (subscription.getSubscriberConnection().hasRoomToAcceptMessages()) {
                        allTopicSubscriptionsSaturated = false;
                        break;
                    }
                }

                /**
                 * if all topic subscriptions are saturated we skip sending. This is to prevent protocol buffers
                 * overfilling. These messages will be tried in order in next buff flush.
                 */
                if (allTopicSubscriptionsSaturated) {
                    break;
                }

                message.markAsScheduledToDeliver(subscriptionsToDeliver);

                iterator.remove();
                for (AndesSubscription localSubscription : subscriptionsToDeliver) {
                    MessageFlusher.getInstance().deliverMessageAsynchronously(localSubscription, message);
                }


                if (log.isDebugEnabled()) {
                    log.debug("Removing Scheduled to send message from buffer. MsgId= " + message.getMessageID());
                }

                sentMessageCount++;


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
