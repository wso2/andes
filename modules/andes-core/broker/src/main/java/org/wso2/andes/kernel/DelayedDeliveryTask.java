/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.kernel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.subscription.LocalSubscription;
import org.wso2.andes.tools.utils.MessageTracer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Delayed delivery task will deliver rejected messages from a delayed queue which is inside a subscription.
 */
public class DelayedDeliveryTask implements Runnable {
    private static final Log log = LogFactory.getLog(DelayedDeliveryTask.class);
    Set<LocalSubscription> subscriptionsWithRejectedMessages = new ConcurrentSkipListSet<>();

    /**
     * {@inheritDoc}
     * <p>
     *     Each subscription is taken iteratively and a rejected message is then delivered. If a subscription
     * </p>
     */
    @Override
    public void run() {
        try {
            Iterator<LocalSubscription> subscriptionIterator = subscriptionsWithRejectedMessages.iterator();
            while (subscriptionIterator.hasNext()) {
                LocalSubscription subscription = subscriptionIterator.next();
                BlockingQueue<RejectedMessage> rejectedMessages = subscription.getRejectedMessages();
                if (!rejectedMessages.isEmpty()) {
                    List<RejectedMessage> rejectedMessageList = new ArrayList<>();
                    // Get all expired messages to a list of a specific subscriber.
                    rejectedMessages.drainTo(rejectedMessageList);
                    for (RejectedMessage message : rejectedMessageList) {
                        MessageFlusher.getInstance().scheduleMessageForSubscription(subscription,
                                                                                message.getDeliverableAndesMetadata());
                        //Tracing message activity
                        MessageTracer.trace(message.getDeliverableAndesMetadata(),
                                                                            MessageTracer.MESSAGE_REQUEUED_SUBSCRIBER);
                    }
                } else {
                    // Remove the subscription from the subscription list as there are no more requeued messages.
                    subscriptionIterator.remove();
                }
            }
        } catch (AndesException e) {
            log.error("Error occurred while scheduling messages to subscription by delayed delivery task.", e);
        }
    }

    /**
     * Adds new subscription which has rejected messages.
     *
     * @param localSubscription The subscription.
     */
    public void addSubscription(LocalSubscription localSubscription) {
        subscriptionsWithRejectedMessages.add(localSubscription);
    }
}
