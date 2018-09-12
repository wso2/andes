/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.andes.kernel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.subscription.AndesSubscription;

import java.util.UUID;

/**
 * DeliveryMessageHandler responsible for submit the messages to a thread pool to deliver asynchronously.
 */
public class DeliveryMessageHandler extends DeliveryResponsibility {

    private static Log log = LogFactory.getLog(DeliveryMessageHandler.class);

    /**
     * Submit the message to the disruptor to deliver asynchronously.
     * {@inheritDoc}
     */
    @Override
    protected boolean performResponsibility(final AndesSubscription subscription,
                                            DeliverableAndesMetadata message) {
        synchronized (subscription.getSubscriberConnection().getProtocolChannelID().toString().intern()) {
            if (!subscription.isAttached() && !message.isStale()) {
                subscription.getStorageQueue().bufferMessageForDelivery(message);
               return false;
            }

            if (log.isDebugEnabled()) {
                log.debug("Scheduled message id= " + message.getMessageID() + " to be sent to subscription= " + subscription);
            }
            // Mark message as came into the subscription for deliver.
            message.markAsScheduledToDeliver(subscription);
            UUID subscriptionChannelID = subscription.getSubscriberConnection().getProtocolChannelID();
            message.markAsDispatchedToDeliver(subscriptionChannelID);
            ProtocolMessage protocolMessage = message.generateProtocolDeliverableMessage(subscriptionChannelID);
            subscription.getSubscriberConnection().addMessageToSendingTracker(protocolMessage);
            MessageFlusher.getInstance().getFlusherExecutor().submit(subscription, protocolMessage);
            return true;
        }
    }
}
