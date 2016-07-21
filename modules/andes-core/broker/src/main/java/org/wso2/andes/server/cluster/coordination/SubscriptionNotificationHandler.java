/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
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

package org.wso2.andes.server.cluster.coordination;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.ClusterNotificationListener;
import org.wso2.andes.kernel.disruptor.inbound.InboundEventManager;
import org.wso2.andes.kernel.disruptor.inbound.InboundSubscriptionSyncEvent;
import org.wso2.andes.kernel.subscription.AndesSubscriptionManager;

/**
 * ClusterNotificationListener implementation listening for subscription changes
 * and handling them
 */
public class SubscriptionNotificationHandler implements ClusterNotificationListener {

    private Log log = LogFactory.getLog(SubscriptionNotificationHandler.class);
    private AndesSubscriptionManager subscriptionManager;
    private InboundEventManager inboundEventManager;

    /**
     * Create a listener to listen for subscription changes in cluster
     *
     * @param subscriptionManager manager to handle notification inside Andes kernel
     * @param eventManager        manager for generating inbound events
     */
    public SubscriptionNotificationHandler(AndesSubscriptionManager subscriptionManager,
                                           InboundEventManager eventManager) {
        this.subscriptionManager = subscriptionManager;
        this.inboundEventManager = eventManager;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void handleClusterNotification(ClusterNotification notification) {
        try {
            InboundSubscriptionSyncEvent subscriptionSyncEvent =
                    new InboundSubscriptionSyncEvent(notification.getEncodedObjectAsString());
            SubscriptionChange changeType = SubscriptionChange.valueOf(notification.getChangeType());
            switch (changeType) {
                case Added:
                    subscriptionSyncEvent.prepareForRemoteSubscriptionAdd(subscriptionManager);
                    inboundEventManager.publishStateEvent(subscriptionSyncEvent);
                    subscriptionSyncEvent.waitForCompletion();
                    break;
                case Closed:
                    subscriptionSyncEvent.prepareForRemoteSubscriptionClose(subscriptionManager);
                    inboundEventManager.publishStateEvent(subscriptionSyncEvent);
                    subscriptionSyncEvent.waitForCompletion();
                    break;
            }
        } catch (Exception e) {
            log.error("Error while handling subscription notification", e);
        }
    }
}
