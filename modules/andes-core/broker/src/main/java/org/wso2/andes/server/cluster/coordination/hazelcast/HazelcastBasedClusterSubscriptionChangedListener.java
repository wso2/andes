/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.server.cluster.coordination.hazelcast;

import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import org.wso2.andes.kernel.SubscriptionListener;
import org.wso2.andes.server.cluster.coordination.ClusterNotificationHandler;
import org.wso2.andes.server.cluster.coordination.ClusterNotification;

/**
 * This listener class is triggered when any subscription change (Subscriber added, subscriber deleted
 * or subscriber disconnected) is happened in cluster via Hazelcast.
 */
public class HazelcastBasedClusterSubscriptionChangedListener implements MessageListener {

    /**
     * Listener to handle cluster event.
     */
    private ClusterNotificationHandler eventListener = new ClusterNotificationHandler();

    /**
     * Register a listener interested in cluster subscription changes
     *
     * @param listener listener to be registered
     */
    public void addSubscriptionListener(SubscriptionListener listener) {
        eventListener.addSubscriptionListener(listener);
    }

    /**
     * This method is triggered when a subscription is changed in clustered environment.
     *
     * @param message contains the ClusterNotification
     */
    @Override
    public void onMessage(Message message) {
        // We need to skip the cluster notifications that are orignated from the same node since they are already
        // processed.
        if (!message.getPublishingMember().localMember()) {
            ClusterNotification clusterNotification = (ClusterNotification) message.getMessageObject();
            eventListener.handleClusterSubscriptionsChanged(clusterNotification.getEncodedObjectAsString(),
                    SubscriptionListener.SubscriptionChange.valueOf(clusterNotification.getChangeType()));
        }
    }
}
