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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesSubscription;
import org.wso2.andes.kernel.SubscriptionListener;
import org.wso2.andes.server.cluster.coordination.ClusterNotification;
import org.wso2.andes.subscription.BasicSubscription;

import java.util.ArrayList;
import java.util.List;

/**
 * This listener class is triggered when any subscription change (Subscriber added, subscriber deleted
 * or subscriber disconnected) is happened in cluster via Hazelcast.
 */
public class ClusterSubscriptionChangedListener implements MessageListener {

    private static Log log = LogFactory.getLog(ClusterSubscriptionChangedListener.class);
    private List<SubscriptionListener> subscriptionListeners = new ArrayList<SubscriptionListener>();

    /**
     * Register a listener interested in cluster subscription changes
     *
     * @param listener listener to be registered
     */
    public void addSubscriptionListener(SubscriptionListener listener) {
        subscriptionListeners.add(listener);
    }

    /**
     * This method is triggered when a subscription is changed in clustered environment.
     *
     * @param message contains the ClusterNotification
     */
    @Override
    public void onMessage(Message message) {
        ClusterNotification clusterNotification = (ClusterNotification) message.getMessageObject();
        log.debug("Handling cluster gossip: received a subscription change notification " + clusterNotification.getDescription());
        AndesSubscription andesSubscription = new BasicSubscription(clusterNotification.getEncodedObjectAsString());
        SubscriptionListener.SubscriptionChange change = SubscriptionListener.SubscriptionChange.valueOf(clusterNotification.getChangeType());
        try {
            for (SubscriptionListener subscriptionListener : subscriptionListeners) {
                subscriptionListener.handleClusterSubscriptionsChanged(andesSubscription, change);
            }
        } catch (AndesException e) {
            log.error("error while handling cluster subscription change notification", e);
        }
    }
}
