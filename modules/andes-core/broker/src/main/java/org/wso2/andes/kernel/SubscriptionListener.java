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

package org.wso2.andes.kernel;

import org.apache.log4j.Logger;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.coordination.ClusterNotificationPublisher;
import org.wso2.andes.server.cluster.coordination.ClusterNotification;
import org.wso2.andes.server.virtualhost.VirtualHostConfigSynchronizer;
import org.wso2.andes.subscription.LocalSubscription;

/**
 * SubscriptionListener listens and handles exchange changes that have occurred locally and cluster-wide.
 */
public class SubscriptionListener implements LocalSubscriptionChangedListener {

    private static final Logger log = Logger.getLogger(SubscriptionListener.class);

    /**
     * Subscription event types that could be present in the cluster.
     */
	public enum SubscriptionChange{
        ADDED,
        DELETED,
        DISCONNECTED,
        /**
         * Merge subscriber after a split brain
         */
        MERGED
    }

    /**
     * Local event handler is used to notify local subscription changes to the cluster.
     */
    private ClusterNotificationPublisher clusterNotificationPublisher;

    /**
     * Creates a listener for binding changes given a publisher to notify the changes that are received to the cluster.
     *
     * @param publisher Hazelcast/RDBMS based or standalone publisher to publish cluster notifications.
     */
    public SubscriptionListener(ClusterNotificationPublisher publisher) {
        clusterNotificationPublisher = publisher;
    }

    /**
     * Handle subscription changes in cluster.
     *
     * @param subscription subscription changed
     * @param changeType   type of change happened
     * @throws AndesException
     */
    public void handleClusterSubscriptionsChanged(AndesSubscription subscription, SubscriptionChange changeType)
            throws AndesException {
        if (log.isDebugEnabled()) {
            log.debug("Cluster event received: " + subscription.encodeAsStr());
        }
        ClusterResourceHolder.getInstance().getSubscriptionManager().updateClusterSubscriptionMaps(subscription,
                changeType);
    }

    /**
     * Handle local subscription changes.
     *
     * @param subscription subscription changed
     * @param changeType   type of change happened
     * @throws AndesException
     */
    public void handleLocalSubscriptionsChanged(LocalSubscription subscription, SubscriptionChange changeType)
            throws AndesException {
        handleClusterSubscriptionsChanged(subscription, changeType);
        ClusterNotification clusterNotification
                = new ClusterNotification(subscription.encodeAsStr(), changeType.toString());
        clusterNotificationPublisher.publishClusterNotification(clusterNotification);
    }
}
