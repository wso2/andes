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

/**
 * QueueListener listens and handles queue changes that have occurred locally and cluster-wide.
 */
public class QueueListener {

    private static final Logger log = Logger.getLogger(QueueListener.class);

    /**
     * Queue event types that could be present in the cluster.
     */
    public enum QueueEvent {
        ADDED,
        DELETED,
        PURGED
    }

    /**
     * Local event handler is used to notify local queue changes to the cluster.
     */
    private ClusterNotificationPublisher clusterNotificationPublisher;

    /**
     * VirtualHostConfigSynchronizer is used to synchronize cluster events received with Qpid.
     */
    VirtualHostConfigSynchronizer virtualHostConfigSynchronizer;

    /**
     * Creates a listener for queue changes given a publisher to notify the changes that are received to the cluster.
     *
     * @param publisher Hazelcast/RDBMS based or standalone publisher to publish cluster notifications.
     */
    public QueueListener(ClusterNotificationPublisher publisher) {
        clusterNotificationPublisher = publisher;
        virtualHostConfigSynchronizer = ClusterResourceHolder.getInstance().getVirtualHostConfigSynchronizer();
    }

    /**
     * Handle when a queue has changed in the cluster.
     *
     * @param andesQueue changed queue
     * @param changeType what type of change has happened
     */
    public void handleClusterQueuesChanged(AndesQueue andesQueue, QueueEvent changeType) throws AndesException {
        if (log.isDebugEnabled()) {
            log.debug("Cluster event received: " + andesQueue.encodeAsString());
        }
        switch (changeType) {
            case ADDED:
                //create a queue
                virtualHostConfigSynchronizer.clusterQueueAdded(andesQueue);
                break;
            case DELETED:
                //Delete remaining subscriptions from the local and cluster subscription maps
                ClusterResourceHolder.getInstance().getSubscriptionManager().deleteAllLocalSubscriptionsOfBoundQueue(
                        andesQueue.queueName, andesQueue.getProtocolType(), andesQueue.getDestinationType());
                ClusterResourceHolder.getInstance().getSubscriptionManager().deleteAllClusterSubscriptionsOfBoundQueue(
                        andesQueue.queueName, andesQueue.getProtocolType(), andesQueue.getDestinationType());

                //delete queue
                virtualHostConfigSynchronizer.clusterQueueRemoved(andesQueue);
                break;
            case PURGED:
                //purge queue
                virtualHostConfigSynchronizer.clusterQueuePurged(andesQueue);
                break;
        }
    }

    /**
     * Handle when a queue has changed in the node.
     *
     * @param andesQueue changed queue
     * @param changeType what type of change has happened
     */
    public void handleLocalQueuesChanged(AndesQueue andesQueue, QueueEvent changeType) throws AndesException {
        ClusterNotification clusterNotification
                = new ClusterNotification(andesQueue.encodeAsString(), changeType.toString());
        clusterNotificationPublisher.publishClusterNotification(clusterNotification);

    }
}
