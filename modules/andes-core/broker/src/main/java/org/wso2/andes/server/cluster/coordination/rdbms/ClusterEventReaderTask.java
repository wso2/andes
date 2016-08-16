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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.wso2.andes.server.cluster.coordination.rdbms;

import org.apache.log4j.Logger;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.coordination.ClusterNotificationHandler;
import org.wso2.andes.server.cluster.coordination.ClusterNotification;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The ClusterEventReaderTask runs periodically and checks for unread cluster notifications.
 */
public class ClusterEventReaderTask implements Runnable {

    /**
     * Logger to log information.
     */
    private static final Logger log = Logger.getLogger(ClusterEventReaderTask.class);

    /**
     * Andes context store instance to perform operations on the context store.
     */
    private AndesContextStore andesContextStore = AndesContext.getInstance().getAndesContextStore();

    /**
     * A lister to listen to cluster events such as queue, binding, exchange, and subscription changes.
     */
    private ClusterNotificationHandler clusterNotificationHandler;

    /**
     * A map of handlers to forward a cluster notification that is received.
     */
    private Map<String, RDBMSBasedClusterNotificationHandler> rdbmsBasedClusterNotificationHandlers;

    /**
     * The node id of this node.
     */
    private String nodeID;

    /**
     * Initialize the task with the listeners.
     */
    public ClusterEventReaderTask() {

        nodeID = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();
        RDBMSBasedEventListenerCreator listenerCreator = new RDBMSBasedEventListenerCreator();
        clusterNotificationHandler = new ClusterNotificationHandler();
        clusterNotificationHandler.addQueueListener(listenerCreator.getQueueListener());
        clusterNotificationHandler.addBindingListener(listenerCreator.getBindingListener());
        clusterNotificationHandler.addExchangeListener(listenerCreator.getExchangeListener());
        clusterNotificationHandler.addSubscriptionListener(listenerCreator.getSubscriptionListener());
        registerHandlers();
    }

    /**
     * Registers handlers for all cluster event types that could be received.
     */
    public void registerHandlers() {
        rdbmsBasedClusterNotificationHandlers = new HashMap<>();
        rdbmsBasedClusterNotificationHandlers.put(ClusterNotification.QUEUE_ADDED, new ClusterQueueAdditionHandler());
        rdbmsBasedClusterNotificationHandlers.put(ClusterNotification.QUEUE_DELETED, new ClusterQueueDeletionHandler());
        rdbmsBasedClusterNotificationHandlers.put(ClusterNotification.QUEUE_PURGED, new ClusterQueuePurgeHandler());
        rdbmsBasedClusterNotificationHandlers
                .put(ClusterNotification.BINDING_ADDED, new ClusterBindingAdditionHandler());
        rdbmsBasedClusterNotificationHandlers
                .put(ClusterNotification.BINDING_DELETED, new
                        ClusterBindingDeletionHandler());
        rdbmsBasedClusterNotificationHandlers
                .put(ClusterNotification.EXCHANGE_ADDED, new ClusterExchangeAdditionHandler());
        rdbmsBasedClusterNotificationHandlers
                .put(ClusterNotification.EXCHANGE_DELETED, new ClusterExchangeDeletionHandler());
        rdbmsBasedClusterNotificationHandlers
                .put(ClusterNotification.SUBSCRIPTION_ADDED, new ClusterSubscriptionAdditionHandler());
        rdbmsBasedClusterNotificationHandlers
                .put(ClusterNotification.SUBSCRIPTION_DELETED, new ClusterSubscriptionDeletionHandler());
        rdbmsBasedClusterNotificationHandlers
                .put(ClusterNotification.SUBSCRIPTION_DISCONNECTED, new ClusterSubscriptionDisconnectionHandler());
        rdbmsBasedClusterNotificationHandlers
                .put(ClusterNotification.SUBSCRIPTION_MERGED, new ClusterSubscriptionMergeHandler());
    }

    /**
     * Read all the unread cluster event from the store, and let the cluster event handler handle it according to the
     * event type.
     */
    @Override
    public void run() {
        try {
            List<ClusterNotification> clusterEvents = andesContextStore.readClusterNotifications(nodeID);
            if (log.isDebugEnabled()){
                log.debug("Cluster event reader received " + clusterEvents.size() + " events.");
            }
            if (!clusterEvents.isEmpty()) {
                for (ClusterNotification event : clusterEvents) {
                    //We need to skip processing a cluster notification which was sent by this node since it has
                    // already been processed.
                    if (!nodeID.equals(event.getOriginatedNode())) {
                        rdbmsBasedClusterNotificationHandlers.get(event.getChangeType()).handleNotification(event
                                .getEncodedObjectAsString(), clusterNotificationHandler);
                    }
                }
            }
        } catch (Throwable e) {
            log.warn("Could not read cluster events. Events will not be reflected in the node until next attempt.", e);
        }
    }
}
