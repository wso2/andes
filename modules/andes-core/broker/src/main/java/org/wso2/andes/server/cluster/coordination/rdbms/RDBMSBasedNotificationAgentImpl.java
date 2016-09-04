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

package org.wso2.andes.server.cluster.coordination.rdbms;


import org.apache.log4j.Logger;
import org.wso2.andes.kernel.AndesBinding;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.ClusterNotificationListener;
import org.wso2.andes.kernel.router.AndesMessageRouter;
import org.wso2.andes.kernel.subscription.AndesSubscription;
import org.wso2.andes.kernel.subscription.StorageQueue;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.coordination.ClusterNotification;
import org.wso2.andes.server.cluster.coordination.ClusterNotificationAgent;

import java.util.List;

/**
 * This class represents a ClusterNotificationAgent implementation which uses
 * Underlying Database for notifying changes to the other nodes.
 */
public class RDBMSBasedNotificationAgentImpl implements ClusterNotificationAgent {


    /**
     * Logger object to log information.
     */
    private static final Logger log = Logger.getLogger(RDBMSBasedNotificationAgentImpl.class);

    /**
     * The context store instance which is used to store events.
     */
    private AndesContextStore contextStore;

    /**
     * The node id of this node.
     */
    private String localNodeID;

    /**
     * Create a RDBMS based ClusterNotificationAgent
     *
     * @param contextStore store for storing the notification
     */
    public RDBMSBasedNotificationAgentImpl(AndesContextStore contextStore) {
        this.contextStore = contextStore;
        this.localNodeID = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyMessageRouterChange(AndesMessageRouter messageRouter, ClusterNotificationListener
            .MessageRouterChange changeType) throws AndesException {
        ClusterNotification clusterNotification = new ClusterNotification(
                messageRouter.encodeAsString(),
                ClusterNotificationListener.NotifiedArtifact.MessageRouter.toString(),
                changeType.toString(),
                "Message Router Notification Message : " + changeType.toString(),
                localNodeID);

        if (log.isDebugEnabled()) {
            log.debug("Sending GOSSIP: " + clusterNotification.getEncodedObjectAsString());
        }
        publishNotificationToDB(clusterNotification);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyQueueChange(StorageQueue storageQueue, ClusterNotificationListener.QueueChange changeType)
            throws AndesException {
        ClusterNotification clusterNotification = new ClusterNotification(
                storageQueue.encodeAsString(),
                ClusterNotificationListener.NotifiedArtifact.Queue.toString(),
                changeType.toString(),
                "Queue Notification Message : " + changeType.toString(),
                localNodeID);

        if (log.isDebugEnabled()) {
            log.debug("Sending GOSSIP: " + clusterNotification.getEncodedObjectAsString());
        }
        publishNotificationToDB(clusterNotification);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyBindingsChange(AndesBinding binding, ClusterNotificationListener.BindingChange changeType)
            throws AndesException {
        ClusterNotification clusterNotification = new ClusterNotification(
                binding.encodeAsString(),
                ClusterNotificationListener.NotifiedArtifact.Binding.toString(),
                changeType.toString(),
                "Binding Notification Message : " + changeType.toString(),
                localNodeID);

        if (log.isDebugEnabled()) {
            log.debug("GOSSIP: " + clusterNotification.getEncodedObjectAsString());
        }
        publishNotificationToDB(clusterNotification);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifySubscriptionsChange(AndesSubscription subscription, ClusterNotificationListener.
            SubscriptionChange changeType) throws AndesException {
        ClusterNotification clusterNotification = new ClusterNotification(
                subscription.encodeAsStr(),
                ClusterNotificationListener.NotifiedArtifact.Subscription.toString(),
                changeType.toString(),
                "Subscription Notification Message : " + changeType.toString(),
                localNodeID);

        if (log.isDebugEnabled()) {
            log.debug("Sending GOSSIP: " + clusterNotification.getEncodedObjectAsString());
        }
        publishNotificationToDB(clusterNotification);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyAnyDBChange() throws AndesException {
        ClusterNotification clusterNotification = new ClusterNotification("", ClusterNotificationListener
                .NotifiedArtifact.DBUpdate.toString(), "", "DBSyncEvent", localNodeID);
        publishNotificationToDB(clusterNotification);
    }

    /**
     * Store notification in the DB. Duplicate the cluster notification for all nodes in
     * the cluster and store them destined to the respective  to each node.
     *
     * @param event notification to store
     * @throws AndesException
     */
    private void publishNotificationToDB(ClusterNotification event) throws AndesException {
        List<String> clusterNodes = AndesContext.getInstance().getClusterAgent().getAllNodeIdentifiers();
        contextStore.storeClusterNotification(clusterNodes, localNodeID, event.getNotifiedArtifact(), event
                .getChangeType(), event.getEncodedObjectAsString(), event.getDescription());
        if (log.isDebugEnabled()) {
            log.debug("Cluster notification " + event.getEncodedObjectAsString() + " stored in Database");
        }
    }
}
