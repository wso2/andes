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

package org.wso2.andes.server.cluster.coordination.hazelcast;

import com.hazelcast.core.ITopic;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.kernel.router.AndesMessageRouter;
import org.wso2.andes.kernel.subscription.*;
import org.wso2.andes.kernel.subscription.AndesSubscription;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.coordination.ClusterNotification;
import org.wso2.andes.server.cluster.coordination.ClusterNotificationAgent;

import java.util.Map;

/**
 * This class represents a ClusterNotificationAgent implementation which uses
 * Hazelcast for notifying changes to the other nodes.
 */
public class HazelcastBasedNotificationAgentImpl implements ClusterNotificationAgent {


    private static Log log = LogFactory.getLog(HazelcastBasedNotificationAgentImpl.class);

    /**
     * This notification agent uses different channels (ITopics) to notify changes on different
     * artifacts. this map keeps which channel is used for which artifact type
     */
    private Map<ClusterNotificationListener.NotifiedArtifact, ITopic<ClusterNotification>> channelMAP;

    /**
     * ID of the local node
     */
    private String localNodeID;

    /**
     * Create a Hazelcast based ClusterNotificationAgent
     *
     * @param channelInfoMap Map of Map<ClusterNotificationListener.NotifiedArtifact,ITopic<ClusterNotification>>
     *                       having channels to publish according to artifact
     */
    public HazelcastBasedNotificationAgentImpl(Map<ClusterNotificationListener.NotifiedArtifact,
            ITopic<ClusterNotification>> channelInfoMap) {
        this.localNodeID = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();
        this.channelMAP = channelInfoMap;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyMessageRouterChange(AndesMessageRouter messageRouter,
                                          ClusterNotificationListener.MessageRouterChange changeType) throws
            AndesException {

        ClusterNotification clusterNotification = new ClusterNotification(
                messageRouter.encodeAsString(),
                ClusterNotificationListener.NotifiedArtifact.MessageRouter.toString(),
                changeType.toString(),
                "Message Router Notification Message : " + changeType.toString(),
                localNodeID);

        if (log.isDebugEnabled()) {
            log.debug("Sending GOSSIP: " + clusterNotification.getEncodedObjectAsString());
        }
        try {
            channelMAP.get(ClusterNotificationListener.NotifiedArtifact.MessageRouter).publish(clusterNotification);
        } catch (Exception e) {
            log.error("Error while sending exchange change notification"
                    + clusterNotification.getEncodedObjectAsString(), e);
            throw new AndesException("Error while sending exchange change notification"
                    + clusterNotification.getEncodedObjectAsString(), e);
        }
    }

    /***
     * {@inheritDoc}
     *
     * @param andesQueue changed queue
     * @param changeType what type of change has happened
     * @throws AndesException
     */
    @Override
    public void notifyQueueChange(StorageQueue andesQueue, ClusterNotificationListener.QueueChange changeType)
            throws AndesException {

        ClusterNotification clusterNotification = new ClusterNotification(
                andesQueue.encodeAsString(),
                ClusterNotificationListener.NotifiedArtifact.Queue.toString(),
                changeType.toString(),
                "Queue Notification Message : " + changeType.toString(),
                localNodeID);

        if (log.isDebugEnabled()) {
            log.debug("Sending GOSSIP: " + clusterNotification.getEncodedObjectAsString());
        }
        try {
            channelMAP.get(ClusterNotificationListener.NotifiedArtifact.Queue).publish(clusterNotification);
        } catch (Exception e) {
            log.error("Error while sending queue change notification : "
                    + clusterNotification.getEncodedObjectAsString(), e);
            throw new AndesException("Error while sending queue change notification : "
                    + clusterNotification.getEncodedObjectAsString(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
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
        try {
            channelMAP.get(ClusterNotificationListener.NotifiedArtifact.Binding).publish(clusterNotification);
        } catch (Exception e) {
            log.error("Error while sending binding change notification"
                    + clusterNotification.getEncodedObjectAsString(), e);
            throw new AndesException("Error while sending binding change notification"
                    + clusterNotification.getEncodedObjectAsString(), e);
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void notifySubscriptionsChange(AndesSubscription subscription,
                                          ClusterNotificationListener.SubscriptionChange changeType) throws
            AndesException {

        ClusterNotification clusterNotification = new ClusterNotification(
                subscription.encodeAsStr(),
                ClusterNotificationListener.NotifiedArtifact.Subscription.toString(),
                changeType.toString(),
                "Subscription Notification Message : " + changeType.toString(),
                localNodeID);

        //check hazelcast instance active because hazelcast bundle get deactivated before notification send
        if (log.isDebugEnabled()) {
            log.debug("Sending GOSSIP: " + clusterNotification.getEncodedObjectAsString());
        }
        try {
            this.channelMAP.get(ClusterNotificationListener.NotifiedArtifact.Subscription).
                    publish(clusterNotification);
        } catch (Exception ex) {
            log.error("Error while sending subscription change notification : "
                    + clusterNotification.getEncodedObjectAsString(), ex);
            throw new AndesException("Error while sending queue change notification : "
                    + clusterNotification.getEncodedObjectAsString(), ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyAnyDBChange() throws AndesException {
        ClusterNotification clusterNotification = new ClusterNotification("", ClusterNotificationListener
                .NotifiedArtifact.DBUpdate.toString(), "", "DBSyncEvent", localNodeID);
        try {
            channelMAP.get(ClusterNotificationListener.NotifiedArtifact.DBUpdate).publish(clusterNotification);
            if (log.isDebugEnabled()) {
                log.debug("Requested for DB sync across the cluster.");
            }
        } catch (Exception e) {
            log.error("Error while sending db sync notification"
                    + clusterNotification.getEncodedObjectAsString(), e);
            throw new AndesException("Error while sending db sync notification"
                    + clusterNotification.getEncodedObjectAsString(), e);
        }
    }
}
