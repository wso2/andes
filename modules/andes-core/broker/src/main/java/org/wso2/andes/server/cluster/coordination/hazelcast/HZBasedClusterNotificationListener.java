/*
 * Copyright (c)2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.wso2.andes.server.cluster.coordination.BindingNotificationHandler;
import org.wso2.andes.server.cluster.coordination.ClusterNotification;
import org.wso2.andes.server.cluster.coordination.ExchangeNotificationHandler;
import org.wso2.andes.server.cluster.coordination.QueueNotificationHandler;
import org.wso2.andes.server.cluster.coordination.SubscriptionNotificationHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * HZBasedClusterNotificationListener receives notifications related to queue, binding,messaged router and
 * subscriptions changes through {@link com.hazelcast.core.ITopic} and forwards the notification to relevant handlers.
 */
public class HZBasedClusterNotificationListener implements MessageListener {

    private static Log log = LogFactory.getLog(HZBasedDatabaseSyncNotificationListener.class);

    /**
     * Listeners interested in exchange changes.
     */
    private List<ExchangeNotificationHandler> exchangeNotificationHandlerList = new ArrayList<>();

    /**
     * Listeners interested in queue changes.
     */
    private List<QueueNotificationHandler> queueNotificationHandlerList = new ArrayList<>();

    /**
     * Listeners interested in binding changes.
     */
    private List<BindingNotificationHandler> bindingNotificationHandlerList = new ArrayList<>();

    /**
     * Listeners interested in subscription changes.
     */
    private List<SubscriptionNotificationHandler> subscriptionNotificationHandlerList = new ArrayList<>();

    /**
     * Register a listener interested in exchange(message router) changes within the cluster.
     *
     * @param handler listener to be registered
     */
    public void addExchangeNotificationHandler(ExchangeNotificationHandler handler) {
        exchangeNotificationHandlerList.add(handler);
    }

    /**
     * Register a listener interested in queue changes within the cluster.
     *
     * @param handler listener to be registered
     */
    public void addQueueNotificationHandler(QueueNotificationHandler handler) {
        queueNotificationHandlerList.add(handler);
    }

    /**
     * Register a listener interested in binding changes within the cluster.
     *
     * @param handler listener to be registered
     */
    public void addBindingNotificationHandler(BindingNotificationHandler handler) {
        bindingNotificationHandlerList.add(handler);
    }

    /**
     * Register a listener interested in subscription changes within the cluster.
     *
     * @param handler listener to be registered
     */
    public void addSubscriptionNotificationHandler(SubscriptionNotificationHandler handler) {
        subscriptionNotificationHandlerList.add(handler);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onMessage(Message message) {

        if (!message.getPublishingMember().localMember()) {
            ClusterNotification clusterNotification = (ClusterNotification) message.getMessageObject();
            if (log.isDebugEnabled()) {
                log.debug("Handling cluster gossip: received cluster notification: " + clusterNotification
                        .getEncodedObjectAsString() + " for artifact: " + clusterNotification.getNotifiedArtifact()
                          + " with change type: " + clusterNotification.getChangeType());
            }
            //Forward the cluster notification to the relevant handler depending on the artifact type.
            switch (clusterNotification.getNotifiedArtifact()) {
                case "MessageRouter":
                    for (ExchangeNotificationHandler handler : exchangeNotificationHandlerList) {
                        handler.handleClusterNotification(clusterNotification);
                    }
                    break;
                case "Binding":
                    for (BindingNotificationHandler handler : bindingNotificationHandlerList) {
                        handler.handleClusterNotification(clusterNotification);
                    }
                    break;
                case "Queue":
                    for (QueueNotificationHandler handler : queueNotificationHandlerList) {
                        handler.handleClusterNotification(clusterNotification);
                    }
                    break;
                case "Subscription":
                    for (SubscriptionNotificationHandler handler : subscriptionNotificationHandlerList) {
                        handler.handleClusterNotification(clusterNotification);
                    }
                    break;
                default:
                    log.error("Unknown cluster event type: " + clusterNotification.getNotifiedArtifact());
                    break;
            }
        }
    }
}
