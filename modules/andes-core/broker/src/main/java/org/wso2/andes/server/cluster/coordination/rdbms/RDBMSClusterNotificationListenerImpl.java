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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.log4j.Logger;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesContextInformationManager;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.ClusterNotificationListener;
import org.wso2.andes.kernel.disruptor.inbound.InboundEventManager;
import org.wso2.andes.kernel.subscription.AndesSubscriptionManager;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.coordination.BindingNotificationHandler;
import org.wso2.andes.server.cluster.coordination.ClusterNotification;
import org.wso2.andes.server.cluster.coordination.ClusterNotificationListenerManager;
import org.wso2.andes.server.cluster.coordination.DBSyncNotificationHandler;
import org.wso2.andes.server.cluster.coordination.ExchangeNotificationHandler;
import org.wso2.andes.server.cluster.coordination.QueueNotificationHandler;
import org.wso2.andes.server.cluster.coordination.SubscriptionNotificationHandler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * This is the ClusterNotificationListenerManager implementation for RDBMS. It periodically polls
 * events from DB and trigger necessary handlers. Changes related to Message routers, queues, bindings
 * and subscriptions are listened and handled.
 */
public class RDBMSClusterNotificationListenerImpl implements ClusterNotificationListenerManager {


    /**
     * Logger to log information.
     */
    private static final Logger log = Logger.getLogger(RDBMSClusterNotificationListenerImpl.class);

    /**
     * Executor service for scheduling lister task on DB polling for notifications
     */
    ScheduledExecutorService scheduledExecutorService;

    /**
     * Andes context store instance to perform operations on the context store.
     */
    private AndesContextStore andesContextStore;

    /**
     * Cluster notification handlers w:r:t Notified artifact
     */
    private Map<ClusterNotificationListener.NotifiedArtifact, ClusterNotificationListener> clusterNotificationListeners;

    /**
     * The node id of this node.
     */
    private String nodeID;


    /**
     * Create a RDBMS based cluster notification listener. This listens for events published to DB.
     *
     * @param contextStore store to read notifications from
     */
    public RDBMSClusterNotificationListenerImpl(AndesContextStore contextStore) {

        this.andesContextStore = contextStore;
        this.nodeID = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();
        clusterNotificationListeners = new HashMap<>(5);
    }

    /**
     * Register a notification handler for a specific artifact
     *
     * @param artifactType         artifact to register notification handler for
     * @param notificationListener ClusterNotificationListener that
     *                             generates inbound notification events and feed to kernel
     */
    private void registerNotificationHandler(ClusterNotificationListener.NotifiedArtifact artifactType,
                                             ClusterNotificationListener notificationListener) {

        clusterNotificationListeners.put(artifactType, notificationListener);
    }

    /**
     * Dispatch cluster notification to relevant handler
     *
     * @param notification notification to dispatch
     */
    private void dispatchClusterNotification(ClusterNotification notification) {
        //We need to skip processing a cluster notification which was sent by this node since it has
        // already been processed.
        if (!nodeID.equals(notification.getOriginatedNode())) {
            ClusterNotificationListener.NotifiedArtifact notifiedArtifact =
                    ClusterNotificationListener.NotifiedArtifact.valueOf(notification.getNotifiedArtifact());
            ClusterNotificationListener handlerToDispatch = clusterNotificationListeners.get
                    (notifiedArtifact);
            handlerToDispatch.handleClusterNotification(notification);
        }
    }

    /**
     * Task for reading all the unread cluster event from the store and dispatch to correct cluster notification
     * handlers
     */
    private class ClusterEventReaderTask implements Runnable {

        @Override
        public void run() {
            try {
                List<ClusterNotification> clusterEvents = andesContextStore.readClusterNotifications(nodeID);
                if (log.isDebugEnabled()) {
                    log.debug("Cluster event reader received " + clusterEvents.size() + " events.");
                }
                if (!clusterEvents.isEmpty()) {
                    for (ClusterNotification event : clusterEvents) {
                        dispatchClusterNotification(event);
                    }
                }
            } catch (Throwable e) {
                log.warn("Could not read cluster events. Events will not be reflected in the node until next attempt" +
                        ".", e);
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void initializeListener(InboundEventManager inboundEventManager, AndesSubscriptionManager
            subscriptionManager,
                                   AndesContextInformationManager contextInformationManager) throws AndesException {

        // Clear cluster notifications stored in the database at startup
        andesContextStore.clearClusterNotifications(nodeID);

        //register cluster notification handlers for each artifact
        registerNotificationHandler(ClusterNotificationListener.NotifiedArtifact.MessageRouter,
                new ExchangeNotificationHandler(contextInformationManager, inboundEventManager));
        registerNotificationHandler(ClusterNotificationListener.NotifiedArtifact.Queue,
                new QueueNotificationHandler(contextInformationManager, inboundEventManager));
        registerNotificationHandler(ClusterNotificationListener.NotifiedArtifact.Binding,
                new BindingNotificationHandler(contextInformationManager, inboundEventManager));
        registerNotificationHandler(ClusterNotificationListener.NotifiedArtifact.Subscription,
                new SubscriptionNotificationHandler(subscriptionManager, inboundEventManager));
        registerNotificationHandler(ClusterNotificationListener.NotifiedArtifact.DBUpdate,
                new DBSyncNotificationHandler());

        //and schedule a periodic task to read cluster events
        // from the store if cluster event sync mode is set to RDBMS.
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("ClusterEventReaderTask-%d").build();
        int clusterEventReaderInterval = AndesConfigurationManager.readValue(AndesConfiguration
                .CLUSTER_EVENT_SYNC_INTERVAL);
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(threadFactory);
        scheduledExecutorService.scheduleWithFixedDelay(new ClusterEventReaderTask(),
                clusterEventReaderInterval, clusterEventReaderInterval, TimeUnit.MILLISECONDS);
        log.info("RDBMS cluster event listener started with an interval of: " + clusterEventReaderInterval + "ms.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reInitializeListener() throws AndesException {
        InboundEventManager eventManager = AndesContext.getInstance().getInboundEventManager();
        AndesSubscriptionManager subscriptionManager = AndesContext.getInstance().getAndesSubscriptionManager();
        AndesContextInformationManager contextInformationManager = AndesContext.getInstance()
                .getAndesContextInformationManager();
        initializeListener(eventManager, subscriptionManager, contextInformationManager);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearAllClusterNotifications() throws AndesException {
        andesContextStore.clearClusterNotifications();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stopListener() throws AndesException {
        scheduledExecutorService.shutdown();
        log.info("RDBMS cluster event listener stopped.");
    }
}
