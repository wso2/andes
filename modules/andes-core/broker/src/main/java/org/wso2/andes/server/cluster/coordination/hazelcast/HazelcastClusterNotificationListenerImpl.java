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
import com.hazelcast.core.MessageListener;
import org.apache.commons.lang.StringUtils;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesContextInformationManager;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.disruptor.inbound.InboundEventManager;
import org.wso2.andes.kernel.subscription.AndesSubscriptionManager;
import org.wso2.andes.server.cluster.coordination.BindingNotificationHandler;
import org.wso2.andes.server.cluster.coordination.ClusterNotification;
import org.wso2.andes.server.cluster.coordination.ClusterNotificationListenerManager;
import org.wso2.andes.server.cluster.coordination.CoordinationConstants;
import org.wso2.andes.server.cluster.coordination.DBSyncNotificationHandler;
import org.wso2.andes.server.cluster.coordination.ExchangeNotificationHandler;
import org.wso2.andes.server.cluster.coordination.QueueNotificationHandler;
import org.wso2.andes.server.cluster.coordination.SubscriptionNotificationHandler;

public class HazelcastClusterNotificationListenerImpl implements ClusterNotificationListenerManager {

    /**
     * Distributed topic to sent among cluster nodes to run andes recover task.
     */
    private ITopic<ClusterNotification> dbSyncNotifierChannel;

    /**
     * Distributed topic to send cluster notifications among cluster nodes.
     */
    private ITopic<ClusterNotification> clusterNotifierChannel;

    private ITopic<ClusterNotification> dynamicDiscoveryNotifierChannel;

    /**
     * IDs of subscribers registered for Hazelcast topics
     */
    private String dbSyncNotificationListenerId;
    private String clusterEventListenerId;
    private String dynamicDiscoveryEventListenerId;

    /**
     * Hazelcast agent for forwarding HZ related requests
     */
    private HazelcastAgent hazelcastAgent;

    /**
     * Defines the maximum number of messages that will be read at a single try from a Hazelcast reliable topic.
     * This value is set to a very low number since these reliable topics only handle cluster notifications on
     * subscription changes, exchange changes, etc.
     * and the frequency of messages being published is very low.
     */
    private final int HAZELCAST_RELIABLE_TOPIC_READ_BACH_SIZE = 5;

    /**
     * Defines the maximum number of messages that can be stored in the ring buffer associated with a
     * Hazelcast Reliable Topic. The buffer could be initialized with a somewhat low number since these reliable topics
     * only handle cluster notifications on subscription changes, exchange changes, etc. which are a very not so
     * frequent. But, there could be extreme and rare situations where subscriptions, bindings, etc. change at a very
     * high rate and to be able to tolerate that, the capacity is kept at 1000.
     */
    private final int HAZELCAST_RING_BUFFER_CAPACITY = 1000;

    /**
     * Disables statistics on the messages published to Hazelcast Reliable Topics.
     * We don't need statistics on the messages that are published, therefore, we have disabled statistics.
     */
    private final boolean ENABLE_STATISTICS = false;


    /**
     * Create a Hazelcast based cluster notification implementation
     *
     * @param hazelcastAgent Hazelcast agent of broker
     */
    public HazelcastClusterNotificationListenerImpl(HazelcastAgent hazelcastAgent) {
        this.hazelcastAgent = hazelcastAgent;
        addTopics();
    }

    @Override
    public void initializeListener(InboundEventManager inboundEventManager,
                                   AndesSubscriptionManager subscriptionManager,
                                   AndesContextInformationManager contextInformationManager) throws AndesException {

        addTopicListeners(inboundEventManager, subscriptionManager, contextInformationManager);
    }

    /**
     * Recreate Hazelcast topics and add listeners.
     */
    public void reInitializeListener() throws AndesException {
        addTopics();
        InboundEventManager eventManager = AndesContext.getInstance().getInboundEventManager();
        AndesSubscriptionManager subscriptionManager = AndesContext.getInstance().getAndesSubscriptionManager();
        AndesContextInformationManager contextInformationManager = AndesContext.getInstance()
                .getAndesContextInformationManager();
        addTopicListeners(eventManager, subscriptionManager, contextInformationManager);
    }

    @Override
    public void clearAllClusterNotifications() throws AndesException {
        //Do nothing since this is handle by hazelcast itself
    }

    @Override
    public void stopListener() throws AndesException {
        //Do nothing, this will be handled by shutting down the hazelcast instance.
    }


    private void addTopics() {
        // Defines the time it takes for a message published to a Hazelcast reliable topic to be expired.
        // The messages that are published to these topics should ideally be read at the same time. One instance
        // where this would not happen is when a node gets disconnected. Since all the messages that are published
        // to these topics are stored in the database, this situation is handled by synchronizing the information in
        // the databases when the node recovers. Therefore, we do not need undelivered messages to delivered
        // after a while. Therefore, we need messages to be held in the buffer onle for a very little time.
        int hazelcastRingBufferTTL = AndesConfigurationManager.readValue(AndesConfiguration
                .COORDINATION_CLUSTER_NOTIFICATION_TIMEOUT);

        // Creating the channel for communicating database sync notification to run andes recovery task.
        this.dbSyncNotifierChannel = hazelcastAgent.createReliableTopic(
                CoordinationConstants.HAZELCAST_DB_SYNC_NOTIFICATION_TOPIC_NAME,
                ENABLE_STATISTICS, HAZELCAST_RELIABLE_TOPIC_READ_BACH_SIZE, HAZELCAST_RING_BUFFER_CAPACITY,
                hazelcastRingBufferTTL);

        // Creating the channel for publishing and subscribing cluster notifications.
        this.clusterNotifierChannel = hazelcastAgent.createReliableTopic(
                CoordinationConstants.HAZELCAST_CLUSTER_EVENT_NOTIFIER_TOPIC_NAME,
                ENABLE_STATISTICS, HAZELCAST_RELIABLE_TOPIC_READ_BACH_SIZE, HAZELCAST_RING_BUFFER_CAPACITY,
                hazelcastRingBufferTTL);

        this.dynamicDiscoveryNotifierChannel = hazelcastAgent.createReliableTopic(
                CoordinationConstants.HAZELCAST_CLUSTER_DYNAMIC_DISCOVERY_NOTIFIER_TOPIC_NAME,
                ENABLE_STATISTICS, HAZELCAST_RELIABLE_TOPIC_READ_BACH_SIZE, HAZELCAST_RING_BUFFER_CAPACITY,
                hazelcastRingBufferTTL);

    }

    private void addTopicListeners(InboundEventManager inboundEventManager,
                                   AndesSubscriptionManager subscriptionManager,
                                   AndesContextInformationManager contextInformationManager) throws AndesException {

        // Register cluster notification listener and add handlers assigned for different types of artifacts.
        HZBasedClusterNotificationListener hzBasedClusterNotificationListener = new
                HZBasedClusterNotificationListener();
        hzBasedClusterNotificationListener.addSubscriptionNotificationHandler(new SubscriptionNotificationHandler
                (subscriptionManager, inboundEventManager));
        hzBasedClusterNotificationListener.addBindingNotificationHandler(new BindingNotificationHandler
                (contextInformationManager, inboundEventManager));
        hzBasedClusterNotificationListener.addExchangeNotificationHandler(new ExchangeNotificationHandler
                (contextInformationManager, inboundEventManager));
        hzBasedClusterNotificationListener.addQueueNotificationHandler(new QueueNotificationHandler
                (contextInformationManager, inboundEventManager));

        clusterEventListenerId = checkAndRegisterListerToTopic(clusterNotifierChannel,
                hzBasedClusterNotificationListener, clusterEventListenerId);

        // Register DB sync notification listener.
        HZBasedDatabaseSyncNotificationListener HZBasedDatabaseSyncNotificationListener = new
                HZBasedDatabaseSyncNotificationListener();
        HZBasedDatabaseSyncNotificationListener.addHandler(new DBSyncNotificationHandler());

        dbSyncNotificationListenerId = checkAndRegisterListerToTopic(dbSyncNotifierChannel,
                HZBasedDatabaseSyncNotificationListener, dbSyncNotificationListenerId);


     HzBasedDynamicDiscoveryListener hzBasedDynamicDiscoveryListener = new HzBasedDynamicDiscoveryListener();

        dynamicDiscoveryEventListenerId = checkAndRegisterListerToTopic(dynamicDiscoveryNotifierChannel,
                hzBasedDynamicDiscoveryListener, dynamicDiscoveryEventListenerId);

    }

    /**
     * Check if there is a listener registered by given id. If registered,
     * remove and register the new listener
     *
     * @param topic      Hazelcast topic
     * @param listener   Listener to register
     * @param listenerId ID of the listener to check if there is an existing
     * @return ID of the registered subscriber
     */
    private String checkAndRegisterListerToTopic(ITopic<ClusterNotification> topic,
                                                 MessageListener<ClusterNotification> listener,
                                                 String listenerId) {
        if (StringUtils.isNotEmpty(listenerId)) {
            topic.removeMessageListener(listenerId);
        }
        return topic.addMessageListener(listener);

    }

    /**
     * Get channel for publishing and subscribing cluster notifications.
     *
     * @return {@link ITopic} for cluster notifications
     */
    public ITopic<ClusterNotification> getClusterNotificationChannel() {
        return clusterNotifierChannel;
    }

    /**
     * Get channel for publishing and subscribing db sync events.
     *
     * @return {@link ITopic} for db sync events
     */
    public ITopic<ClusterNotification> getDBSyncNotificationChannel() {
        return dbSyncNotifierChannel;
    }


    public ITopic<ClusterNotification> getDynamicDiscoveryNotificationChannel() {
        return dynamicDiscoveryNotifierChannel;
    }
}
