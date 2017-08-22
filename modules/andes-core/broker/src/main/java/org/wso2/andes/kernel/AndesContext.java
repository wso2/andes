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

import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.StoreConfiguration;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.disruptor.inbound.InboundEventManager;
import org.wso2.andes.kernel.registry.MessageRouterRegistry;
import org.wso2.andes.kernel.registry.StorageQueueRegistry;
import org.wso2.andes.kernel.subscription.AndesSubscriptionManager;
import org.wso2.andes.server.cluster.ClusterAgent;
import org.wso2.andes.server.cluster.coordination.ClusterNotificationListenerManager;

import java.util.List;

/**
 * AndesContext is used to pass instances created and configurations read through component level
 * to Andes. A place holder class.
 */
public class AndesContext {

    private AndesSubscriptionManager andesSubscriptionManager;
    private AndesContextStore andesContextStore;
    private StoreConfiguration storeConfiguration;
    private boolean isClusteringEnabled;
    private AMQPConstructStore AMQPConstructStore;
    private static AndesContext instance = new AndesContext();
    private MessageStore messageStore;
    private int deliveryTimeoutForMessage;

    /**
     * This is mainly used by Cluster Manager to manger cluster communication
     */
    private ClusterAgent clusterAgent;

    /**
     * Registry for keeping storage queues created in Andes
     */
    private StorageQueueRegistry storageQueueRegistry;

    /**
     * Registry for keeping AndesMessageRouter instances. This is similar
     * to an exchange registry
     */
    private MessageRouterRegistry messageRouterRegistry;

    /**
     * InboundEventManager instance for pumping inbound events into Inbound Disruptor
     */
    private InboundEventManager inboundEventManager;

    /**
     * AndesContextInformationManager instance for managing context information
     * inside broker
     */
    private AndesContextInformationManager andesContextInformationManager;

    /**
     * Holds the listener handling cluster notifications
     */
    private ClusterNotificationListenerManager clusterNotificationListenerManager;

    /**
     * Getter for cluster agent
     *
     * @return cluster agent for this node if one is available, else null
     */
    public ClusterAgent getClusterAgent() {
        return clusterAgent;
    }

    /**
     * Setter for cluster agent.
     *
     * @param clusterAgent
     */
    public void setClusterAgent(ClusterAgent clusterAgent) {
        this.clusterAgent = clusterAgent;
    }

    /**
     * Get virtual host configuration object
     * @return StoreConfiguration
     */
    public StoreConfiguration getStoreConfiguration() {
        return storeConfiguration;
    }

    /**
     * Get andesSubscriptionManager instance. All subscription management is done by
     * this object.
     * @return AndesSubscriptionManager instance
     */
    public AndesSubscriptionManager getAndesSubscriptionManager() {
        return andesSubscriptionManager;
    }

    /**
     * Set andesSubscriptionManager instance
     * @param andesSubscriptionManager instance to set
     */
    public void setAndesSubscriptionManager(AndesSubscriptionManager andesSubscriptionManager) {
        this.andesSubscriptionManager = andesSubscriptionManager;
    }

    /**
     * set andes context store
     *
     * @param andesContextStore context store to store
     */
    public void setAndesContextStore(AndesContextStore andesContextStore) {
        this.andesContextStore = andesContextStore;
    }

    /**
     * get andes context store
     *
     * @return context store
     */
    public AndesContextStore getAndesContextStore() {
        return this.andesContextStore;
    }

    /**
     * get andes context instance
     *
     * @return andes context
     */
    public static AndesContext getInstance() {
        return instance;
    }

    /**
     * get if clustering is enabled
     *
     * @return if clustering is on
     */
    public boolean isClusteringEnabled() {
        return isClusteringEnabled;
    }

    /**
     * set if clustering is enabled. This is set by activator
     * of Andes component
     *
     * @param isClusteringEnabled if clustering is enabled
     */
    public void setClusteringEnabled(boolean isClusteringEnabled) {
        this.isClusteringEnabled = isClusteringEnabled;
    }

    /**
     * set AMQP constructs store instance
     *
     * @param AMQPConstructStore AMQP constructs store
     */
    public void setAMQPConstructStore(AMQPConstructStore AMQPConstructStore) {
        this.AMQPConstructStore = AMQPConstructStore;
    }

    /**
     * get AMQP construct store
     *
     * @return AMQP construct store
     */
    public AMQPConstructStore getAMQPConstructStore() {
        return AMQPConstructStore;
    }

    /**
     *  get thrift server host ip
     * @return  thrift server host ip
     */
    public String getThriftServerHost() {
        return AndesConfigurationManager.readValue(AndesConfiguration.COORDINATION_THRIFT_SERVER_HOST);
    }

    /**
     * get thrift server port
     *
     * @return The port value
     */
    public Integer getThriftServerPort() {
        return AndesConfigurationManager.readValue(AndesConfiguration.COORDINATION_THRIFT_SERVER_PORT);
    }

    /**
     * Read configuration properties related to persistent stores and construct semantic object
     * for simple reference.
     */
    public void constructStoreConfiguration() {

        storeConfiguration = new StoreConfiguration();

        storeConfiguration.setMessageStoreClassName((String) AndesConfigurationManager.readValue
                (AndesConfiguration.PERSISTENCE_MESSAGE_STORE_HANDLER));

        List<String> messageStoreProperties = AndesConfigurationManager.readValueList
                (AndesConfiguration.LIST_PERSISTENCE_MESSAGE_STORE_PROPERTIES);

        for (String messageStoreProperty : messageStoreProperties) {
            storeConfiguration.addMessageStoreProperty(messageStoreProperty, (String) AndesConfigurationManager
                    .readValueOfChildByKey(AndesConfiguration.PERSISTENCE_MESSAGE_STORE_PROPERTY, messageStoreProperty));
        }

        storeConfiguration.setAndesContextStoreClassName((String) AndesConfigurationManager.readValue
                (AndesConfiguration.PERSISTENCE_CONTEXT_STORE_HANDLER));

        List<String> contextStoreProperties = AndesConfigurationManager.readValueList
                (AndesConfiguration.LIST_PERSISTENCE_CONTEXT_STORE_PROPERTIES);

        for (String contextStoreProperty : contextStoreProperties) {
            storeConfiguration.addContextStoreProperty(contextStoreProperty, (String) AndesConfigurationManager
                    .readValueOfChildByKey(AndesConfiguration.PERSISTENCE_CONTEXT_STORE_PROPERTY,contextStoreProperty));
        }
    }

    /**
     * Get delivery time out of a message. If this is breached an ack for the message
     * will be simulated internally.
     * @return time out value
     */
    public int getDeliveryTimeoutForMessage() {
        return deliveryTimeoutForMessage;
    }

    /**
     * Set delivery time out of a message. If this is breached an ack for the message
     * will be simulated internally.
     * @param deliveryTimeoutForMessage time out value to set
     */
    public void setDeliveryTimeoutForMessage(int deliveryTimeoutForMessage) {
        this.deliveryTimeoutForMessage = deliveryTimeoutForMessage;
    }

    /**
     * Gets the message store.
     *
     * @return The message store.
     */
    public MessageStore getMessageStore() {
        return messageStore;
    }

    /**
     * Sets message store instance
     *
     * @param messageStore The message store
     */
    public void setMessageStore(MessageStore messageStore) {
        this.messageStore = messageStore;
    }

    /**
     * Set storageQueueRegistry to context
     * @param storageQueueRegistry queue registry to set
     */
    public void setStorageQueueRegistry(StorageQueueRegistry storageQueueRegistry) {
        this.storageQueueRegistry = storageQueueRegistry;
    }


    /**
     * Get MessageRouterRegistry instance
     * @return MessageRouterRegistry instance
     */
    public MessageRouterRegistry getMessageRouterRegistry() {
        return messageRouterRegistry;
    }

    /**
     * Set MessageRouterRegistry to context
     * @param messageRouterRegistry MessageRouterRegistry to set
     */
    public void setMessageRouterRegistry(MessageRouterRegistry messageRouterRegistry) {
        this.messageRouterRegistry = messageRouterRegistry;
    }

    /**
     * Get StorageQueueRegistry instance
     *
     * @return StorageQueueRegistry instance
     */
    public StorageQueueRegistry getStorageQueueRegistry() {
        return storageQueueRegistry;
    }

    public void setInboundEventManager(InboundEventManager inboundEventManager) {
        this.inboundEventManager = inboundEventManager;
    }

    /**
     * Get InboundEventManager instance. This is used for publishing inbound events
     * into inbound disruptor
     *
     * @return InboundEventManager instance
     */
    public InboundEventManager getInboundEventManager() {
        return inboundEventManager;
    }

    /**
     * Set AndesContextInformationManager to the context
     *
     * @param contextInformationManager AndesContextInformationManager instance to set
     */
    public void setAndesContextInformationManager(AndesContextInformationManager contextInformationManager) {
        this.andesContextInformationManager = contextInformationManager;
    }

    /**
     * Get AndesContextInformationManager instance set to context. This is used to manage
     * context information inside broker
     *
     * @return AndesContextInformationManager instance
     */
    public AndesContextInformationManager getAndesContextInformationManager() {
        return andesContextInformationManager;
    }


    /**
     * Get ClusterNotificationListenerManager set to the context.
     *
     * @return ClusterNotificationListenerManager of broker.
     */
    public ClusterNotificationListenerManager getClusterNotificationListenerManager() {
        return clusterNotificationListenerManager;
    }

    /**
     * Set ClusterNotificationListenerManager to the context.
     *
     * @param clusterNotificationListenerManager ClusterNotificationListenerManager instance to set
     */
    public void setClusterNotificationListenerManager(ClusterNotificationListenerManager
                                                              clusterNotificationListenerManager) {
        this.clusterNotificationListenerManager = clusterNotificationListenerManager;
    }
}
