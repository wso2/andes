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

import org.apache.axis2.clustering.ClusteringAgent;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.StoreConfiguration;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.server.cluster.ClusterAgent;
import org.wso2.andes.subscription.SubscriptionStore;

import java.util.List;

/**
 * AndesContext is used to pass instances created and configurations read through component level
 * to Andes. A place holder class.
 */
public class AndesContext {
    private SubscriptionStore subscriptionStore;
    private AndesContextStore andesContextStore;
    private StoreConfiguration storeConfiguration;
    private ClusteringAgent clusteringAgent;
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
     * get subscription store
     *
     * @return subscription store
     */
    public SubscriptionStore getSubscriptionStore() {
        return subscriptionStore;
    }

    /**
     * set subscription store
     *
     * @param subscriptionStore subscription store
     */
    public void setSubscriptionStore(SubscriptionStore subscriptionStore) {
        this.subscriptionStore = subscriptionStore;
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
     * set if clustering is enabled
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
}
