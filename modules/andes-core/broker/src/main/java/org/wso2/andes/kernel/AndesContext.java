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
import org.wso2.andes.configuration.qpid.ServerConfiguration;
import org.wso2.andes.subscription.SubscriptionStore;
import org.wso2.carbon.base.api.ServerConfigurationService;

import java.util.List;
import java.util.Map;

/**
 * AndesContext is used to pass instances created and configurations read through component level
 * to Andes. A place holder class.
 */
public class AndesContext {
    private SubscriptionStore subscriptionStore;
    private AndesContextStore andesContextStore;
    private StoreConfiguration storeConfiguration;
	private Map<String, AndesSubscription> dataSenderMap;
    private ClusteringAgent clusteringAgent;
    private boolean isClusteringEnabled;
    private AMQPConstructStore AMQPConstructStore;
    private static AndesContext instance = new AndesContext();

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

    public void addDataSender(String key, AndesSubscription dataSender) {
        dataSenderMap.put(key, dataSender);
    }

    public AndesSubscription getDataSender(String key) {
        return dataSenderMap.get(key);
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

    public void setClusteringAgent(ClusteringAgent clusteringAgent){
      this.clusteringAgent = clusteringAgent;
    }

    public ClusteringAgent getClusteringAgent() {
        return clusteringAgent;
    }

    /**
     *  get thrift server host ip
     * @return  thrift server host ip
     */
    public String getThriftServerHost() throws AndesException {
        return AndesConfigurationManager.getInstance().readConfigurationValue(AndesConfiguration
                .COORDINATION_THRIFT_SERVER_HOST);
    }

    /**
     * get thrift server port
     * @return
     */
    public Integer getThriftServerPort() throws AndesException {
        return AndesConfigurationManager.getInstance().readConfigurationValue(AndesConfiguration
                .COORDINATION_THRIFT_SERVER_PORT);
    }

    /***
     * Read configuration properties related to persistent stores and construct semantic object
     * for simple reference.
     */
    public void constructStoreConfiguration() throws AndesException {

        try {
            storeConfiguration = new StoreConfiguration();

            storeConfiguration.setMessageStoreClassName((String)AndesConfigurationManager.getInstance()
                    .readConfigurationValue(AndesConfiguration.PERSISTENCE_MESSAGE_STORE_HANDLER));

            List<String> messageStoreProperties = AndesConfigurationManager.getInstance().readPropertyList(AndesConfiguration.LIST_PERSISTENCE_MESSAGE_STORE_PROPERTIES);

            for (String messageStoreProperty : messageStoreProperties) {
                storeConfiguration.addMessageStoreProperty(messageStoreProperty,(String)AndesConfigurationManager.getInstance().readValueOfChildByKey(AndesConfiguration.PERSISTENCE_MESSAGE_STORE_PROPERTY,messageStoreProperty));
            }

            storeConfiguration.setAndesContextStoreClassName((String)AndesConfigurationManager
                    .getInstance().readConfigurationValue(AndesConfiguration
                            .PERSISTENCE_CONTEXT_STORE_HANDLER));

            List<String> contextStoreProperties = AndesConfigurationManager.getInstance().readPropertyList(AndesConfiguration.LIST_PERSISTENCE_CONTEXT_STORE_PROPERTIES);

            for (String contextStoreProperty : contextStoreProperties) {
                storeConfiguration.addContextStoreProperty(contextStoreProperty, (String) AndesConfigurationManager
                        .getInstance().readValueOfChildByKey(AndesConfiguration.PERSISTENCE_CONTEXT_STORE_PROPERTY,
                                contextStoreProperty));
            }

        }catch (AndesException e) {
            // The possible configuration related error is propagated as an AndesException since this method is triggered from the
            // business-messaging component. (common.lang.ConfigurationException is not visible
            // there.)
            throw e; //Since this exception is already having meaningful info.
        }
    }
}
