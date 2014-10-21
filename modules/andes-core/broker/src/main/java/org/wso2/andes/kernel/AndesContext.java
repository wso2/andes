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
import org.wso2.andes.configuration.VirtualHostsConfiguration;
import org.wso2.andes.subscription.SubscriptionStore;

import java.util.Map;

/**
 * AndesContext is used to pass instances created and configurations read through component level
 * to Andes. A place holder class.
 */
public class AndesContext {
    private SubscriptionStore subscriptionStore;
    private AndesContextStore andesContextStore;
    private VirtualHostsConfiguration virtualHostsConfiguration;
	private Map<String, AndesSubscription> dataSenderMap;
    private ClusteringAgent clusteringAgent;
    private boolean isClusteringEnabled;
    private AMQPConstructStore AMQPConstructStore;
    private static AndesContext instance = new AndesContext();
    private String thriftServerHost;
    private int thriftServerPort;
    private String thriftCoordinatorServerIP;
    private int thriftCoordinatorServerPort;

    /**
     * Set virtual host configuration
     * @param configuration VirtualHostsConfiguration
     */
    public void setVirtualHostConfiguration(VirtualHostsConfiguration configuration) {
        this.virtualHostsConfiguration = configuration;
    }

    /**
     * Get virtual host configuration object
     * @return VirtualHostsConfiguration
     */
    public VirtualHostsConfiguration getVirtualHostsConfiguration() {
        return virtualHostsConfiguration;
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
    public String getThriftServerHost() {
        return thriftServerHost;
    }

    /**
     * set thrift server host ip
     * @param thriftServerHost
     */
    public void setThriftServerHost(String thriftServerHost) {
        this.thriftServerHost = thriftServerHost;
    }

    /**
     * get thrift server port
     * @return
     */
    public int getThriftServerPort() {
        return thriftServerPort;
    }

    /**
     * return thrift server port
     * @param thriftServerPort
     */
    public void setThriftServerPort(int thriftServerPort) {
        this.thriftServerPort = thriftServerPort;
    }

    /**
     * get IP address of thrift coordinator
     * @return  IP of the Slot Manager
     */
    public String getThriftCoordinatorServerIP() {
        return thriftCoordinatorServerIP;
    }

    /**
     * set IP of the thrift coordinator
     * @param thriftCoordinatorServerIP
     */
    public void setThriftCoordinatorServerIP(String thriftCoordinatorServerIP) {
        this.thriftCoordinatorServerIP = thriftCoordinatorServerIP;
    }

    /**
     * get thrift coordinator port
     * @return slot manager port
     */
    public int getThriftCoordinatorServerPort() {
        return thriftCoordinatorServerPort;
    }

    /**
     * set thrift coordinator port
     * @param thriftCoordinatorServerPort
     */
    public void setThriftCoordinatorServerPort(int thriftCoordinatorServerPort) {
        this.thriftCoordinatorServerPort = thriftCoordinatorServerPort;
    }
}
