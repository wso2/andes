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
package org.wso2.andes.server;

import org.wso2.andes.kernel.AndesRecoveryTask;
import org.wso2.andes.server.cassandra.AndesSubscriptionManager;
import org.wso2.andes.server.cassandra.MessageExpirationWorker;
import org.wso2.andes.server.cluster.ClusterManager;
import org.wso2.andes.server.configuration.BrokerConfiguration;
import org.wso2.andes.server.store.QpidDeprecatedMessageStore;
import org.wso2.andes.server.virtualhost.VirtualHostConfigSynchronizer;

/**
 * Class <code>ClusterResourceHolder</code> holds the Cluster implementation specific
 * Objects.This act as a Singleton object holder for that objects with in the broker.
 */
public class ClusterResourceHolder {

    private static ClusterResourceHolder resourceHolder;

    /**
     * Holds the Cassandra MessageStore instance to use throughout the broker
     */
    private QpidDeprecatedMessageStore qpidDeprecatedMessageStore;


    /**
     * Holds Cassandra SubscriptionManager
     */
    private AndesSubscriptionManager subscriptionManager;


    /**
     * Holds the Cluster Configuration Data
     */
    private BrokerConfiguration clusterConfiguration;


    /**
     * Holds VirtualHost Config Synchronizer
     */
    private VirtualHostConfigSynchronizer virtualHostConfigSynchronizer;


    /**
     * Holds a reference to andes recovery task
     */
    private AndesRecoveryTask andesRecoveryTask;

    /**
     * holds cluster manager
     */
    private ClusterManager clusterManager;
    private MessageExpirationWorker messageExpirationWorker;

    private ClusterResourceHolder() {

    }


    public static ClusterResourceHolder getInstance() {
        if (resourceHolder == null) {

            synchronized (ClusterResourceHolder.class) {
                if (resourceHolder == null) {
                    resourceHolder = new ClusterResourceHolder();
                }
            }
        }

        return resourceHolder;
    }


    public QpidDeprecatedMessageStore getQpidDeprecatedMessageStore() {
        return qpidDeprecatedMessageStore;
    }

    public AndesSubscriptionManager getSubscriptionManager() {
        return subscriptionManager;
    }

    public void setQpidDeprecatedMessageStore(QpidDeprecatedMessageStore qpidDeprecatedMessageStore) {
        this.qpidDeprecatedMessageStore = qpidDeprecatedMessageStore;
    }

    public void setSubscriptionManager(AndesSubscriptionManager subscriptionManager) {
        this.subscriptionManager = subscriptionManager;
    }

    public void setClusterManager(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public ClusterManager getClusterManager() {
        return this.clusterManager;
    }

    public BrokerConfiguration getClusterConfiguration() {
        return clusterConfiguration;
    }

    public void setClusterConfiguration(BrokerConfiguration clusterConfiguration) {
        this.clusterConfiguration = clusterConfiguration;
    }

    public VirtualHostConfigSynchronizer getVirtualHostConfigSynchronizer() {
        return virtualHostConfigSynchronizer;
    }

    public void setVirtualHostConfigSynchronizer(VirtualHostConfigSynchronizer virtualHostConfigSynchronizer) {
        this.virtualHostConfigSynchronizer = virtualHostConfigSynchronizer;
    }

    public AndesRecoveryTask getAndesRecoveryTask() {
        return andesRecoveryTask;
    }

    public void setAndesRecoveryTask(AndesRecoveryTask andesRecoveryTask) {
        this.andesRecoveryTask = andesRecoveryTask;
    }

    public MessageExpirationWorker getMessageExpirationWorker() {
        return messageExpirationWorker;
    }

    public void setMessageExpirationWorker(MessageExpirationWorker messageExpirationWorker) {
        this.messageExpirationWorker = messageExpirationWorker;
    }
}
