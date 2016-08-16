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

import org.apache.log4j.Logger;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.coordination.ClusterNotificationPublisher;
import org.wso2.andes.server.cluster.coordination.ClusterNotification;
import org.wso2.andes.server.virtualhost.VirtualHostConfigSynchronizer;

/**
 * ExchangeListener listens and handles exchange changes that have occurred locally and cluster-wide.
 */
public class ExchangeListener {

    private static final Logger log = Logger.getLogger(ExchangeListener.class);

    /**
     * Exchange event types that could be present in the cluster.
     */
    public enum ExchangeChange {
        ADDED,
        DELETED
    }

    /**
     * Local event handler is used to notify local exchange changes to the cluster.
     */
    private ClusterNotificationPublisher clusterNotificationPublisher;

    /**
     * VirtualHostConfigSynchronizer is used to synchronize cluster events received with Qpid.
     */
    VirtualHostConfigSynchronizer virtualHostConfigSynchronizer;

    /**
     * Creates a listener for binding changes given a publisher to notify the changes that are received to the cluster.
     *
     * @param publisher Hazelcast/RDBMS based or standalone publisher to publish cluster notifications.
     */
    public ExchangeListener(ClusterNotificationPublisher publisher) {
        clusterNotificationPublisher = publisher;
        virtualHostConfigSynchronizer = ClusterResourceHolder.getInstance().getVirtualHostConfigSynchronizer();
    }

    /**
     * Handle when an exchange has changed in cluster.
     *
     * @param exchange   exchange changed
     * @param changeType the change
     * @throws AndesException
     */
    public void handleClusterExchangesChanged(AndesExchange exchange, ExchangeChange changeType) throws AndesException {
        if (log.isDebugEnabled()) {
            log.debug("Cluster event received: " + exchange.encodeAsString());
        }
        switch (changeType) {
            case ADDED:
                //create a exchange
                virtualHostConfigSynchronizer.clusterExchangeAdded(exchange);
                break;
            case DELETED:
                //delete exchange
                virtualHostConfigSynchronizer.clusterExchangeRemoved(exchange);
                break;
        }
    }

    /**
     * Handle when an exchange is changed in the local node.
     *
     * @param exchange   exchange changed
     * @param changeType the change
     * @throws AndesException
     */
    public void handleLocalExchangesChanged(AndesExchange exchange, ExchangeChange changeType) throws AndesException {
        ClusterNotification clusterNotification
                = new ClusterNotification(exchange.encodeAsString(), changeType.toString());
        clusterNotificationPublisher.publishClusterNotification(clusterNotification);

    }
}
