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

package org.wso2.andes.server.cluster.coordination;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.coordination.rdbms.RDBMSBasedNotificationAgentImpl;
import org.wso2.andes.server.cluster.coordination.rdbms.RDBMSClusterNotificationListenerImpl;

/**
 * This i the factory class for creating event publishers and event listeners
 * for cluster communication across nodes
 */
public class CoordinationComponentFactory {

    private Log log = LogFactory.getLog(CoordinationComponentFactory.class);

    private boolean isRDBMSBasedCoordinationEnabled;

    private boolean isClusteringEnabled;

    /**
     * Create a {@link ClusterNotificationListenerManager} implementation based on the
     * configurations
     *
     * @return ClusterNotificationListenerManager instance
     */
    public ClusterNotificationListenerManager createClusterNotificationListener() throws AndesException {
        initConfiguration();
        ClusterNotificationListenerManager clusterNotificationListenerManager = null;
        if (ClusterResourceHolder.getInstance().getClusterManager().isClusteringEnabled()) {
            if (isRDBMSBasedCoordinationEnabled) {
                log.info("Broker is initialized with RDBMS based cluster event synchronization.");
                AndesContextStore contextStore = AndesContext.getInstance().getAndesContextStore();
                clusterNotificationListenerManager = new RDBMSClusterNotificationListenerImpl(contextStore);
            }
        }
        return clusterNotificationListenerManager;

    }

    /**
     * Create a {@link org.wso2.andes.server.cluster.coordination.ClusterNotificationAgent}
     * implementation based on the configurations.
     *
     * @return ClusterNotificationAgent instance
     * @throws AndesException in case of Hazelcast based ClusterNotificationAgent you need to call
     *                        {@link CoordinationComponentFactory#createClusterNotificationListener()} first.
     *                        Otherwise exception is thrown
     */
    public ClusterNotificationAgent createClusterNotificationAgent() throws AndesException {
        initConfiguration();
        AndesContextStore contextStore = AndesContext.getInstance().getAndesContextStore();
        ClusterNotificationAgent clusterNotificationAgent = new StandaloneMockNotificationAgent();
        if (isClusteringEnabled) {
            if (isRDBMSBasedCoordinationEnabled) {
                clusterNotificationAgent = new RDBMSBasedNotificationAgentImpl(contextStore);
            }
        }
        return clusterNotificationAgent;
    }

    /**
     * Init the configs needed
     */
    private void initConfiguration() {
        isRDBMSBasedCoordinationEnabled =
                AndesConfigurationManager.readValue(AndesConfiguration.CLUSTER_EVENT_SYNC_MODE_RDBMS_ENABLED);
        isClusteringEnabled = ClusterResourceHolder.getInstance().getClusterManager().isClusteringEnabled();
    }

}

