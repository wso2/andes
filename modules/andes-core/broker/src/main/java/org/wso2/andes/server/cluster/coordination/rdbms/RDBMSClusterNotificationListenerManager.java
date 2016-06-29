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

package org.wso2.andes.server.cluster.coordination.rdbms;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.log4j.Logger;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.server.cluster.coordination.ClusterNotificationListenerManager;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * The RDBMSClusterNotificationListenerManager provides means to initialize the node with RDBMS based cluster notification
 * synchronization..
 */
public class RDBMSClusterNotificationListenerManager implements ClusterNotificationListenerManager {

    private static final Logger log = Logger.getLogger(RDBMSClusterNotificationListenerManager.class);

    ScheduledExecutorService scheduledExecutorService;

    /**
     * {@inheritDoc}
     * <p/>
     * Schedules the {@link ClusterEventReaderTask} to run with the configured interval in the broker.xml.
     */
    @Override
    public void initializeListener() throws AndesException {

        // Clear cluster notifications stored in the database and schedule a periodic task to read cluster events
        // from the store if cluster event sync mode is set to RDBMS.
        clearClusterNotificationsBoundToNode();
        scheduleClusterNotificationReader();
    }

    /**
     * Schedules the {@link ClusterEventReaderTask} to run with the configured interval in the broker.xml.
     */
    private void scheduleClusterNotificationReader() {
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
    public void clearAllClusterNotifications() throws AndesException {
        AndesContext.getInstance().getAndesContextStore().clearClusterNotifications();
    }

    /**
     * Clears cluster notifications stored in the database that are bound to this node.
     *
     * @throws AndesException
     */
    private void clearClusterNotificationsBoundToNode() throws AndesException {
        AndesContext.getInstance().getAndesContextStore().clearClusterNotifications(AndesContext.getInstance()
                .getClusterAgent().getLocalNodeIdentifier());
    }

    /**
     * Stops the listener task.
     *
     * @throws AndesException
     */
    @Override
    public void stopListener() throws AndesException {
        scheduledExecutorService.shutdown();
        log.info("RDBMS cluster event listener stopped.");
    }
}
