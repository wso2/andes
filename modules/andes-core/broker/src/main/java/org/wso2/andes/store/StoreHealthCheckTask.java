/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.store;

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.server.ClusterResourceHolder;

/**
 * A periodic task which will check weather  given {@link HealthAwareStore} is operational.
 * 
 */
public class StoreHealthCheckTask implements Runnable {

    private static final Log logger = LogFactory.getLog(StoreHealthCheckTask.class);

    /**
     * the store which became in-operational. (therefore this task is checking
     * weather it becomes available again)
     */
    private HealthAwareStore store;

    /**
     * Instantiates a new task with a specified store and a list of listeners.
     * @param store in-operational store.
     * @param healthListeners listeners which this task needs to notify.
     */
    StoreHealthCheckTask(HealthAwareStore store, Collection<StoreHealthListener> healthListeners) {
        this.store = store;
    }

    /**
     * Checks periodically weather store is available.
     */
    @Override
    public void run() {
        try {
            String myNodeId = ClusterResourceHolder.getInstance().getClusterManager().getMyNodeID();
            logger.info(String.format("about to check store [%s]'s operational status ", store.getClass()));
            store.isOperational(myNodeId, System.currentTimeMillis());
        } catch (Throwable e) {
            logger.error("Error occurred while checking store availability.", e);
        }
    }

}
