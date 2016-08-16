/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 *
 */

package org.wso2.andes.server.cluster.coordination.rdbms;

import org.apache.log4j.Logger;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.server.cluster.coordination.ClusterNotification;
import org.wso2.andes.server.cluster.coordination.ClusterNotificationPublisher;

import java.util.List;

/**
 * The RDBMS based implementation of ClusterNotificationPublisher which stores received local event changes.
 */
public class RDBMSBasedClusterNotificationPublisher implements ClusterNotificationPublisher {

    /**
     * Logger object to log information.
     */
    private static final Logger log = Logger.getLogger(RDBMSBasedClusterNotificationPublisher.class);

    /**
     * The context store instance which is used to store events.
     */
    private AndesContextStore contextStore;

    /**
     * prefix to which the cluster notification type will be appended. Could take the values of "QUEUE", "BINDING",
     * "EXCHANGE" and "SUBSCRIPTION".
     */
    private String prefix;

    /**
     * The node id of this node.
     */
    private String nodeID;

    /**
     * Initialize the handler with a specific prefix.
     *
     * @param prefix the prefix to be used when storing events
     */
    public RDBMSBasedClusterNotificationPublisher(String prefix) {
        this.prefix = prefix;
        contextStore = AndesContext.getInstance().getAndesContextStore();
        nodeID = AndesContext.getInstance().getClusterAgent().getLocalNodeIdentifier();
    }

    /**
     * Stores the received notification in the database.
     * {@inheritDoc}
     */
    @Override
    public void publishClusterNotification(ClusterNotification clusterNotification) throws AndesException {
        String eventType;
        switch (clusterNotification.getChangeType()) {
            case "ADDED":
                eventType = prefix + "_ADDED";
                break;
            case "DELETED":
                eventType = prefix + "_DELETED";
                break;
            case "PURGED":
                eventType = prefix + "_PURGED";
                break;
            case "MERGED":
                eventType = prefix + "_MERGED";
                break;
            case "DISCONNECTED":
                eventType = prefix + "_DISCONNECTED";
                break;
            default:
                log.error("Unknown event type to be stored: " + clusterNotification.getChangeType());
                throw new AndesException("Unknown event type to be stored: " + clusterNotification.getChangeType());
        }

        //duplicate the cluster notification for all nodes in the cluster and store them destined to the respective
        // to each node.
        List<String> clusterNodes = AndesContext.getInstance().getClusterAgent().getAllNodeIdentifiers();
        contextStore.storeClusterNotification(clusterNodes, nodeID, eventType,
                                              clusterNotification.getEncodedObjectAsString());
        if (log.isDebugEnabled()) {
            log.debug("Cluster notification " + clusterNotification.getEncodedObjectAsString() + " stored in Database");
        }
    }
}
