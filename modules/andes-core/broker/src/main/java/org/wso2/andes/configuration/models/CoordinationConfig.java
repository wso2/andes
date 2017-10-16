/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except 
 * in compliance with the License.
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
package org.wso2.andes.configuration.models;

import org.wso2.carbon.config.annotation.Configuration;
import org.wso2.carbon.config.annotation.Element;

/**
 * Configuration model for broker coordination.
 */
@Configuration(description = "Broker coordination config.")
public class CoordinationConfig {

    @Element(description = "The node ID of each member. If it is left as \"default\", the default node ID will be\n"
            + "generated for it. (Using IP + UUID). The node ID of each member should ALWAYS be unique.")
    private String nodeID = "default";

    @Element(description = "Hostname of the thrift server used to maintain and sync slot (message groups) ranges "
            + "nodes.")
    private String thriftServerHost = "localhost";

    @Element(description = "Port of the thrift server used to maintain and sync slot (message groups) ranges "
            + "nodes.")
    private int thriftServerPort = 7611;

    @Element(description = "Thrify server SO timeout")
    private int thriftSOTimeout = 0;

    @Element(description = "Thrift server reconnect timeout. Value specified in seconds")
    private int thriftServerReconnectTimeout = 5;

    @Element(description = "Hazelcast reliable topics are used to share all notifications across the MB cluster (e.g. subscription\n"
            + "changes), And this property defines the time-to-live for a notification since its creation. (in Seconds)")
    private int clusterNotificationTimeout = 10;

    @Element(description = "RDBMS based coordination algorithm related configs.")
    private RDBMSBasedCoordinationConfig rdbmsBasedCoordination = new RDBMSBasedCoordinationConfig();

    @Element(description = "RDBMS based cluster event synchronization configs.")
    private RDBMSClusterEventSynchronizationConfig rdbmsBasedClusterEventSynchronization = new RDBMSClusterEventSynchronizationConfig();

    public String getNodeID() {
        return nodeID;
    }

    public String getThriftServerHost() {
        return thriftServerHost;
    }

    public int getThriftServerPort() {
        return thriftServerPort;
    }

    public int getThriftSOTimeout() {
        return thriftSOTimeout;
    }

    public int getThriftServerReconnectTimeout() {
        return thriftServerReconnectTimeout;
    }

    public int getClusterNotificationTimeout() {
        return clusterNotificationTimeout;
    }

    public RDBMSBasedCoordinationConfig getRdbmsBasedCoordinationConfig() {
        return rdbmsBasedCoordination;
    }

    public RDBMSClusterEventSynchronizationConfig getRdbmsBasedClusterEventSynchronizationConfig() {
        return rdbmsBasedClusterEventSynchronization;
    }

}
