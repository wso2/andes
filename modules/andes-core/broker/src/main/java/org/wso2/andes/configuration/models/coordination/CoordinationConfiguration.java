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
package org.wso2.andes.configuration.models.coordination;

import org.wso2.carbon.config.annotation.Configuration;
import org.wso2.carbon.config.annotation.Element;

/**
 * Configuration model for broker rdbmsBasedCoordination.
 */
@Configuration(description = "Broker Coordination config")
public class CoordinationConfiguration {

    @Element(description = "The node ID of each member")
    private String nodeID = "default";

    @Element(description = "Thrift is used to maintain and sync slot (message groups) ranges between MB nodes")
    private String thriftServerHost = "localhost";

    @Element(description = "Thrift is used to maintain and sync slot (message groups) ranges between MB nodes")
    private int thriftServerPort = 7611;

    @Element(description = "Thrift is used to maintain and sync slot (message groups) ranges between MB nodes")
    private int thriftSOTimeout = 0;

    @Element(description = "Thrift server reconnect timeout. Value specified in SECONDS.")
    private long thriftServerReconnectTimeout = 5;

    @Element(description = "Hazelcast reliable topics are used to share all notifications across the MB cluster"
            + " (e.g. subscription changes), And this property defines the time-to-live for a notification since "
            + "its creation. (in Seconds)")
    private int clusterNotificationTimeout = 10;

    @Element(description = "Configurations related to RDBMS based rdbmsBasedCoordination algorithm")
    private RDBMSBasedCoordination rdbmsBasedCoordination = new RDBMSBasedCoordination();

    @Element(description = "Configurations related to RDBMS based rdbmsBasedCoordination algorithm")
    private RDBMSBasedClusterEventConfiguration rdbmsBasedClusterEventSynchronization = new RDBMSBasedClusterEventConfiguration();

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

    public long getThriftServerReconnectTimeout() {
        return thriftServerReconnectTimeout;
    }

    public int getClusterNotificationTimeout() {
        return clusterNotificationTimeout;
    }

    public RDBMSBasedCoordination getRdbmsBasedCoordination() {
        return rdbmsBasedCoordination;
    }

    public RDBMSBasedClusterEventConfiguration getRdbmsBasedClusterEventSynchronization() {
        return rdbmsBasedClusterEventSynchronization;
    }

}