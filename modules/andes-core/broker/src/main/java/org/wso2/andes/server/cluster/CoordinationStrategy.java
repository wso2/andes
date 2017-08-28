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

package org.wso2.andes.server.cluster;

import org.wso2.andes.kernel.AndesException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * The type used define the coordination algorithm used to identify the coordinator in the cluster.
 */
interface CoordinationStrategy {

    /**
     * Used to query about current node's coordinator status
     *
     * @return true if current node is the coordinator, False otherwise
     */
    boolean isCoordinator();


    /**
     * Return the socket address of the coordinator Node. This socket address can be used to communicate with the
     * coordinator node using thrift.
     *
     * @return socket address of the coordinator thrift server if present, null otherwise
     */
    InetSocketAddress getThriftAddressOfCoordinator();

    /**
     * Return all ids of the connected nodes.
     *
     * @return list of member ids
     */
    List<String> getAllNodeIdentifiers() throws AndesException;

    /**
     * Return all ids of the connected nodes.
     *
     * @return list of member ids
     */
    List<NodeDetail> getAllNodeDetails() throws AndesException;

    /**
     * Meant to be invoked when coordination algorithm should start working. This is typically invoked during the
     * server start up.
     *
     * @param configurableClusterAgent cluster agent used to indicate cluster change events
     * @param nodeId                   local node ID
     * @param thriftAddress            local node's thrift server socket address
     */
    void start(CoordinationConfigurableClusterAgent configurableClusterAgent, String nodeId,
            InetSocketAddress thriftAddress);

    /**
     * Meant to be invoked when coordination algorithm should stop working. This is typically during the server
     * shutdown.
     */
    void stop();
}
