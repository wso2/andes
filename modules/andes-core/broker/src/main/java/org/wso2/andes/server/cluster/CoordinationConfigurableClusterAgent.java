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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.server.cluster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.server.cluster.error.detection.DisabledNetworkPartitionDetector;
import org.wso2.andes.server.cluster.error.detection.NetworkPartitionDetector;
import org.wso2.andes.server.cluster.error.detection.NetworkPartitionListener;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Hazelcast based cluster agent implementation
 */
public class CoordinationConfigurableClusterAgent implements ClusterAgent {

    /**
     * Class logger
     */
    private static final Log log = LogFactory.getLog(CoordinationConfigurableClusterAgent.class);

    /**
     * Coordination algorithm used to elect the coordinator
     */
    private final CoordinationStrategy coordinationStrategy;

    /**
     * Unique id of local member used for message ID generation
     */
    private int uniqueIdOfLocalMember;

    /**
     * Cluster manager used to indicate membership change events
     */
    private ClusterManager manager;

    /**
     * Implementation of scheme used to detect network partitions
     */
    private NetworkPartitionDetector networkPartitionDetector;
    
    /*
    * Maximum number of attempts to read node id of a cluster member
    */
    public static final int MAX_NODE_ID_READ_ATTEMPTS = 4;
    
    public CoordinationConfigurableClusterAgent() {
        networkPartitionDetector = new DisabledNetworkPartitionDetector();
        coordinationStrategy = new RDBMSCoordinationStrategy();
    }

    /**
     * Membership listener calls this method when a new node joins the cluster
     *
     * @param nodeId Node ID of the new member
     */
    public void memberAdded(String nodeId) {
        manager.memberAdded(nodeId);
    }

    /**
     * Membership listener calls this method when a node leaves the cluster
     *
     * @param nodeId Node ID of the member who left
     * @throws AndesException
     */
    public void memberRemoved(String nodeId) throws AndesException {
        manager.memberRemoved(nodeId);
    }

    public void becameCoordinator() {
        manager.localNodeElectedAsCoordinator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getUniqueIdForLocalNode() {
        return uniqueIdOfLocalMember;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCoordinator() {
        return coordinationStrategy.isCoordinator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InetSocketAddress getThriftAddressOfCoordinator() {
        return coordinationStrategy.getThriftAddressOfCoordinator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start(ClusterManager manager) throws AndesException{
        this.manager = manager;
        networkPartitionDetector.start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        coordinationStrategy.stop();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getLocalNodeIdentifier() {
        String nodeId;

        // Get Node ID configured by user in broker.xml (if not "default" we must use it as the ID)
        nodeId = AndesConfigurationManager.readValue(AndesConfiguration.COORDINATION_NODE_ID);
        return nodeId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getAllNodeIdentifiers() throws AndesException {
        return coordinationStrategy.getAllNodeIdentifiers();
    }



    /**
     * Gets address of all the members in the cluster. i.e address:port
     *
     * @return A list of address of the nodes in a cluster
     */
    public List<String> getAllClusterNodeAddresses() throws AndesException {
        List<String> nodeDetailStringList = new ArrayList<>();

        List<NodeDetail> nodeDetails = coordinationStrategy.getAllNodeDetails();

        if (AndesContext.getInstance().isClusteringEnabled()) {
            for (NodeDetail nodeDetail : nodeDetails) {
                nodeDetailStringList.add(nodeDetail.getNodeId() + "," + nodeDetail.getClusterAgentAddress().getHostString() + ","
                        + nodeDetail.getClusterAgentAddress().getPort() + "," + nodeDetail.isCoordinator());
            }
        }

        return nodeDetailStringList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addNetworkPartitionListener(int priority, NetworkPartitionListener listener) {
        networkPartitionDetector.addNetworkPartitionListener(priority, listener);
    }
}
