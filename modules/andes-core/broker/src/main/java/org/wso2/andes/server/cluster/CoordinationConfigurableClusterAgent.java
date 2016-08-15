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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.core.Member;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.slot.SlotCoordinationConstants;
import org.wso2.andes.server.cluster.coordination.CoordinationConstants;
import org.wso2.andes.server.cluster.error.detection.DisabledNetworkPartitionDetector;
import org.wso2.andes.server.cluster.error.detection.HazelcastBasedNetworkPartitionDetector;
import org.wso2.andes.server.cluster.error.detection.NetworkPartitionDetector;
import org.wso2.andes.server.cluster.error.detection.NetworkPartitionListener;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
     * Hazelcast instance used to communicate with the hazelcast cluster
     */
    private final HazelcastInstance hazelcastInstance;

    /**
     * Unique id of local member used for message ID generation
     */
    private int uniqueIdOfLocalMember;

    /**
     * Cluster manager used to indicate membership change events
     */
    private ClusterManager manager;

    /**
     * Hold coordinator information
     */
    private IMap<String, String> coordinatorNodeDetailsMap;

    /**
     * Node identifier (set in broker.xml) of each node is stored in this map against the <ip>:<port> of that node.
     *
     *  Key - <ip>:<port>
     *  Value - NodeId
     */
    private IMap<String, String> nodeIdMap;

    /**
     * Implementation of scheme used to detect network partitions
     */
    private NetworkPartitionDetector networkPartitionDetector;
    
    /*
    * Maximum number of attempts to read node id of a cluster member
    */
    public static final int MAX_NODE_ID_READ_ATTEMPTS = 4;
    
    public CoordinationConfigurableClusterAgent(HazelcastInstance hazelcastInstance) {

        this.hazelcastInstance = hazelcastInstance;
        nodeIdMap = hazelcastInstance.getMap(CoordinationConstants.NODE_ID_MAP_NAME);

        boolean isNetworkPartitionDectectionEnabled = AndesConfigurationManager.readValue(
                                                         AndesConfiguration.RECOVERY_NETWORK_PARTITIONS_DETECTION);

        if (isNetworkPartitionDectectionEnabled) {
            networkPartitionDetector = new HazelcastBasedNetworkPartitionDetector(hazelcastInstance);
        } else {
            networkPartitionDetector = new DisabledNetworkPartitionDetector();
        }

        boolean isRDBMBasedCoordinationEnabled = AndesConfigurationManager.readValue(
                AndesConfiguration.RDBMS_BASED_COORDINATION);

        if (isRDBMBasedCoordinationEnabled) {
            coordinationStrategy = new RDBMSCoordinationStrategy();
        } else {
            coordinationStrategy = new HazelcastCoordinationStrategy(hazelcastInstance);
        }
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
        updateCoordinatorNodeDetailMap();
        manager.localNodeElectedAsCoordinator();
    }
    
    /**
     * Get id of the give node
     *
     * @param node
     *         Hazelcast member node
     * @return id of the node
     */
    public String getIdOfNode(Member node) {
        String nodeId = nodeIdMap.get(node.getSocketAddress().toString());
        if(StringUtils.isEmpty(nodeId)) {
            nodeId = node.getSocketAddress().toString();
        }
        return nodeId;
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

        Member localMember = hazelcastInstance.getCluster().getLocalMember();
        nodeIdMap.set(localMember.getSocketAddress().toString(), getLocalNodeIdentifier());

        checkForDuplicateNodeId(localMember);

        coordinatorNodeDetailsMap = hazelcastInstance.getMap(CoordinationConstants.COORDINATOR_NODE_DETAILS_MAP_NAME);

        // Generate a unique id for this node for message id generation
        IdGenerator idGenerator = this.hazelcastInstance.getIdGenerator(
                CoordinationConstants.HAZELCAST_ID_GENERATOR_NAME);
        this.uniqueIdOfLocalMember = (int) idGenerator.newId();
        if (log.isDebugEnabled()) {
            log.debug("Unique ID generation for message ID generation:" + uniqueIdOfLocalMember);
        }

        String thriftCoordinatorServerIP = AndesContext.getInstance().getThriftServerHost();
        int thriftCoordinatorServerPort = AndesContext.getInstance().getThriftServerPort();
        InetSocketAddress thriftAddress = new InetSocketAddress(thriftCoordinatorServerIP, thriftCoordinatorServerPort);

        coordinationStrategy.start(this, getLocalNodeIdentifier(), thriftAddress);

        networkPartitionDetector.start();
    }

    /**
     * Check if the local node id is already taken by a different node in the cluster
     *
     * @param localMember
     *         Current member
     * @throws AndesException
     */
    private void checkForDuplicateNodeId(Member localMember) throws AndesException {
        Set<Member> members = hazelcastInstance.getCluster().getMembers();

        for (Member member : members) {
            int nodeIdReadAttempts = 1;
            String nodeIdOfMember = getIdOfNode(member);

            /*
             Node ID can be null if the node has not initialized yet. Therefore try to read the node id
             MAX_NODE_ID_READ_ATTEMPTS times before failing.
              */
            while ((nodeIdOfMember == null) && (nodeIdReadAttempts <= MAX_NODE_ID_READ_ATTEMPTS)) {

                // Exponentially increase waiting time,
                long sleepTime = Math.round(Math.pow(2, nodeIdReadAttempts));
                log.warn("Node id was null for member " + member + ". Node id will be read again after "
                         + sleepTime + " seconds.");

                try {
                    TimeUnit.SECONDS.sleep(sleepTime);
                } catch (InterruptedException ignore) {
                }

                nodeIdReadAttempts++;
                nodeIdOfMember = getIdOfNode(member);
            }

            if (nodeIdOfMember == null) {
                throw new AndesException("Failed to read Node id of hazelcast member " + member);
            }

            if ((localMember != member) && (nodeIdOfMember.equals(getLocalNodeIdentifier()))) {
                throw new AndesException("Another node with the same node id: " + getLocalNodeIdentifier()
                                         + " found in the cluster. Cannot start the node.");
            }
        }
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

        // If the config value is "default" we must generate the ID
        if (AndesConfiguration.COORDINATION_NODE_ID.get().getDefaultValue().equals(nodeId)) {
            Member localMember = hazelcastInstance.getCluster().getLocalMember();
            nodeId = CoordinationConstants.NODE_NAME_PREFIX + localMember.getSocketAddress();
        }

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
     * {@inheritDoc}
     */
    @Override
    public CoordinatorInformation getCoordinatorDetails() {
        String ipAddress = coordinatorNodeDetailsMap.get(
                SlotCoordinationConstants.CLUSTER_COORDINATOR_SERVER_IP);
        String port = coordinatorNodeDetailsMap.get(SlotCoordinationConstants.CLUSTER_COORDINATOR_SERVER_PORT);

        return new CoordinatorInformation(ipAddress, port);
    }

    /**
     * Sets coordinator's hostname and port in {@link org.wso2.andes.server.cluster.coordination
     * .CoordinationConstants#COORDINATOR_NODE_DETAILS_MAP_NAME} hazelcast map.
     */
    private void updateCoordinatorNodeDetailMap() {
        // Adding cluster coordinator's node IP and port
        Member localMember = hazelcastInstance.getCluster().getLocalMember();
        coordinatorNodeDetailsMap.put(SlotCoordinationConstants.CLUSTER_COORDINATOR_SERVER_IP,
                                      localMember.getSocketAddress().getAddress().getHostAddress());
        coordinatorNodeDetailsMap.put(SlotCoordinationConstants.CLUSTER_COORDINATOR_SERVER_PORT,
                                      Integer.toString(localMember.getSocketAddress().getPort()));
    }

    /**
     * Gets address of all the members in the cluster. i.e address:port
     *
     * @return A list of address of the nodes in a cluster
     */
    public List<String> getAllClusterNodeAddresses() {
        List<String> addresses = new ArrayList<>();
        if (AndesContext.getInstance().isClusteringEnabled()) {
            for (Member member : hazelcastInstance.getCluster().getMembers()) {
                InetSocketAddress socket = member.getSocketAddress();
                addresses.add(getIdOfNode(member) + ","
                              + socket.getAddress().getHostAddress() + "," + socket.getPort());
            }
        }
        return addresses;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addNetworkPartitionListener(int priority, NetworkPartitionListener listener) {
        networkPartitionDetector.addNetworkPartitionListener(priority, listener);
    }
}
