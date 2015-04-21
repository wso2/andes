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
package org.wso2.andes.server.cluster;


import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.coordination.CoordinationConstants;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;
import org.wso2.andes.kernel.slot.SlotCoordinationConstants;
import org.wso2.andes.kernel.slot.SlotManagerClusterMode;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Cluster Manager is responsible for Handling the Broker Cluster Management Tasks like
 * Queue Worker distribution. Fail over handling for cluster nodes. etc.
 */
public class ClusterManager {

    private Log log = LogFactory.getLog(ClusterManager.class);

    /**
     * HazelcastAgent instance
     */
    private HazelcastAgent hazelcastAgent;

    /**
     * Id of the local node
     */
    private String nodeId;

    /**
     * each node is assigned  an ID 0-x after arranging nodeIDs in an ascending order
     */
    private int nodeSyncSyncId;

    /**
     * AndesContextStore instance
     */
    private AndesContextStore andesContextStore;

    /**
     * Create a ClusterManager instance
     */
    public ClusterManager() {
        this.andesContextStore = AndesContext.getInstance().getAndesContextStore();
    }

    /**
     * Initialize the Cluster manager.
     *
     * @throws AndesException
     */
    public void init() throws AndesException{

        if (!AndesContext.getInstance().isClusteringEnabled()) {
            this.initStandaloneMode();
            return;
        }

        initClusterMode();
    }

    /**
     * Handles changes needs to be done in current node when a node joins to the cluster
     */
    public void memberAdded() {
        reAssignNodeSyncId();
        //update thrift coordinator server details
        updateThriftCoordinatorDetailsToMap();
        updateCoordinatorNodeDetailMap();
    }

    /**
     * Handles changes needs to be done in current node when a node leaves the cluster
     */
    public void memberRemoved(Member node) throws AndesException {
        String deletedNodeId = hazelcastAgent.getIdOfNode(node);

        //refresh global queue sync ID
        reAssignNodeSyncId();

        // Below steps are carried out only by the 0th node of the list.
        if (nodeSyncSyncId == 0) {
            //clear persisted states of disappeared node
            clearAllPersistedStatesOfDisappearedNode(deletedNodeId);

            //Reassign the slot to free slots pool
            if (AndesContext.getInstance().getClusteringAgent().isCoordinator()) {
                SlotManagerClusterMode.getInstance().reAssignSlotsWhenMemberLeaves(deletedNodeId);
            }
        }

        // Deactivate durable subscriptions belonging to the node
        ClusterResourceHolder.getInstance().getSubscriptionManager().
                deactivateClusterDurableSubscriptionsForNodeID(AndesContext.getInstance().getClusteringAgent()
                                .isCoordinator(),
                        hazelcastAgent.getIdOfNode(node));

        //update thrift coordinator server details
        //  setThriftCoordinatorServerDetails();
    }

    /**
     * get binding address of the node
     *
     * @param nodeId id of node assigned by Hazelcast
     * @return bind address
     */
    public String getNodeAddress(String nodeId) throws AndesException {
        return andesContextStore.getAllStoredNodeData().get(nodeId);
    }

    /**
     * Get whether clustering is enabled
     *
     * @return true if clustering is enabled, false otherwise.
     */
    public boolean isClusteringEnabled() {
        return AndesContext.getInstance().isClusteringEnabled();
    }

    /**
     * Get the node ID of the current node
     *
     * @return current node's ID
     */
    public String getMyNodeID() {
        return nodeId;
    }

    /**
     * gracefully stop all global queue workers assigned for the current node
     */
    public void shutDownMyNode() throws AndesException {
        //clear stored node IDS and mark subscriptions of node as closed
        clearAllPersistedStatesOfDisappearedNode(nodeId);
    }

    /**
     * Gets the unique ID for the local node
     *
     * @return unique ID
     */
    public int getUniqueIdForLocalNode() {
        if (AndesContext.getInstance().isClusteringEnabled()) {
            return hazelcastAgent.getUniqueIdForNode();
        }
        return 0;
    }

    /**
     * Initialize the node in stand alone mode without hazelcast.
     *
     * @throws AndesException, UnknownHostException
     */
    private void initStandaloneMode() throws AndesException{

        try {
            // Get Node ID configured by user in broker.xml (if not "default" we must use it as the ID)
            this.nodeId = AndesConfigurationManager.readValue(AndesConfiguration.COORDINATION_NODE_ID);

            if (AndesConfiguration.COORDINATION_NODE_ID.get().getDefaultValue().equals(this.nodeId)) {
                this.nodeId = CoordinationConstants.NODE_NAME_PREFIX + InetAddress.getLocalHost().toString();
            }

            //update node information in durable store
            List<String> nodeList = new ArrayList<String>(andesContextStore.getAllStoredNodeData().keySet());

            for (String node : nodeList) {
                andesContextStore.removeNodeData(node);
            }

            clearAllPersistedStatesOfDisappearedNode(nodeId);

            log.info("Initializing Standalone Mode. Current Node ID:" + this.nodeId);

            andesContextStore.storeNodeDetails(nodeId, (String) AndesConfigurationManager.readValue(AndesConfiguration.TRANSPORTS_BIND_ADDRESS));
        } catch (UnknownHostException e) {
            throw new AndesException("Unable to get the localhost address.", e);
        }
    }

    /**
     * Initializes cluster mode
     *
     * @throws AndesException
     */
    private void initClusterMode() throws AndesException {

        this.hazelcastAgent = HazelcastAgent.getInstance();
        this.nodeId = this.hazelcastAgent.getNodeId();
        log.info("Initializing Cluster Mode. Current Node ID:" + this.nodeId);

        //add node information to durable store
        andesContextStore.storeNodeDetails(nodeId, (String) AndesConfigurationManager.readValue
                (AndesConfiguration.TRANSPORTS_BIND_ADDRESS));

        /**
         * If nodeList size is one, this is the first node joining to cluster. Here we check if
         * there has been any nodes that lived before and somehow suddenly got killed. If there are
         * such nodes clear the state of them and copy back node queue messages of them back to
         * global queue. We need to clear up current node's state as well as there might have been a
         * node with same id and it was killed
         */
        clearAllPersistedStatesOfDisappearedNode(nodeId);

        List<String> storedNodes = new ArrayList<String>(andesContextStore.getAllStoredNodeData().keySet());
        List<String> availableNodeIds = hazelcastAgent.getMembersNodeIDs();
        for (String storedNodeId : storedNodes) {
            if (!availableNodeIds.contains(storedNodeId)) {
                clearAllPersistedStatesOfDisappearedNode(storedNodeId);
            }
        }
        memberAdded();
        log.info("Handling cluster gossip: Node " + nodeId + "  Joined the Cluster");
    }

    /**
     * update global queue synchronizing ID according to current status in cluster
     */
    private void reAssignNodeSyncId() {
        this.nodeSyncSyncId = hazelcastAgent.getIndexOfLocalNode();
    }

    /**
     * Clears all persisted states of a disappeared node
     *
     * @param nodeID node ID
     * @throws AndesException
     */
    private void clearAllPersistedStatesOfDisappearedNode(String nodeID) throws AndesException {

        log.info("Clearing the Persisted State of Node with ID " + nodeID);

        //remove node from nodes list
        andesContextStore.removeNodeData(nodeID);
        //close all local queue and topic subscriptions belonging to the node
        ClusterResourceHolder.getInstance().getSubscriptionManager().closeAllClusterSubscriptionsOfNode(nodeID);

    }

    /**
     * Get the ID of the given node
     *
     * @param node given node
     * @return ID of the node
     */
    public String getNodeId(Member node) {
        return hazelcastAgent.getIdOfNode(node);
    }

    /**
     * set coordinator's thrift server IP and port in hazelcast map.
     */
    public void updateThriftCoordinatorDetailsToMap() {

        String thriftCoordinatorServerIP = AndesContext.getInstance().getThriftServerHost();
        int thriftCoordinatorServerPort = AndesContext.getInstance().getThriftServerPort();


        if (AndesContext.getInstance().getClusteringAgent().isCoordinator()) {
            log.info("This node is elected as the Slot Coordinator. Registering " +
                     thriftCoordinatorServerIP + ":" + thriftCoordinatorServerPort);
            hazelcastAgent.getThriftServerDetailsMap().put(SlotCoordinationConstants.THRIFT_COORDINATOR_SERVER_IP, thriftCoordinatorServerIP);
            hazelcastAgent.getThriftServerDetailsMap().put(SlotCoordinationConstants.THRIFT_COORDINATOR_SERVER_PORT,
                                                           Integer.toString(thriftCoordinatorServerPort));
        }
    }

    /**
     * Sets coordinator's hostname and port in
     * {@link org.wso2.andes.server.cluster.coordination.CoordinationConstants#COORDINATOR_NODE_DETAILS_MAP_NAME}
     * hazelcast map.
     */
    public void updateCoordinatorNodeDetailMap(){
        if (AndesContext.getInstance().getClusteringAgent().isCoordinator()) {
            // Adding cluster coordinator's node IP and port
            hazelcastAgent.getCoordinatorNodeDetailsMap().put(
                    SlotCoordinationConstants.CLUSTER_COORDINATOR_SERVER_IP,
                    hazelcastAgent.getLocalMember().getSocketAddress().getAddress().getHostAddress());
            hazelcastAgent.getCoordinatorNodeDetailsMap().put(
                    SlotCoordinationConstants.CLUSTER_COORDINATOR_SERVER_PORT,
                    Integer.toString(hazelcastAgent.getLocalMember().getSocketAddress().getPort()));
        }
    }

    /**
     * Gets the coordinator node's address. i.e address:port
     *
     * @return Address of the coordinator node
     */
    public String getCoordinatorNodeAddress() {
        if (AndesContext.getInstance().isClusteringEnabled()) {
            IMap<String, String> coordinatorNodeDetailsMap = hazelcastAgent.getCoordinatorNodeDetailsMap();
            if (null != coordinatorNodeDetailsMap) {
                String ipAddress = coordinatorNodeDetailsMap.get(SlotCoordinationConstants.CLUSTER_COORDINATOR_SERVER_IP);
                String port = coordinatorNodeDetailsMap.get(SlotCoordinationConstants.CLUSTER_COORDINATOR_SERVER_PORT);
                if (null != ipAddress && null != port) {
                    return ipAddress + ":" + port;
                }
            }
        }
        return StringUtils.EMPTY;
    }

    /**
     * Gets address of all the members in the cluster. i.e address:port
     *
     * @return A list of address of the nodes in a cluster
     */
    public List<String> getAllClusterNodeAddresses() {
        List<String> addresses = new ArrayList<String>();
        if (AndesContext.getInstance().isClusteringEnabled()) {
            for (Member member : HazelcastAgent.getInstance().getAllClusterMembers()) {
                InetSocketAddress socket = member.getSocketAddress();
                addresses.add(socket.getAddress().getHostAddress() + ":" + socket.getPort());
            }
        }
        return addresses;
    }

}
