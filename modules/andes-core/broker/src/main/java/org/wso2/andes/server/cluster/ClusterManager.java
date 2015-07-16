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


import com.hazelcast.core.Member;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.slot.SlotManagerClusterMode;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.coordination.CoordinationConstants;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;

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

    /**
     * Class logger
     */
    private Log log = LogFactory.getLog(ClusterManager.class);

    /**
     * Id of the local node
     */
    private String nodeId;

    /**
     * AndesContextStore instance
     */
    private AndesContextStore andesContextStore;

    /**
     * Cluster agent for managing cluster communication
     */
    private ClusterAgent clusterAgent;

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

        if (AndesContext.getInstance().isClusteringEnabled()) {
            initClusterMode();
        } else {
            initStandaloneMode();
        }
    }

    /**
     * Handles changes needs to be done in current node when a node joins to the cluster
     */
    public void memberAdded(String addedNodeId) {
        log.info("Handling cluster gossip: Node " + addedNodeId + "  Joined the Cluster");
    }

    /**
     * Handles changes needs to be done in current node when a node leaves the cluster
     * @param deletedNodeId deleted node id
     */
    public void memberRemoved(String deletedNodeId) throws AndesException {
        log.info("Handling cluster gossip: Node " + deletedNodeId + "  left the Cluster");

        SlotManagerClusterMode.getInstance().setRemovedNode(deletedNodeId);

        if(clusterAgent.isCoordinator()) {
            SlotManagerClusterMode.getInstance().recoverSlots();

            //clear persisted states of disappeared node
            clearAllPersistedStatesOfDisappearedNode(deletedNodeId);

            //Reassign the slot to free slots pool
            SlotManagerClusterMode.getInstance().reAssignSlotsWhenMemberLeaves(deletedNodeId);
        }

        // Deactivate durable subscriptions belonging to the node
        ClusterResourceHolder.getInstance().getSubscriptionManager().deactivateClusterDurableSubscriptionsForNodeID(
                clusterAgent.isCoordinator(), deletedNodeId);
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
     * Perform cleanup tasks before shutdown
     */
    public void prepareLocalNodeForShutDown() throws AndesException {
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
            return clusterAgent.getUniqueIdForLocalNode();
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
            List<String> nodeList = new ArrayList<>(andesContextStore.getAllStoredNodeData().keySet());

            for (String node : nodeList) {
                andesContextStore.removeNodeData(node);
            }

            clearAllPersistedStatesOfDisappearedNode(nodeId);

            log.info("Initializing Standalone Mode. Current Node ID:" + this.nodeId + " "
                     + InetAddress.getLocalHost().getHostAddress());

            andesContextStore.storeNodeDetails(nodeId, InetAddress.getLocalHost().getHostAddress());
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

        // Set the cluster agent from the Andes Context.
        this.clusterAgent = AndesContext.getInstance().getClusterAgent();

        clusterAgent.start(this);

        this.nodeId = clusterAgent.getLocalNodeIdentifier();
        log.info("Initializing Cluster Mode. Current Node ID:" + this.nodeId);

        String localMemberHostAddress = clusterAgent.getLocalNodeIdentifier();

        if (log.isDebugEnabled()) {
            log.debug("Stored node ID : " + this.nodeId +  ". Stored node data(Hazelcast local "
                      + "member host address) : " + localMemberHostAddress);
        }

        //add node information to durable store
        andesContextStore.storeNodeDetails(nodeId, localMemberHostAddress);

        /**
         * If nodeList size is one, this is the first node joining to cluster. Here we check if
         * there has been any nodes that lived before and somehow suddenly got killed. If there are
         * such nodes clear the state of them and copy back node queue messages of them back to
         * global queue. We need to clear up current node's state as well as there might have been a
         * node with same id and it was killed
         */
        clearAllPersistedStatesOfDisappearedNode(nodeId);

        List<String> storedNodes = new ArrayList<>(andesContextStore.getAllStoredNodeData().keySet());
        List<String> availableNodeIds = clusterAgent.getAllNodeIdentifiers();
        for (String storedNodeId : storedNodes) {
            if (!availableNodeIds.contains(storedNodeId)) {
                clearAllPersistedStatesOfDisappearedNode(storedNodeId);
            }
        }
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
     * Perform coordinator initialization tasks, when this node is elected as the new coordinator
     */
    public void localNodeElectedAsCoordinator() {
    }

    /**
     * Gets the coordinator node's address. i.e address:port
     *
     * @return Address of the coordinator node
     */
    public String getCoordinatorNodeAddress() {
        if (AndesContext.getInstance().isClusteringEnabled()) {
            CoordinatorInformation coordinatorDetails = clusterAgent.getCoordinatorDetails();
            String ipAddress = coordinatorDetails.getHostname();
            String port = coordinatorDetails.getPort();
            if (null != ipAddress && null != port) {
                return ipAddress + ":" + port;
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
        List<String> addresses = new ArrayList<>();
        if (AndesContext.getInstance().isClusteringEnabled()) {
            for (Member member : HazelcastAgent.getInstance().getAllClusterMembers()) {
                InetSocketAddress socket = member.getSocketAddress();
                addresses.add(socket.getAddress().getHostAddress() + ":" + socket.getPort());
            }
        }
        return addresses;
    }

    /**
     * Gets the message store's health status
     *
     * @return true if healthy, else false.
     */
    public boolean getStoreHealth() {
        String myNodeId = getMyNodeID();

        boolean isMessageStoreOperational = AndesContext.getInstance().getMessageStore().isOperational(myNodeId,
                System.currentTimeMillis());
        boolean isAndesContextStoreOperational = AndesContext.getInstance().getAndesContextStore().isOperational
                (myNodeId, System.currentTimeMillis());

        return isMessageStoreOperational && isAndesContextStoreOperational;
    }
}
