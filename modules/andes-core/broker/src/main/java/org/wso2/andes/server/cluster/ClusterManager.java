/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cassandra.OnflightMessageTracker;
import org.wso2.andes.server.cassandra.QueueDeliveryWorker;
import org.wso2.andes.server.cluster.coordination.CoordinationConstants;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;
import org.wso2.andes.server.configuration.ClusterConfiguration;
import org.wso2.andes.server.util.AndesConstants;
import org.wso2.andes.server.util.AndesUtils;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
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
    private int globalQueueSyncId;

    /**
     * GlobalQueueManager to manage global queues
     */
    private GlobalQueueManager globalQueueManager;

    /**
     * in memory map keeping global queues assigned to the this node
     */
    private List<String> globalQueuesAssignedToMe = Collections.synchronizedList(new ArrayList<String>());

    /**
     * AndesContextStore instance
     */
    private AndesContextStore andesContextStore;

    private SlotManager slotManager;

    /**
     * Create a ClusterManager instance
     */
    public ClusterManager() {
        this.andesContextStore = AndesContext.getInstance().getAndesContextStore();
        this.globalQueueManager = new GlobalQueueManager(MessagingEngine.getInstance().getDurableMessageStore());
        this.slotManager = SlotManager.getInstance();

    }

    /**
     * Initialize the Cluster manager.
     *
     * @throws Exception
     */
    public void init() throws Exception {
        try {
            if (!AndesContext.getInstance().isClusteringEnabled()) {
                this.initStandaloneMode();
                return;
            }

            initClusterMode();

        } catch (Exception e) {
            log.error("Error while initializing the Hazelcast coordination ", e);
            throw e;
        }
    }

    /**
     * get global queue manager working in the current node
     *
     * @return global queue manager
     */
    public GlobalQueueManager getGlobalQueueManager() {
        return this.globalQueueManager;
    }

    /**
     * Handles changes needs to be done in current node when a node joins to the cluster
     */
    public void handleNewNodeJoiningToCluster(Member node) {
        String nodeId = hazelcastAgent.getIdOfNode(node);

        if (log.isInfoEnabled()) {
            log.info("Handling cluster gossip: Node with ID " + nodeId + " joined the cluster");
        }

        reAssignGlobalQueueSyncId();
        handleGlobalQueueAddition();
    }

    /**
     * Handles changes needs to be done in current node when a node leaves the cluster
     */
    public void handleNodeLeavingCluster(Member node) throws AndesException {
        String deletedNodeId = hazelcastAgent.getIdOfNode(node);

        if (log.isInfoEnabled()) {
            log.info("Handling cluster gossip: Node with ID " + deletedNodeId + " left the cluster");
        }

        //refresh global queue sync ID
        reAssignGlobalQueueSyncId();
        //reassign global queue workers
        handleGlobalQueueAddition();

        // Below steps are carried out only by the 0th node of the list.
        if (globalQueueSyncId == 0) {
            if (log.isDebugEnabled()) {
                log.debug("Removing persisted states of the left node:" + deletedNodeId);
            }

            //Update the durable store
            andesContextStore.removeNodeData(deletedNodeId);

            //clear persisted states of disappeared node
            clearAllPersistedStatesOfDissapearedNode(deletedNodeId);

            //Reassign the slot to free slots pool
             slotManager.reAssignAssignedSlotsToFreeSlotsPool(deletedNodeId);
            // check and copy back messages of node queue belonging to disappeared node
            checkAndCopyMessagesOfNodeQueueBackToGlobalQueue(AndesUtils.getNodeQueueNameForNodeId(deletedNodeId));
        }
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
     * Get all global queues assigned to a node
     *
     * @param nodeId Id of the node
     * @return global queues as an array of Strings
     */
    public String[] getGlobalQueuesAssigned(String nodeId) {
        List<String> globalQueuesToBeAssigned = new ArrayList<String>();
        List<String> membersUniqueRepresentations = new ArrayList<String>();

        for (Member member : hazelcastAgent.getAllClusterMembers()) {
            membersUniqueRepresentations.add(member.getUuid());
        }

        Collections.sort(membersUniqueRepresentations);

        int indexOfRequestedId = membersUniqueRepresentations.indexOf(nodeId.substring(nodeId.length() - 36, nodeId.length()));
        int globalQueueCount = ClusterResourceHolder.getInstance().getClusterConfiguration().getGlobalQueueCount();
        int clusterNodeCount = hazelcastAgent.getClusterSize();

        for (int count = 0; count < globalQueueCount; count++) {
            if (count % clusterNodeCount == indexOfRequestedId) {
                globalQueuesToBeAssigned.add(AndesConstants.GLOBAL_QUEUE_NAME_PREFIX + count);
            }
        }

        return globalQueuesToBeAssigned.toArray(new String[globalQueuesToBeAssigned.size()]);
    }

    /**
     * get how many messages in the given global queue
     *
     * @param globalQueue global queue name
     * @return number of messages in the global queue
     */
    public int numberOfMessagesInGlobalQueue(String globalQueue) throws AndesException {
        return globalQueueManager.getMessageCountOfGlobalQueue(globalQueue);
    }

    //TODO:hasitha can we implement moving global queue workers?
    public boolean updateWorkerForQueue(String queueToBeMoved, String newNodeToAssign) {
        return false;
    }

    /**
     * Get the node ID of the current node
     *
     * @return ID of the local node
     */
    public String getMyNodeID() {
        return nodeId;
    }

    /**
     * gracefully stop all global queue workers assigned for the current node
     */
    public void shutDownMyNode() {
        try {

            //clear stored node IDS and mark subscriptions of node as closed
            clearAllPersistedStatesOfDissapearedNode(nodeId);
            //stop all global queue Workers
            globalQueueManager.removeAllQueueWorkersLocally();
            //if in clustered mode copy back node queue messages back to global queue
            if (AndesContext.getInstance().isClusteringEnabled()) {
                checkAndCopyMessagesOfNodeQueueBackToGlobalQueue(MessagingEngine.getMyNodeQueueName());
            }
        } catch (Exception e) {
            log.error("Error stopping global queues while shutting down", e);
        }

    }

    /**
     * get message count of node queue belonging to given node
     *
     * @param nodeId           ID of the node
     * @param destinationQueue destination queue name
     * @return message count
     */
    public int getNodeQueueMessageCount(String nodeId, String destinationQueue) throws AndesException {
        String nodeQueueName = AndesUtils.getNodeQueueNameForNodeId(nodeId);
        QueueAddress nodeQueueAddress = new QueueAddress(QueueAddress.QueueType.QUEUE_NODE_QUEUE, nodeQueueName);
        return MessagingEngine.getInstance().getDurableMessageStore().countMessagesOfQueue(nodeQueueAddress, destinationQueue);
    }

    public int getUniqueIdForLocalNode() {
        if (AndesContext.getInstance().isClusteringEnabled()) {
            return hazelcastAgent.getUniqueIdForTheNode();
        }
        return 0;
    }

    /**
     * remove in-memory messages tracked for this queue
     *
     * @param destinationQueueName name of queue messages should be removed
     */
    public void removeInMemoryMessagesAccumulated(String destinationQueueName) {
        //remove in-memory messages accumulated due to sudden subscription closing
        QueueDeliveryWorker queueDeliveryWorker = ClusterResourceHolder.getInstance().getQueueDeliveryWorker();
        if (queueDeliveryWorker != null) {
            queueDeliveryWorker.clearMessagesAccumilatedDueToInactiveSubscriptionsForQueue(destinationQueueName);
        }
        //remove sent but not acked messages
        OnflightMessageTracker.getInstance().getSentButNotAckedMessagesOfQueue(destinationQueueName);
    }

    /**
     * check and move all metadata of messages of node queue to global queue
     */
    private void checkAndCopyMessagesOfNodeQueueBackToGlobalQueue(String nodeQueueName) throws AndesException {
        MessageStore messageStore = MessagingEngine.getInstance().getDurableMessageStore();
        QueueAddress nodeQueueAddress = new QueueAddress(QueueAddress.QueueType.QUEUE_NODE_QUEUE, nodeQueueName);
        long lastProcessedMessageID = 0;
        int numberOfMessagesMoved = 0;
        List<AndesMessageMetadata> messageList = messageStore.getNextNMessageMetadataFromQueue(nodeQueueAddress, lastProcessedMessageID, 40);
        while (messageList.size() != 0) {
            for (AndesMessageMetadata metadata : messageList) {
                lastProcessedMessageID = metadata.getMessageID();
            }
            messageStore.moveMessageMetaData(nodeQueueAddress, null, messageList);
            numberOfMessagesMoved += messageList.size();
            messageList = messageStore.getNextNMessageMetadataFromQueue(nodeQueueAddress, lastProcessedMessageID, 40);
        }

        if (log.isInfoEnabled()) {
            log.info("Moved " + numberOfMessagesMoved
                    + " Number of Messages from Node Queue "
                    + nodeQueueName + "to Global Queues ");
        }

    }

    private void initStandaloneMode() throws Exception {
        final ClusterConfiguration config = ClusterResourceHolder.getInstance().getClusterConfiguration();

        this.nodeId = CoordinationConstants.NODE_NAME_PREFIX + InetAddress.getLocalHost().toString();

        //update node information in durable store
        List<String> nodeList = new ArrayList<String>(andesContextStore.getAllStoredNodeData().keySet());

        for (String node : nodeList) {
            andesContextStore.removeNodeData(node);
        }

        clearAllPersistedStatesOfDissapearedNode(nodeId);

        if (log.isInfoEnabled()) {
            log.info("NodeID:" + this.nodeId);
        }

        andesContextStore.storeNodeDetails(nodeId, config.getBindIpAddress());

        //start all global queue workers on the node
        //TODO commented by sajini
        //startAllGlobalQueueWorkers();
    }

    private void initClusterMode() throws Exception {
        final ClusterConfiguration config = ClusterResourceHolder.getInstance().getClusterConfiguration();

        this.hazelcastAgent = HazelcastAgent.getInstance();
        this.nodeId = this.hazelcastAgent.getNodeId();
        if (log.isInfoEnabled()) {
            log.info("NodeID:" + this.nodeId);
        }

        //add node information to durable store
        andesContextStore.storeNodeDetails(nodeId, config.getBindIpAddress());

        /**
         * If nodeList size is one, this is the first node joining to cluster. Here we check if there has been
         * any nodes that lived before and somehow suddenly got killed. If there are such nodes clear the state of them and
         * copy back node queue messages of them back to global queue.
         * We need to clear up current node's state as well as there might have been a node with same id and it was killed
         */
        clearAllPersistedStatesOfDissapearedNode(nodeId);

        List<String> storedNodes = new ArrayList<String>(andesContextStore.getAllStoredNodeData().keySet());
        List<String> availableNodeIds = hazelcastAgent.getMembersNodeIDs();
        for (String storedNodeId : storedNodes) {
            if (!availableNodeIds.contains(storedNodeId)) {
                clearAllPersistedStatesOfDissapearedNode(storedNodeId);
                checkAndCopyMessagesOfNodeQueueBackToGlobalQueue(AndesUtils.getNodeQueueNameForNodeId(storedNodeId));
            }
        }
        handleNewNodeJoiningToCluster(hazelcastAgent.getLocalMember());
    }

    /**
     * update global queue synchronizing ID according to current status in cluster
     */
    private void reAssignGlobalQueueSyncId() {
        this.globalQueueSyncId = hazelcastAgent.getIndexOfLocalNode();
    }

    /**
     * Start and stop global queue workers
     */
    private void updateGlobalQueuesAssignedTome() {

        List<String> globalQueuesToBeAssigned = new ArrayList<String>();
        int globalQueueCount = ClusterResourceHolder.getInstance().getClusterConfiguration().getGlobalQueueCount();
        int clusterNodeCount = hazelcastAgent.getClusterSize();
        for (int count = 0; count < globalQueueCount; count++) {
            if (count % clusterNodeCount == globalQueueSyncId) {
                globalQueuesToBeAssigned.add(AndesConstants.GLOBAL_QUEUE_NAME_PREFIX + count);
            }
        }
        this.globalQueuesAssignedToMe.clear();
        for (String q : globalQueuesToBeAssigned) {
            globalQueuesAssignedToMe.add(q);
        }
    }

    /**
     * Start all global queues and workers
     */
    private void startAllGlobalQueueWorkers() {
        if (!AndesContext.getInstance().isClusteringEnabled()) {
            List<String> globalQueueNames = AndesUtils.getAllGlobalQueueNames();
            for (String globalQueueName : globalQueueNames) {
                globalQueueManager.scheduleWorkForGlobalQueue(globalQueueName);
            }
        }
    }

    /**
     * When redistributing the global queues among cluster nodes, some nodes will get more global queues
     * than the existing global queues. This case is handled by below methods.
     */
    private void handleGlobalQueueAddition() {
        //get the current globalQueue Assignments
        List<String> currentGlobalQueueAssignments = new ArrayList<String>();
        for (String q : globalQueuesAssignedToMe) {
            currentGlobalQueueAssignments.add(q);
        }

        //update GlobalQueues to be assigned as to new situation in cluster
        updateGlobalQueuesAssignedTome();

        //stop any global queue worker that is not assigned to me now
        //TODO commented by sajini

//        for (String globalQueue : currentGlobalQueueAssignments) {
//            if (!globalQueuesAssignedToMe.contains(globalQueue)) {
//                globalQueueManager.removeWorker(globalQueue);
//            }
//        }

        //start global queue workers for queues assigned to me
        //TODO commented by sajini
//        for (String globalQueue : globalQueuesAssignedToMe) {
//            globalQueueManager.scheduleWorkForGlobalQueue(globalQueue);
//        }
    }

    /**
     * This method should be called when a node leaves the cluster to delete the persisted states of the disappeared nodes
     *
     * @param nodeID Id of the node
     * @throws AndesException
     */
    private void clearAllPersistedStatesOfDissapearedNode(String nodeID) throws AndesException {
        if (log.isDebugEnabled()) {
            log.info("Clearing the Persisted State of Node with ID " + nodeID);
        }

        //remove node from nodes list
        andesContextStore.removeNodeData(nodeID);

        if (!AndesContext.getInstance().isClusteringEnabled()) {
            //if in stand-alone mode close all local queue and topic subscriptions
            synchronized (this) {
                ClusterResourceHolder.getInstance().getSubscriptionManager().closeAllLocalSubscriptionsOfNode(nodeID);
            }
        } else {
            //close all cluster queue and topic subscriptions for the node
            ClusterResourceHolder.getInstance().getSubscriptionManager().closeAllClusterSubscriptionsOfNode(nodeID);
        }
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
}
