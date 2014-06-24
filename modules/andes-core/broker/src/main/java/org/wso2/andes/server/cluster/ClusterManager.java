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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cassandra.OnflightMessageTracker;
import org.wso2.andes.server.cassandra.QueueDeliveryWorker;
import org.wso2.andes.server.cluster.coordination.CoordinationConstants;
import org.wso2.andes.server.cluster.coordination.CoordinationException;
import org.wso2.andes.server.cluster.coordination.ZooKeeperAgent;
import org.wso2.andes.server.configuration.ClusterConfiguration;
import org.apache.zookeeper.*;
import org.wso2.andes.server.util.AndesConstants;
import org.wso2.andes.server.util.AndesUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * F
 * Cluster Manager is responsible for Handling the Broker Cluster Management Tasks like
 * Queue Worker distribution. Fail over handling for cluster nodes. etc.
 */
public class ClusterManager {
    private Log log = LogFactory.getLog(ClusterManager.class);

    private ZooKeeperAgent zkAgent;
    private int nodeId;
    private String zkNode;

    private GlobalQueueManager globalQueueManager;

    //each node is assigned  an ID 0-x after arranging nodeIDs in an ascending order
    private int globalQueueSyncId;

    //Map that Keeps the node id
    private Map<Integer, ClusterNode> nodeMap = new ConcurrentHashMap<Integer, ClusterNode>();

    //in memory map keeping destination Queues active in the cluster
    private List<String> destinationQueueList = Collections.synchronizedList(new ArrayList<String>());

    //in memory map keeping nodeIds assigned for each node in cluster
    private List<Integer> clusterNodeIDList = Collections.synchronizedList(new ArrayList<Integer>());

    //in memory map keeping global queues assigned to the this node
    private List<String> globalQueuesAssignedToMe = Collections.synchronizedList(new ArrayList<String>());

    private String connectionString;

    private MessageStore messageStore;

    private SubscriptionStore subscriptionStore;

    /**
     * Create a ClusterManager instance for clustered environment
     *
     * @param zkConnectionString zookeeper port
     */
    public ClusterManager(String zkConnectionString) {
        this.messageStore = MessagingEngine.getInstance().getCassandraBasedMessageStore();
        this.subscriptionStore = MessagingEngine.getInstance().getSubscriptionStore();
        this.globalQueueManager = new GlobalQueueManager(messageStore);
        this.connectionString = zkConnectionString;
    }

    /**
     * Create a ClusterManager instance for standalone environment.
     * Then this will handle only standalone operations
     */
    public ClusterManager() {
        this.messageStore = MessagingEngine.getInstance().getCassandraBasedMessageStore();
        this.subscriptionStore = MessagingEngine.getInstance().getSubscriptionStore();
        this.globalQueueManager = new GlobalQueueManager(messageStore);
        this.nodeId = 0;
    }

    private static int getNodeIdFromZkNode(String node) {
        return Integer.parseInt(node.substring(node.length() - 5));
    }

    /**
     * Initialize the Cluster manager. This will create ZNodes related to nodes and assign node ids
     *
     * @throws CoordinationException in a Error when communicating with Zookeeper
     */
    public void init() throws CoordinationException {
        final ClusterConfiguration config = ClusterResourceHolder.getInstance().getClusterConfiguration();

        /**
         * do following if clustering is disabled. Here no Zookeeper is involved
         * so nodeID will be always 0
         */
        if (!config.isClusteringEnabled()) {

            //update node information in durable store
            List<String> nodeList = subscriptionStore.getStoredNodeIDList();
            for (String node : nodeList) {
                subscriptionStore.deleteNodeData(node);
            }
            subscriptionStore.addNodeDetails("" + nodeId, config.getBindIpAddress());

            //start all global queue workers on the node
            startAllGlobalQueueWorkers();
            return;
        }


        /**
         * do following if cluster is enabled
         */
        try {

            // create a new node with a generated randomId
            // get the node name and id

            zkAgent = new ZooKeeperAgent(connectionString);
            zkAgent.initQueueWorkerCoordination();
            final String nodeName = CoordinationConstants.QUEUE_WORKER_NODE +
                    (UUID.randomUUID()).toString().replace("-", "_");
            String path = CoordinationConstants.QUEUE_WORKER_COORDINATION_PARENT
                    + nodeName;
            //Register the watcher for zoo keeper parent to be fired when children changed
            zkAgent.getZooKeeper().
                    getChildren(CoordinationConstants.QUEUE_WORKER_COORDINATION_PARENT, new Watcher() {

                        @Override
                        public void process(WatchedEvent watchedEvent) {
                            if (Event.EventType.NodeChildrenChanged == watchedEvent.getType()) {
                                try {
                                    List<String> nodeListFromZK =
                                            zkAgent.getZooKeeper().
                                                    getChildren(
                                                            CoordinationConstants.QUEUE_WORKER_COORDINATION_PARENT, false);

                                    //identify and register this node
                                    for (String node : nodeListFromZK) {
                                        if ((CoordinationConstants.NODE_SEPARATOR + node).contains(nodeName)) {

                                            zkNode = node;
                                            nodeId = getNodeIdFromZkNode(node);
                                            log.info("Initializing Cluster Manager , " + "Selected Node id : " + nodeId);

                                            //add node information to durable store
                                            subscriptionStore.addNodeDetails("" + nodeId, config.getBindIpAddress());

                                            //register a listener for changes on my node data only
                                            zkAgent.getZooKeeper().
                                                    getData(CoordinationConstants.
                                                            QUEUE_WORKER_COORDINATION_PARENT +
                                                            CoordinationConstants.NODE_SEPARATOR +
                                                            node, new NodeDataChangeListener(), null);
                                            break;
                                        }

                                    }

                                    for (String node : nodeListFromZK) {

                                        //update in-memory node list
                                        int id = getNodeIdFromZkNode(node);
                                        clusterNodeIDList.add(id);
                                    }

                                    List<String> storedNodes = subscriptionStore.getStoredNodeIDList();

                                    /**
                                     * If nodeList size is one, this is the first node joining to cluster. Here we check if there has been
                                     * any nodes that lived before and somehow suddenly got killed. If there are such nodes clear the state of them and
                                     * copy back node queue messages of them back to global queue.
                                     * If node was the same machine:ip and zookeeper assigned a different id this logic will handle the confusion
                                     * We need to clear up current node's state as well as there might have been a node with same id and it was killed
                                     */
                                    clearAllPersistedStatesOfDissapearedNode(nodeId);

                                    for (String storedNode : storedNodes) {
                                        int storedNodeId = Integer.parseInt(storedNode);
                                        if (!clusterNodeIDList.contains(storedNodeId)) {
                                            clearAllPersistedStatesOfDissapearedNode(storedNodeId);
                                            checkAndCopyMessagesOfNodeQueueBackToGlobalQueue(nodeId, AndesUtils.getNodeQueueNameForNodeId(storedNodeId));
                                        }

                                    }
                                } catch (Exception e) {
                                    log.error("Error while coordinating cluster information while joining to cluster", e);
                                    throw new RuntimeException(e);
                                }
                            }
                        }
                    });
            // Once this method called above defined watcher will be fired
            zkAgent.getZooKeeper().create(path, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

            //wait until above task is completed
            Thread.sleep(4000);
            //update global queue synchronizing ID
            reAssignGlobalQueueSyncId();
            //handle global queue addition for this node
            handleGlobalQueueAddition();

            //notify node addition to all nodes and register node existence listeners for all nodes on this node
            List<String> nodeList = zkAgent.getZooKeeper().
                    getChildren(CoordinationConstants.QUEUE_WORKER_COORDINATION_PARENT, false);

            for (String node : nodeList) {
                String currentNodePath = CoordinationConstants.QUEUE_WORKER_COORDINATION_PARENT +
                        CoordinationConstants.NODE_SEPARATOR + node;

                String data = CoordinationConstants.NODE_CHANGED_PREFIX + zkNode;
                //notify all other nodes that a new node joined with node ID
                zkAgent.getZooKeeper().setData(currentNodePath, data.getBytes(), -1);
                //Add Listener for node existence of any node in the cluster
                zkAgent.getZooKeeper().exists(currentNodePath, new NodeExistenceListener(node));

            }

        } catch (Exception e) {
            e.printStackTrace();
            String msg = "Error while initializing the zookeeper coordination ";
            log.error("Error while initializing the zookeeper coordination ", e);
            throw new CoordinationException(msg, e);
        }
    }

    /**
     * get global queue manager working in the current node
     *
     * @return
     */
    public GlobalQueueManager getGlobalQueueManager() {
        return this.globalQueueManager;
    }

    /**
     * Start all global queues and workers
     *
     * @throws CoordinationException
     */
    public void startAllGlobalQueueWorkers() throws CoordinationException {

        if (!ClusterResourceHolder.getInstance().getClusterConfiguration().isClusteringEnabled()) {
            List<String> globalQueueNames = AndesUtils.getAllGlobalQueueNames();
            for (String globalQueueName : globalQueueNames) {
                globalQueueManager.scheduleWorkForGlobalQueue(globalQueueName);
            }
        }
    }

    public void handleGlobalQueueAddition() {

        try {
            //get the current globalQueue Assignments
            List<String> currentGlobalQueueAssignments = new ArrayList<String>();
            for (String q : globalQueuesAssignedToMe) {
                currentGlobalQueueAssignments.add(q);
            }
            //update GlobalQueues to be assigned as to new situation in cluster
            updateGlobalQueuesAssignedTome();

            //stop any global queue worker that is not assigned to me now
            for (String globalQueue : currentGlobalQueueAssignments) {
                if (!globalQueuesAssignedToMe.contains(globalQueue)) {
                    globalQueueManager.removeWorker(globalQueue);
                }
            }

            //start global queue workers for queues assigned to me
            for (String globalQueue : globalQueuesAssignedToMe) {
                globalQueueManager.scheduleWorkForGlobalQueue(globalQueue);
            }

        } catch (KeeperException e) {
            log.error("Error in handling global queue worker assignment", e);
        } catch (InterruptedException e) {
            log.error("Error in handling global queue worker assignment", e);
        }


    }

    /**
     * Handles changes needs to be done in current node when a node joins to the cluster
     *
     * @param node node name
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void handleNewNodeJoiningToCluster(String node) throws KeeperException, InterruptedException {
        int id = getNodeIdFromZkNode(node);
        log.info("Handling cluster gossip: Node with ID " + id + " Joined the Cluster");
        //update in memory node map
        clusterNodeIDList.add(id);
        //reassign global queue sync id
        reAssignGlobalQueueSyncId();
        //register a node existence listener for the new node
        zkAgent.getZooKeeper().exists(CoordinationConstants.
                QUEUE_WORKER_COORDINATION_PARENT +
                CoordinationConstants.NODE_SEPARATOR + node,
                new NodeExistenceListener(node));
        //reassign global queues for yourself
        handleGlobalQueueAddition();
    }

    /**
     * update global queue synchronizing ID according to current status in cluster
     */
    private void reAssignGlobalQueueSyncId() {
        Collections.sort(clusterNodeIDList);
        int indexOfMyId = clusterNodeIDList.indexOf(new Integer(nodeId));
        globalQueueSyncId = indexOfMyId;
    }

    /**
     * Start and stop global queue workers
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void updateGlobalQueuesAssignedTome() throws KeeperException, InterruptedException {

        List<String> globalQueuesToBeAssigned = new ArrayList<String>();
        int globalQueueCount = ClusterResourceHolder.getInstance().getClusterConfiguration().getGlobalQueueCount();
        List<String> nodeList = zkAgent.getZooKeeper().
                getChildren(CoordinationConstants.QUEUE_WORKER_COORDINATION_PARENT, false);
        int clusterNodeCount = nodeList.size();
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
     * This class will handle node data changes in ZooKeeper nodes
     */
    private class NodeDataChangeListener implements Watcher {

        @Override
        public void process(WatchedEvent event) {
            if (Event.EventType.NodeDataChanged == event.getType()) {


                //Date Change Event received. This can be Queue addition , Queue removal, Node addition
                try {

                    //as watcher is consumed we have to register a watcher again
                    byte[] data = zkAgent.getZooKeeper().
                            getData(CoordinationConstants.
                                    QUEUE_WORKER_COORDINATION_PARENT +
                                    CoordinationConstants.NODE_SEPARATOR +
                                    zkNode, new NodeDataChangeListener(), null);
                    String dataStr = new String(data);

                    //check if it is a new node joining
                    if (dataStr.startsWith(CoordinationConstants.NODE_CHANGED_PREFIX)) {
                        String node = dataStr.split(":")[1];
                        handleNewNodeJoiningToCluster(node);
                    }

                } catch (Exception e) {
                    log.fatal("Error processing the Node data change : This might cause serious " +
                            "issues in distributed queue management", e);
                }
            }
        }

    }

    /**
     * Called when a node in MB cluster disappears. Any logic that should be handled
     * by other nodes should come here
     */
    private class NodeExistenceListener implements Watcher {

        private String watchZNode = null;

        public NodeExistenceListener(String zNode) {
            this.watchZNode = zNode;
        }

        @Override
        public void process(WatchedEvent watchedEvent) {
            //node left the cluster
            if (Event.EventType.NodeDeleted == watchedEvent.getType()) {
                String path = watchedEvent.getPath();


                String[] parts = path.split(CoordinationConstants.NODE_SEPARATOR);
                String deletedNode = parts[parts.length - 1];

                try {
                    int deletedNodeId = getNodeIdFromZkNode(deletedNode);
                    log.info("Handling cluster gossip: Node with ID " + deletedNodeId + " left the cluster");
                    //Update the durable store
                    subscriptionStore.deleteNodeData("" + deletedNodeId);
                    //update in memory list
                    clusterNodeIDList.remove(new Integer(deletedNodeId));
                    //refresh global queue sync ID
                    reAssignGlobalQueueSyncId();
                    //reassign global queue workers
                    handleGlobalQueueAddition();

                    Collections.sort(clusterNodeIDList);
                    int IdOfMessageCopyTaskOwningNode = clusterNodeIDList.get(0);

                    if(nodeId == IdOfMessageCopyTaskOwningNode) {

                        //clear persisted states of disappeared node
                        clearAllPersistedStatesOfDissapearedNode(deletedNodeId);

                        /**
                         * check and copy back messages of node queue belonging to disappeared node
                         * the node having smallest zkId out of active nodes should handle this task
                         */
                        checkAndCopyMessagesOfNodeQueueBackToGlobalQueue(IdOfMessageCopyTaskOwningNode,AndesUtils.getNodeQueueNameForNodeId(deletedNodeId));
                    }

                } catch (Exception e) {
                    log.error("Error while removing node details", e);
                }

            } else {
                //if some other event type came we have to register the watcher again
                try {
                    zkAgent.getZooKeeper().exists(CoordinationConstants.
                            QUEUE_WORKER_COORDINATION_PARENT +
                            CoordinationConstants.NODE_SEPARATOR + watchZNode,
                            this);

                } catch (Exception e) {
                    e.printStackTrace();
                    log.error("Error while registering a watch for loader node : " + watchZNode, e);
                }
            }

        }

    }

    /**
     * get node id assigned by ZooKeeper for this node
     *
     * @return node id
     */
    public int getNodeId() {
        return nodeId;
    }

    /**
     * get Zookeeper connection
     *
     * @return connection string
     */
    public String getZkConnectionString() {
        return connectionString;
    }

    /**
     * get binding address of the node
     *
     * @param nodeId id of node assigned by zookeeper
     * @return bind address
     */
    public String getNodeAddress(int nodeId) {
        return subscriptionStore.getNodeData("" + nodeId);
    }

    /**
     * get Id list of ZooKeeper nodes
     *
     * @return list of Ids of cluster nodes
     */
    public List<Integer> getZkNodes() {
        List<String> storedNodes = subscriptionStore.getStoredNodeIDList();
        ArrayList<Integer> zkNodeIdList = new ArrayList<Integer>();
        for (String storedNode : storedNodes) {
            Integer ZKId = Integer.parseInt(storedNode);
            zkNodeIdList.add(ZKId);
        }

        return zkNodeIdList;
    }

    public String[] getGlobalQueuesAssigned(int nodeId) {
        List<String> globalQueuesToBeAssigned = new ArrayList<String>();
        try {
            Collections.sort(clusterNodeIDList);
            int indexOfRequestedId = clusterNodeIDList.indexOf(new Integer(nodeId));
            int globalQueueCount = ClusterResourceHolder.getInstance().getClusterConfiguration().getGlobalQueueCount();
            List<String> nodeList = zkAgent.getZooKeeper().
                    getChildren(CoordinationConstants.QUEUE_WORKER_COORDINATION_PARENT, false);
            int clusterNodeCount = nodeList.size();
            for (int count = 0; count < globalQueueCount; count++) {

                if (count % clusterNodeCount == indexOfRequestedId) {
                    globalQueuesToBeAssigned.add(AndesConstants.GLOBAL_QUEUE_NAME_PREFIX + count);
                }
            }
        } catch (KeeperException e) {
            log.error("Error occurred while getting global queues assigned for node", e);
        } catch (InterruptedException e) {
            log.error("Error occurred while getting global queues assigned for node", e);
        }
        String[] globalQueueList = globalQueuesToBeAssigned.toArray(new String[globalQueuesToBeAssigned.size()]);
        return globalQueueList;
    }

    /**
     * get how many messages in the given global queue
     *
     * @param globalQueue global queue name
     * @return
     */
    public int numberOfMessagesInGlobalQueue(String globalQueue) throws AndesException {
        return globalQueueManager.getMessageCountOfGlobalQueue(globalQueue);
    }

    /**
     * Get all topics in the cluster
     *
     * @return list of topics created
     * @throws AndesException
     */
    public List<String> getTopics() throws AndesException {
        return subscriptionStore.getTopics();
    }

    //TODO:hasitha can we implement moving global queue workers?
    public boolean updateWorkerForQueue(String queueToBeMoved, String newNodeToAssign) {
        boolean successful = false;
        return false;
    }

    /**
     * Get whether clustering is enabled
     *
     * @return
     */
    public boolean isClusteringEnabled() {
        ClusterConfiguration config = ClusterResourceHolder.getInstance().getClusterConfiguration();
        return config.isClusteringEnabled();
    }

    /**
     * Get the node ID assigned by ZooKeeper for the current node
     *
     * @return
     */
    public String getMyNodeID() {
        String nodeID = Integer.toString(nodeId);
        return nodeID;
    }

    /**
     * get destination queues in broker cluster
     *
     * @return
     */
    public List<String> getDestinationQueuesInCluster() {
        List<String> queueList = new ArrayList<String>();
        for (AndesQueue queue : subscriptionStore.getDurableQueues()) {
            queueList.add(queue.queueName);
        }
        return queueList;
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
            if(ClusterResourceHolder.getInstance().getClusterConfiguration().isClusteringEnabled()) {
                checkAndCopyMessagesOfNodeQueueBackToGlobalQueue(nodeId, MessagingEngine.getMyNodeQueueName());
            }
        } catch (Exception e) {
            log.error("Error stopping global queues while shutting down", e);
        }

    }

    /**
     * get message count of node queue belonging to given node
     *
     * @param zkId             ZK ID of the node
     * @param destinationQueue destination queue name
     * @return message count
     */
    public int getNodeQueueMessageCount(int zkId, String destinationQueue) throws AndesException {
        String nodeQueueName = AndesUtils.getNodeQueueNameForNodeId(zkId);
        QueueAddress nodeQueueAddress = new QueueAddress(QueueAddress.QueueType.QUEUE_NODE_QUEUE, nodeQueueName);
        return MessagingEngine.getInstance().getCassandraBasedMessageStore().countMessagesOfQueue(nodeQueueAddress, destinationQueue);
    }


    public void clearAllPersistedStatesOfDissapearedNode(int nodeID) {

        log.info("Clearing the Persisted State of Node with ID " + nodeID);

        SubscriptionStore subscriptionStore = MessagingEngine.getInstance().getSubscriptionStore();
        try {

            //remove node from nodes list
            subscriptionStore.deleteNodeData("" + nodeID);

            //if in stand-alone mode close all local queue and topic subscriptions
            if(!ClusterResourceHolder.getInstance().getClusterConfiguration().isClusteringEnabled()) {
                synchronized (this) {
                    subscriptionStore.closeAllLocalSubscriptionsOfNode(true);
                    subscriptionStore.closeAllLocalSubscriptionsOfNode(false);
                }
            } else {
                //close all cluster queue and topic subscriptions for the node
                subscriptionStore.closeAllClusterSubscriptionsOfNode(nodeID, false);
                subscriptionStore.closeAllClusterSubscriptionsOfNode(nodeID, true);
            }

        } catch (AndesException e) {
            log.error("Error while clearing state of disappeared node", e);
        }
    }






    /**
     * remove in-memory messages tracked for this queue
     *
     * @param destinationQueueName name of queue messages should be removed
     * @throws AndesException
     */
    public void removeInMemoryMessagesAccumulated(String destinationQueueName) throws AndesException {
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
    private void checkAndCopyMessagesOfNodeQueueBackToGlobalQueue(int IdOfTaskOwningNode, String nodeQueueName) throws AndesException {
        if (this.nodeId == IdOfTaskOwningNode) {
            MessageStore messageStore = MessagingEngine.getInstance().getCassandraBasedMessageStore();
            QueueAddress nodeQueueAddress = new QueueAddress(QueueAddress.QueueType.QUEUE_NODE_QUEUE, nodeQueueName);
            long lastProcessedMessageID = 0;
            int numberOfMessagesMoved = 0;
            List<AndesMessageMetadata> messageList = messageStore.getNextNMessageMetadataFromQueue(nodeQueueAddress, lastProcessedMessageID, 40);
            while (messageList.size() != 0) {
                Iterator<AndesMessageMetadata> metadataIterator = messageList.iterator();
                while (metadataIterator.hasNext()) {
                    AndesMessageMetadata metadata = metadataIterator.next();
                    lastProcessedMessageID = metadata.getMessageID();
                }
                messageStore.moveMessageMetaData(nodeQueueAddress, null, messageList);
                numberOfMessagesMoved += messageList.size();
                messageList = messageStore.getNextNMessageMetadataFromQueue(nodeQueueAddress, lastProcessedMessageID, 40);
            }

            log.info("Moved " + numberOfMessagesMoved
                    + " Number of Messages from Node Queue "
                    + nodeQueueName + "to Global Queues ");
        }
    }

}
