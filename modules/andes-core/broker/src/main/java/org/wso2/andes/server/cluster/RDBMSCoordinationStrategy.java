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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.server.cluster.coordination.rdbms.MembershipEventType;
import org.wso2.andes.server.cluster.coordination.rdbms.RDBMSMembershipEventingEngine;
import org.wso2.andes.server.cluster.coordination.rdbms.RDBMSMembershipListener;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * RDBMSCoordinationStrategy uses a RDBMS based approached to identify membership event related to the cluster.
 * This includes electing coordinator and notifying member added and left events.
 */
class RDBMSCoordinationStrategy implements CoordinationStrategy, RDBMSMembershipListener {
    /**
     * Class logger
     */
    private Log logger = LogFactory.getLog(RDBMSCoordinationStrategy.class);

    /**
     * Used to send and receive cluster notifications
     */
    private RDBMSMembershipEventingEngine membershipEventingEngine;

    /**
     * Local node's thrift server socket address
     */
    private InetSocketAddress thriftAddress;

    /**
     * Time waited before notifying others about coordinator change.
     */
    private int coordinatorEntryCreationWaitTime;

    /**
     * Used to identify a node using its IP address and port
     */
    private InetSocketAddress hazelcastAddress;

    /**
     * Possible node states
     *
     *               +----------+
     *     +-------->+ Election +<---------+
     *     |         +----------+          |
     *     |            |    |             |
     *     |            |    |             |
     *  +-----------+   |    |   +-------------+
     *  | Candidate +<--+    +-->+ Coordinator |
     *  +-----------+            +-------------+
     */
    private enum NodeState {
        COORDINATOR, CANDIDATE, ELECTION
    }

    /**
     * Heartbeat interval in seconds
     */
    private final int heartBeatInterval;

    /**
     * After this much of time the node is assumed to have left the cluster
     */
    private final int heartbeatMaxAge;

    /**
     * Cluster agent used to notify about the coordinator change status
     */
    private CoordinationConfigurableClusterAgent configurableClusterAgent;

    /**
     * Long running coordination task
     */
    private CoordinatorElectionTask coordinatorElectionTask;
    /**
     * Current state of the node
     */
    private NodeState currentNodeState;

    /**
     * Used to communicate with the context store
     */
    private AndesContextStore contextStore;

    /**
     * Used to uniquely identify a node in the cluster
     */
    private String localNodeId;

    /**
     * Thread executor used to run the coordination algorithm
     */
    private final ExecutorService threadExecutor;

    /**
     * Default constructor
     */
    RDBMSCoordinationStrategy() {
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("RDBMSCoordinationStrategy-%d")
                                                                     .build();
        threadExecutor = Executors.newSingleThreadExecutor(namedThreadFactory);

        heartBeatInterval = AndesConfigurationManager
                .readValue(AndesConfiguration.RDBMS_BASED_COORDINATION_HEARTBEAT_INTERVAL);
        coordinatorEntryCreationWaitTime = AndesConfigurationManager
                .readValue(AndesConfiguration.RDBMS_BASED_COORDINATOR_ENTRY_CREATION_WAIT_TIME);
                //(heartBeatInterval / 2) + 1;

        // Maximum age of a heartbeat. After this much of time, the heartbeat is considered invalid and node is
        // considered to have left the cluster.
        heartbeatMaxAge = heartBeatInterval * 2;

        if (heartBeatInterval <= coordinatorEntryCreationWaitTime) {
            throw new RuntimeException("Configuration error. " + AndesConfiguration
                    .RDBMS_BASED_COORDINATION_HEARTBEAT_INTERVAL + " * 2 should be greater than " +
                    AndesConfiguration.RDBMS_BASED_COORDINATOR_ENTRY_CREATION_WAIT_TIME);
        }
    }

    /*
    * ======================== Methods from RDBMSMembershipListener ============================
    */

    /**
     * {@inheritDoc}
     */
    @Override
    public void memberAdded(String nodeID) {
        configurableClusterAgent.memberAdded(nodeID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void memberRemoved(String nodeID) {
        try {
            configurableClusterAgent.memberRemoved(nodeID);
        } catch (AndesException e) {
            logger.error("Error while handling node removal, " + nodeID, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void coordinatorChanged(String coordinator) {
        // Do nothing
    }

    /*
    * ======================== Methods from CoordinationStrategy ============================
    */

    /**
     * {@inheritDoc}
     */
    @Override
    public void start(CoordinationConfigurableClusterAgent configurableClusterAgent, String nodeId,
            InetSocketAddress thriftAddress, InetSocketAddress hazelcastAddress) {
        localNodeId = nodeId;
        this.thriftAddress = thriftAddress;
        this.hazelcastAddress = hazelcastAddress;

        // TODO detect if already started
        contextStore = AndesContext.getInstance().getAndesContextStore();
        this.configurableClusterAgent = configurableClusterAgent;

        membershipEventingEngine = new RDBMSMembershipEventingEngine();
        membershipEventingEngine.start(nodeId);

        // Register listener for membership changes
        membershipEventingEngine.addEventListener(this);

        currentNodeState = NodeState.ELECTION;

        // Clear old membership events for current node.
        try {
            contextStore.clearMembershipEvents(nodeId);
        } catch (AndesException e) {
            logger.warn("Error while clearing old membership events for local node (" + nodeId + ")", e);
        }

        coordinatorElectionTask = new CoordinatorElectionTask();
        threadExecutor.execute(coordinatorElectionTask);

        // Wait until node state become Candidate/Coordinator because thrift server needs to start after that.
        int timeout = 500;
        int waitTime = 0;
        int maxWaitTime = heartbeatMaxAge * 5;
        while (currentNodeState == NodeState.ELECTION) {
            try {
                TimeUnit.MILLISECONDS.sleep(timeout);
                waitTime = waitTime + timeout;
                if (waitTime == maxWaitTime) {
                    throw new RuntimeException("Node is stuck in the ELECTION state for "
                            + waitTime + " milliseconds.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("An error occurred while waiting to get current node state.", e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCoordinator() {
        return currentNodeState == NodeState.COORDINATOR;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InetSocketAddress getThriftAddressOfCoordinator() {
        InetSocketAddress coordinatorThriftAddress = null;

        try {
            coordinatorThriftAddress = contextStore.getCoordinatorThriftAddress();
        } catch (AndesException e) {
            logger.error("Error occurred while reading coordinator thrift address", e);
        }

        return coordinatorThriftAddress;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getAllNodeIdentifiers() throws AndesException {
        List<NodeHeartBeatData> allNodeInformation = contextStore.getAllHeartBeatData();

        return getNodeIds(allNodeInformation);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<NodeDetail> getAllNodeDetails() throws AndesException {
        List<NodeDetail> nodeDetails = new ArrayList<>();

        List<NodeHeartBeatData> allHeartBeatData = contextStore.getAllHeartBeatData();
        String coordinatorNodeId = contextStore.getCoordinatorNodeId();

        for (NodeHeartBeatData nodeHeartBeatData : allHeartBeatData) {
            boolean isCoordinatorNode = coordinatorNodeId.equals(nodeHeartBeatData.getNodeId());

            nodeDetails.add(new NodeDetail(nodeHeartBeatData.getNodeId(),
                    nodeHeartBeatData.getClusterAgentAddress(),
                    isCoordinatorNode));
        }
        return nodeDetails;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        // TODO detect if started
        if (isCoordinator()) {
            try {
                contextStore.removeCoordinator();
            } catch (AndesException e) {
                logger.error("Error occurred while removing coordinator when shutting down", e);
            }
        }

        membershipEventingEngine.stop();
        coordinatorElectionTask.stop();
        threadExecutor.shutdown();
    }

    /**
     * Return a list of node ids from the heartbeat data list
     *
     * @param allHeartbeatData list of heartbeat data
     * @return list of node IDs
     */
    private List<String> getNodeIds(List<NodeHeartBeatData> allHeartbeatData) {
        List<String> allNodeIds = new ArrayList<>(allHeartbeatData.size());
        for (NodeHeartBeatData nodeHeartBeatData : allHeartbeatData) {
            allNodeIds.add(nodeHeartBeatData.getNodeId());
        }
        return allNodeIds;
    }

    /**
     * The main task used to run the coordination algorithm
     */
    private class CoordinatorElectionTask implements Runnable {
        /**
         * Indicate if the task should run
         */
        private boolean running;

        private CoordinatorElectionTask() {
            running = true;
        }

        @Override
        public void run() {
            while (running) {

                try {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Current node state: " + currentNodeState);
                    }

                    switch (currentNodeState) {
                    case CANDIDATE:
                        currentNodeState = performStandByTask();
                        break;
                    case COORDINATOR:
                        currentNodeState = performCoordinatorTask();
                        break;
                    case ELECTION:
                        currentNodeState = performElectionTask();
                        break;
                    }

                } catch (Throwable e) {
                    logger.error("Error detected while running coordination algorithm. Node became a "
                            + NodeState.CANDIDATE + " node", e);
                    currentNodeState = NodeState.CANDIDATE;
                }

            }
        }

        /**
         * Perform periodic task that should be done by a CANDIDATE node
         *
         * @return next NodeStatus
         * @throws AndesException
         * @throws InterruptedException
         */
        private NodeState performStandByTask() throws AndesException, InterruptedException {
            NodeState nextState;

            updateNodeHeartBeat();

            // Read current coordinators validity. We can improve this by returning the status (TIMED_OUT or DELETED or
            // VALID)from this call. If DELETED we do not have to check a second time.
            boolean coordinatorValid = contextStore.checkIfCoordinatorValid(heartbeatMaxAge);
            TimeUnit.MILLISECONDS.sleep(heartBeatInterval);

            if (coordinatorValid) {
                nextState = NodeState.CANDIDATE;
            } else {
                coordinatorValid = contextStore.checkIfCoordinatorValid(heartbeatMaxAge);

                if (coordinatorValid) {
                    nextState = NodeState.CANDIDATE;
                } else {
                    logger.info("Going for election since the Coordinator is invalid");

                    contextStore.removeCoordinator();
                    nextState = NodeState.ELECTION;
                }
            }
            return nextState;
        }

        /**
         * Try to update the heart beat entry for local node in the DB. If the entry is deleted by the coordinator,
         * this will recreate the entry.
         *
         * @throws AndesException
         */
        private void updateNodeHeartBeat() throws AndesException {
            boolean heartbeatEntryExists = contextStore.updateNodeHeartbeat(localNodeId);
            if (!heartbeatEntryExists) {
                contextStore.createNodeHeartbeatEntry(localNodeId, hazelcastAddress);
            }
        }

        /**
         * Perform periodic task that should be done by a Coordinating node
         *
         * @return next NodeState
         * @throws AndesException
         * @throws InterruptedException
         */
        private NodeState performCoordinatorTask() throws AndesException, InterruptedException {
            // Try to update the coordinator heartbeat
            boolean stillCoordinator = contextStore.updateCoordinatorHeartbeat(localNodeId);

            if (stillCoordinator) {
                updateNodeHeartBeat();

                long currentTimeMillis = System.currentTimeMillis();
                List<NodeHeartBeatData> allNodeInformation = contextStore.getAllHeartBeatData();

                List<String> allActiveNodeIds = getNodeIds(allNodeInformation);
                List<String> newNodes = new ArrayList<>();
                List<String> removedNodes = new ArrayList<>();

                for (NodeHeartBeatData nodeHeartBeatData : allNodeInformation) {
                    long heartbeatAge = currentTimeMillis - nodeHeartBeatData.getLastHeartbeat();

                    String nodeId = nodeHeartBeatData.getNodeId();
                    if (nodeHeartBeatData.isNewNode()) {
                        newNodes.add(nodeId);

                        // update node info as read
                        contextStore.markNodeAsNotNew(nodeId);
                    } else if (heartbeatAge >= heartbeatMaxAge) {
                        removedNodes.add(nodeId);
                        allActiveNodeIds.remove(nodeId);

                        contextStore.removeNodeHeartbeat(nodeId);
                    }
                }

                for (String newNode : newNodes) {
                    logger.info("Member added " + newNode);
                    membershipEventingEngine.notifyMembershipEvent(allActiveNodeIds, MembershipEventType.MEMBER_ADDED,
                            newNode);
                }

                for (String removedNode : removedNodes) {
                    logger.info("Member removed " + removedNode);
                    membershipEventingEngine.notifyMembershipEvent(allActiveNodeIds, MembershipEventType.MEMBER_REMOVED,
                            removedNode);
                }

                TimeUnit.MILLISECONDS.sleep(heartBeatInterval);
                return NodeState.COORDINATOR;
            } else {
                logger.info("Going for election since Coordinator state is lost");
                return NodeState.ELECTION;
            }
        }

        /**
         * Perform new coordinator election task
         *
         * @return next NodeState
         * @throws InterruptedException
         */
        private NodeState performElectionTask() throws InterruptedException {
            NodeState nextState;

            try {
                nextState = tryToElectSelfAsCoordinator();
            } catch (AndesException e) {
                logger.info("Current node became a " + NodeState.CANDIDATE + " node", e);
                nextState = NodeState.CANDIDATE;
            }
            return nextState;
        }

        /**
         * Try to elect local node as the coordinator by creating the coordinator entry
         *
         * @return next NodeState
         * @throws AndesException
         * @throws InterruptedException
         */
        private NodeState tryToElectSelfAsCoordinator() throws AndesException, InterruptedException {
            NodeState nextState;
            boolean electedAsCoordinator = contextStore.createCoordinatorEntry(localNodeId, thriftAddress);
            if (electedAsCoordinator) {
                // backoff
                TimeUnit.MILLISECONDS.sleep(coordinatorEntryCreationWaitTime);
                boolean isCoordinator = contextStore.checkIsCoordinator(localNodeId);

                if (isCoordinator) {
                    contextStore.updateCoordinatorHeartbeat(localNodeId);
                    logger.info("Elected current node as the coordinator");

                    configurableClusterAgent.becameCoordinator();
                    nextState = NodeState.COORDINATOR;

                    // notify nodes about coordinator change
                    membershipEventingEngine.notifyMembershipEvent(getAllNodeIdentifiers(),
                            MembershipEventType.COORDINATOR_CHANGED, localNodeId);
                } else {
                    logger.info("Election resulted in current node becoming a " + NodeState.CANDIDATE + " node");
                    nextState = NodeState.CANDIDATE;
                }

            } else {
                logger.info("Election resulted in current node becoming a " + NodeState.CANDIDATE + " node");
                nextState = NodeState.CANDIDATE;
            }

            return nextState;
        }

        /**
         * Stop coordination task
         */
        public void stop() {
            running = false;
        }
    }
}
