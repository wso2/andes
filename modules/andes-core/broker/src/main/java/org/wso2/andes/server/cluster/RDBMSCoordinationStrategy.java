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
import org.wso2.andes.configuration.BrokerConfigurationService;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
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
     * Thread executor used for expiring the coordinator state
     */
    private final ScheduledExecutorService scheduledExecutorService;
    /**
     * Default hearbeat interval value
     */
   private static final  int DEFAULT_HEARTBEAT_INTERVAL = 5;
    /**
     * Default rdbms coordination entry creation wait time value
     */
    private static final  int DEFAULT_ENTRY_CREATION_WAIT_TIME = 3;
    /**
     * Default constructor
     */
    RDBMSCoordinationStrategy() {
        threadExecutor = Executors.newSingleThreadExecutor(createThreadFactory("RDBMSCoordinationStrategy-%d"));

        scheduledExecutorService = Executors.newScheduledThreadPool(2,
                                                                    createThreadFactory("RDBMSCoordinationScheduledTask-%d"));

        heartBeatInterval = BrokerConfigurationService.getInstance().getBrokerConfiguration().getCoordination()
                .getRdbmsBasedCoordination().getHeartbeatInterval();
        coordinatorEntryCreationWaitTime = BrokerConfigurationService.getInstance().getBrokerConfiguration()
                .getCoordination().getRdbmsBasedCoordination().getCoordinatorEntryCreationWaitTime();
                //(heartBeatInterval / 2) + 1;

        // Maximum age of a heartbeat. After this much of time, the heartbeat is considered invalid and node is
        // considered to have left the cluster.
        heartbeatMaxAge = heartBeatInterval * 2;

        if (heartBeatInterval <= coordinatorEntryCreationWaitTime) {
            throw new RuntimeException(
                    "Configuration error. " + DEFAULT_HEARTBEAT_INTERVAL + " * 2 should be greater than "
                            + DEFAULT_ENTRY_CREATION_WAIT_TIME);
        }
    }

    /**
     * Create a thread factory with the given name.
     *
     * @param nameFormat thread name format
     * @return thread factory with the given name format
     */
    private ThreadFactory createThreadFactory(String nameFormat) {
        return new ThreadFactoryBuilder()
                .setNameFormat(nameFormat)
                .build();
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

    /**
     * Notify coordinator lost event asynchronously
     */
    private void notifyCoordinatorStateLostEvent() {
        scheduledExecutorService.submit(() -> {
            try {
                configurableClusterAgent.coordinatorStateLost();
            } catch (Throwable e) {
                logger.error("Error while notifying coordinator state lost", e);
            }
        });
    }

    /*
    * ======================== Methods from CoordinationStrategy ============================
    */

    /**
     * {@inheritDoc}
     */
    @Override
    public void start(CoordinationConfigurableClusterAgent configurableClusterAgent, String nodeId,
            InetSocketAddress thriftAddress) {
        localNodeId = nodeId;
        this.thriftAddress = thriftAddress;

        // TODO detect if already started
        contextStore = AndesContext.getInstance().getAndesContextStore();
        this.configurableClusterAgent = configurableClusterAgent;

        try {
            contextStore.createNodeHeartbeatEntry(localNodeId);
        } catch (AndesException e) {
            throw new RuntimeException("Error while registering node ID", e);
        }

        membershipEventingEngine = new RDBMSMembershipEventingEngine();
        membershipEventingEngine.start(nodeId);

        // Register listener for membership changes
        membershipEventingEngine.addEventListener(this);

        currentNodeState = NodeState.ELECTION;

        // Clear old membership events for current node.
        try {
            contextStore.clearMembershipEvents(nodeId);
            contextStore.removeNodeHeartbeat(nodeId);
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
        scheduledExecutorService.shutdown();
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

        /**
         * Scheduled future for the COORDINATOR state expiration task
         */
        private ScheduledFuture<?> scheduledFuture;

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
                        if (currentNodeState != NodeState.COORDINATOR) {
                            notifyCoordinatorStateLostEvent();
                        }
                        break;
                    case ELECTION:
                        currentNodeState = performElectionTask();
                        break;
                    }

                } catch (Throwable e) {
                    logger.error("Error detected while running coordination algorithm. Node became a "
                            + NodeState.CANDIDATE + " node", e);
                    cancelStateExpirationTask();
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
                contextStore.createNodeHeartbeatEntry(localNodeId);
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
                resetScheduleStateExpirationTask();
                long startTime = System.currentTimeMillis();
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

                // Reduce the time spent in updating membership events from wait time
                long endTime = System.currentTimeMillis();
                long timeToWait = heartBeatInterval - (endTime - startTime);

                if (timeToWait > 0) {
                    TimeUnit.MILLISECONDS.sleep(timeToWait);
                } else {
                    logger.warn("Sending membership events took more than the heart beat interval");
                }

                return NodeState.COORDINATOR;
            } else {
                logger.info("Going for election since Coordinator state is lost");
                cancelStateExpirationTask();
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
                    resetScheduleStateExpirationTask();

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

        /**
         * Cancel the coordinator expiration task if one exists.
         */
        private void cancelStateExpirationTask() {
            if (scheduledFuture != null) {
                scheduledFuture.cancel(true);
                scheduledFuture = null;
            }
        }

        /**
         * Reset the coordinator expiration task. This method is called when the COORDINATOR state is reassigned
         */
        private void resetScheduleStateExpirationTask() {
            cancelStateExpirationTask();
            scheduledFuture = scheduledExecutorService.schedule(() -> {
                currentNodeState = NodeState.ELECTION;
                notifyCoordinatorStateLostEvent();
            }, heartbeatMaxAge, TimeUnit.MILLISECONDS);
        }
    }
}
