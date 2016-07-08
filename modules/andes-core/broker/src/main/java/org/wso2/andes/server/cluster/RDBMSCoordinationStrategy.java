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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * HybridRDBMSCoordinationStrategy used a RDBMS based approached to identify membership event related to cluster.
 * This include electing coordinator and notifying member added and left events.
 */
class RDBMSCoordinationStrategy implements CoordinationStrategy, MembershipListener {
    /**
     * Class logger
     */
    private Log logger = LogFactory.getLog(RDBMSCoordinationStrategy.class);

    /**
     * Possible node states
     */
    private enum NodeState {
        COORDINATOR, STANDBY, ELECTION
    }

    /**
     * Heartbeat interval in seconds
     * TODO this should be read from the configuration
     */
    private static final int HEARTBEAT_INTERVAL = 5;

    /**
     * After this much of time the node is assumed to have left the cluster
     */
    private static final int HEARTBEAT_EXPIRY_INTERVAL = HEARTBEAT_INTERVAL * 2;

    /**
     * Hazelcast instance used to receive hazelcast membership information
     */
    private final HazelcastInstance hazelcastInstance;

    /**
     * Cluster agent used to notify about the coordinator change status
     */
    private HazelcastClusterAgent hazelcastClusterAgent;

    /**
     * Registration id for membership listener
     */
    private String listenerRegistrationId;

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

    RDBMSCoordinationStrategy(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;

        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("RDBMSCoordinationStrategy-%d")
                                                                     .build();
        threadExecutor = Executors.newSingleThreadExecutor(namedThreadFactory);
    }


    /*
    * ======================== Methods from MembershipListener ============================
    */

    /**
     * {@inheritDoc}
     */
    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
        hazelcastClusterAgent.memberAdded(membershipEvent.getMember());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        Member member = membershipEvent.getMember();
        try {
            hazelcastClusterAgent.memberRemoved(member);
        } catch (AndesException e) {
            logger.error("Error while handling node removal, " + member.getSocketAddress(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
        // Do nothing
    }

    /*
    * ======================== Methods from CoordinationStrategy ============================
    */

    /**
     * {@inheritDoc}
     */
    @Override
    public void start(HazelcastClusterAgent hazelcastClusterAgent, String nodeId) {
        localNodeId = nodeId;

        // TODO detect if already started
        contextStore = AndesContext.getInstance().getAndesContextStore();
        this.hazelcastClusterAgent = hazelcastClusterAgent;

        // Register listener for membership changes
        listenerRegistrationId = hazelcastInstance.getCluster().addMembershipListener(this);

        currentNodeState = NodeState.ELECTION;

        coordinatorElectionTask = new CoordinatorElectionTask();
        threadExecutor.execute(coordinatorElectionTask);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCoordinator() {
        return NodeState.COORDINATOR == currentNodeState;
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
                logger.error("Error occurred while removing coordination when shutting down");
            }
        }

        hazelcastInstance.getCluster().removeMembershipListener(listenerRegistrationId);
        coordinatorElectionTask.stop();
        threadExecutor.shutdown();
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
            try {

                while (running) {
                    switch (currentNodeState) {
                    case STANDBY:
                        currentNodeState = performStandByTask();
                        break;
                    case COORDINATOR:
                        currentNodeState = performCoordinatorTask();
                        break;
                    case ELECTION:
                        currentNodeState = performElectionTask();
                        break;
                    }
                }

            } catch (Throwable e) {
                logger.error("Error detected while running coordination algorithm. Node became a standby node", e);
                currentNodeState = NodeState.STANDBY;
            }
        }

        /**
         * Perform periodic task that should be done by a standby node
         *
         * @return Next NodeStatus
         * @throws AndesException
         * @throws InterruptedException
         */
        private NodeState performStandByTask() throws AndesException, InterruptedException {
            NodeState nextState;

            contextStore.updateNodeHeartbeat(localNodeId);

            // Read current coordinators validity. We can improve this by returning the status (TIMED_OUT or DELETED or
            // VALID)from this call. If DELETED we do not have to check a second time.
            boolean coordinatorValid = contextStore.checkIfCoordinatorValid(HEARTBEAT_EXPIRY_INTERVAL);
            TimeUnit.SECONDS.sleep(HEARTBEAT_INTERVAL);

            if (coordinatorValid) {
                nextState = NodeState.STANDBY;
            } else {
                coordinatorValid = contextStore.checkIfCoordinatorValid(HEARTBEAT_EXPIRY_INTERVAL);

                if (coordinatorValid) {
                    nextState = NodeState.STANDBY;
                } else {
                    logger.info("Going for election since no Coordinator is invalid");

                    contextStore.removeCoordinator();
                    nextState = NodeState.ELECTION;
                }
            }
            return nextState;
        }

        /**
         * Perform periodic task that should be done by a Coordinating node
         *
         * @return Next NodeState
         * @throws AndesException
         * @throws InterruptedException
         */
        private NodeState performCoordinatorTask() throws AndesException, InterruptedException {
            // Try to update the coordinator heartbeat
            boolean stillCoordinator = contextStore.updateCoordinatorHeartbeat(localNodeId);

            if (stillCoordinator) {
                List<NodeHeartBeatData> allNodeInformation = contextStore.getAllNodeInformation();

                for (NodeHeartBeatData nodeHeartBeatData : allNodeInformation) {
                    long heartbeatAge = TimeUnit.MILLISECONDS
                            .toSeconds(System.currentTimeMillis() - nodeHeartBeatData.getLastHeartbeat());

                    String nodeId = nodeHeartBeatData.getNodeId();
                    if (nodeHeartBeatData.isNewNode()) {
                        // TODO: notify nodes about member added event
                        logger.info("Member added " + nodeId);
                        // update node info to not new node
                        contextStore.markNodeAsNotNew(nodeId);
                    } else if (HEARTBEAT_EXPIRY_INTERVAL <= heartbeatAge) {
                        // TODO: notify nodes about member left event
                        logger.info("Member removed " + nodeId);
                        contextStore.removeNodeHeartbeat(nodeId);
                    }
                }

                TimeUnit.SECONDS.sleep(HEARTBEAT_INTERVAL);
                return NodeState.COORDINATOR;
            } else {
                logger.info("Going for election since Coordinator state is lost");
                return NodeState.ELECTION;
            }
        }

        /**
         * Perform new coordinator election task
         *
         * @return Next NodeState
         * @throws InterruptedException
         */
        private NodeState performElectionTask() throws InterruptedException {
            NodeState nextState;

            try {
                nextState = tryToElectSelfAsCoordinator();
            } catch (AndesException e) {
                logger.info("Current node became a standby node");
                nextState = NodeState.STANDBY;
            }
            return nextState;
        }

        /**
         * Try to elect local node as the coordinator by creating the coordinator entry
         *
         * @return Next NodeState
         * @throws AndesException
         * @throws InterruptedException
         */
        private NodeState tryToElectSelfAsCoordinator() throws AndesException, InterruptedException {
            NodeState nextState;
            boolean electedAsCoordinator = contextStore.createCoordinatorEntry(localNodeId);
            if (electedAsCoordinator) {
                // backoff
                TimeUnit.SECONDS.sleep((HEARTBEAT_INTERVAL / 2) + 1);
                boolean isCoordinator = contextStore.checkIsCoordinator(localNodeId);

                if (isCoordinator) {
                    contextStore.updateCoordinatorHeartbeat(localNodeId);
                    // TODO: notify nodes about coordinator change
                    logger.info("Elected current node as the coordinator");
                    hazelcastClusterAgent.becameCoordinator();
                    nextState = NodeState.COORDINATOR;
                } else {
                    logger.info("Election resulted in current node becoming a standby node");
                    nextState = NodeState.STANDBY;
                }

            } else {
                logger.info("Election resulted in current node becoming a standby node");
                nextState = NodeState.STANDBY;
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
