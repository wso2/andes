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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.slot.SlotCoordinationConstants;
import org.wso2.andes.server.cluster.coordination.CoordinationConstants;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * In this strategy the oldest member is elected as the coordinator
 */
public class HazelcastCoordinationStrategy implements CoordinationStrategy, MembershipListener {

    private static final Logger log = Logger.getLogger(HazelcastCoordinationStrategy.class);
    /**
     * Used to query if current node is the oldest one
     */
    private final HazelcastInstance hazelcastInstance;

    /**
     * registration id for membership listener
     */
    private String listenerRegistrationId;

    /**
     * Used to communicate membership events;
     */
    private CoordinationConfigurableClusterAgent configurableClusterAgent;

    /**
     * Used to identify coordinator change event
     */
    private final AtomicBoolean isCoordinator;

    /**
     * Hold coordinator information
     */
    private IMap<String, String> coordinatorNodeDetailsMap;

    /**
     * This map is used to store thrift server host and thrift server port
     * map's key is port or host name.
     */
    private IMap<String,String> thriftServerDetailsMap;

    public HazelcastCoordinationStrategy(HazelcastInstance hazelcastInstance) {
        this.isCoordinator = new AtomicBoolean(false);
        this.hazelcastInstance = hazelcastInstance;
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
        thriftServerDetailsMap = hazelcastInstance.getMap(CoordinationConstants.THRIFT_SERVER_DETAILS_MAP_NAME);
        coordinatorNodeDetailsMap = hazelcastInstance.getMap(CoordinationConstants.COORDINATOR_NODE_DETAILS_MAP_NAME);

        // Register listener for membership changes
        listenerRegistrationId = hazelcastInstance.getCluster()
                                                  .addMembershipListener(this);

        this.configurableClusterAgent = configurableClusterAgent;
        checkAndNotifyCoordinatorChange();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCoordinator() {
        Member oldestMember = hazelcastInstance.getCluster().getMembers().iterator().next();

        return oldestMember.localMember();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InetSocketAddress getThriftAddressOfCoordinator() {
        String hostname = thriftServerDetailsMap.get(SlotCoordinationConstants.THRIFT_COORDINATOR_SERVER_IP);
        String portString = thriftServerDetailsMap.get(SlotCoordinationConstants.THRIFT_COORDINATOR_SERVER_PORT);

        InetSocketAddress coordinatorAddress = null;

        if ((null != hostname) && (null != portString)) {
            int port = Integer.parseInt(portString);
            coordinatorAddress = new InetSocketAddress(hostname, port);
        }

        return coordinatorAddress;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getAllNodeIdentifiers() throws AndesException {
        Set<Member> members = hazelcastInstance.getCluster().getMembers();
        List<String> nodeIDList = new ArrayList<>();
        for (Member member : members) {
            nodeIDList.add(configurableClusterAgent.getIdOfNode(member));
        }

        return nodeIDList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<NodeDetail> getAllNodeDetails() throws AndesException {
        List<NodeDetail> nodeDetails = new ArrayList<>();

        CoordinatorInformation coordinatorDetails = getCoordinatorDetails();
        InetSocketAddress coordinatorSocketAddress = new InetSocketAddress(coordinatorDetails.getHostname(),
                Integer.parseInt(coordinatorDetails.getPort()));

        for (Member member : hazelcastInstance.getCluster().getMembers()) {
            InetSocketAddress nodeSocketAddress = member.getSocketAddress();
            String nodeId = configurableClusterAgent.getIdOfNode(member);
            boolean isCoordinator = nodeSocketAddress.equals(coordinatorSocketAddress);

            nodeDetails.add(new NodeDetail(nodeId, nodeSocketAddress, isCoordinator));
        }

        return nodeDetails;
    }

    /**
     * Return current coordinator hostname and port
     *
     * @return coordinator details
     */
    private CoordinatorInformation getCoordinatorDetails() {
        String ipAddress = coordinatorNodeDetailsMap.get(
                SlotCoordinationConstants.CLUSTER_COORDINATOR_SERVER_IP);
        String port = coordinatorNodeDetailsMap.get(SlotCoordinationConstants.CLUSTER_COORDINATOR_SERVER_PORT);

        return new CoordinatorInformation(ipAddress, port);
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        hazelcastInstance.getCluster().removeMembershipListener(listenerRegistrationId);
    }

    /*
    * ======================== Methods from MembershipListener ============================
    */

    /**
     * {@inheritDoc}
     */
    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
        Member member = membershipEvent.getMember();
        log.info("Handling cluster gossip: New member joined to the cluster. Member Socket Address:"
                + member.getSocketAddress() + " UUID:" + member.getUuid());

        checkAndNotifyCoordinatorChange();

        int maximumNumOfTries = 3;
        String nodeId;
        int numberOfAttemptsTried = 0;
        /*
         * Try a few times until nodeId is read from distributed Hazelcast Map
         * and give up
         */
        nodeId = configurableClusterAgent.getIdOfNode(member);
        if (null == nodeId) {
            while (numberOfAttemptsTried < maximumNumOfTries) {
                try {
                    // Exponentially increase waiting time
                    long sleepTime = Math.round(Math.pow(2, (numberOfAttemptsTried)));
                    TimeUnit.SECONDS.sleep(sleepTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                nodeId = configurableClusterAgent.getIdOfNode(member);
                numberOfAttemptsTried = numberOfAttemptsTried + 1;
                if (!(StringUtils.isEmpty(nodeId))) {
                    break;
                }
            }
        }
        if (StringUtils.isEmpty(nodeId)) {
            log.warn("Node ID is not set for member " + member + " when newly joined");
        }
        configurableClusterAgent.memberAdded(nodeId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        Member member = membershipEvent.getMember();
        log.info("Handling cluster gossip: A member left the cluster. Member Socket Address:"
                + member.getSocketAddress() + " UUID:" + member.getUuid());

        try {
            checkAndNotifyCoordinatorChange();
            configurableClusterAgent.memberRemoved(configurableClusterAgent.getIdOfNode(member));
        } catch (AndesException e) {
            log.error("Error while handling node removal, " + member.getSocketAddress(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
        // do nothing here, since member attributes are not used in the implementation
    }

    /**
     * Check if the current node is the new coordinator and notify cluster agent.
     */
    private void checkAndNotifyCoordinatorChange() {
        if (isCoordinator() && isCoordinator.compareAndSet(false, true)) {
            updateThriftCoordinatorDetailsToMap();
            updateCoordinatorNodeDetailMap();
            configurableClusterAgent.becameCoordinator();
        } else {
            isCoordinator.set(false);
        }
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
     * Set coordinator's thrift server IP and port in hazelcast map.
     */
    private void updateThriftCoordinatorDetailsToMap() {

        String thriftCoordinatorServerIP = AndesContext.getInstance().getThriftServerHost();
        int thriftCoordinatorServerPort = AndesContext.getInstance().getThriftServerPort();


        log.info("This node is elected as the Slot Coordinator. Registering " + thriftCoordinatorServerIP + ":"
                + thriftCoordinatorServerPort);
        thriftServerDetailsMap.put(SlotCoordinationConstants.THRIFT_COORDINATOR_SERVER_IP, thriftCoordinatorServerIP);
        thriftServerDetailsMap.put(SlotCoordinationConstants.THRIFT_COORDINATOR_SERVER_PORT,
                Integer.toString(thriftCoordinatorServerPort));
    }
}
