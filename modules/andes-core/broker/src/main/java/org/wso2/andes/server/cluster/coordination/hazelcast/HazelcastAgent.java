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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.andes.server.cluster.coordination.hazelcast;

import com.hazelcast.core.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.server.cluster.coordination.ClusterCoordinationHandler;
import org.wso2.andes.server.cluster.coordination.ClusterNotification;
import org.wso2.andes.server.cluster.coordination.CoordinationConstants;
import org.wso2.andes.server.slot.Slot;
import org.wso2.andes.server.cluster.coordination.hazelcast.custom.serializer.wrapper.HashmapStringListWrapper;
import org.wso2.andes.server.cluster.coordination.hazelcast.custom.serializer.wrapper.TreeSetLongWrapper;
import org.wso2.andes.server.cluster.coordination.hazelcast.custom.serializer.wrapper.TreeSetStringWrapper;


import java.util.*;

/**
 * This is a singleton class, which contains all Hazelcast related operations.
 */
public class HazelcastAgent {
    private static Log log = LogFactory.getLog(HazelcastAgent.class);

    /**
     * Value used to indicate the cluster initialization success state
     */
    private static final long INIT_SUCCESSFUL = 1L;

    /**
     * Singleton HazelcastAgent Instance.
     */
    private static HazelcastAgent hazelcastAgentInstance = new HazelcastAgent();

    /**
     * Hazelcast instance exposed by Carbon.
     */
    private HazelcastInstance hazelcastInstance;

    /**
     * Distributed topic to communicate subscription change notifications among cluster nodes.
     */
    private ITopic<ClusterNotification> subscriptionChangedNotifierChannel;

    /**
     * Distributed topic to communicate binding change notifications among cluster nodes.
     */
    private ITopic<ClusterNotification> bindingChangeNotifierChannel;

    /**
     * Distributed topic to communicate queue purge notifications among cluster nodes.
     */
    private ITopic<ClusterNotification> queueChangedNotifierChannel;


    /**
     * Distributed topic to communicate exchange change notification among cluster nodes.
     */
    private ITopic<ClusterNotification> exchangeChangeNotifierChannel;

    /**
     * These distributed maps are used for slot management
     */


    /**
     * distributed Map to store message ID list against queue name
     */
    private IMap<String, TreeSetLongWrapper> slotIdMap;

    /**
     * to keep track of assigned slots up to now. Key of the map contains nodeID+"_"+queueName
     */
    private IMap<String, HashmapStringListWrapper> slotAssignmentMap;

    /**
     *distributed Map to store last assigned ID against queue name
     */
    private IMap<String, Long> lastAssignedIDMap;

    /**
     * Distributed Map to keep track of non-empty slots which are unassigned from
     * other nodes
     */
    private IMap<String, TreeSetStringWrapper> unAssignedSlotMap;

    /**
     * This map is used to store thrift server host and thrift server port
     * map's key is port or host name.
     */
    private IMap<String,String> thriftServerDetailsMap;
    /**
     * Unique ID generated to represent the node.
     * This ID is used when generating message IDs.
     */

    private int uniqueIdOfLocalMember;

    /**
     * Lock used to initialize the Slot map used by the Slot manager.
     */
    private ILock initializationLock;

    /**
     * This is used to indicate if the cluster initialization was done properly. Used a atomic long
     * since am atomic boolean is not available in the current Hazelcast implementation.
     */
    private IAtomicLong initializationDoneIndicator;

    /**
     * Private constructor.
     */
    private HazelcastAgent() {

    }

    /**
     * Get singleton HazelcastAgent.
     *
     * @return HazelcastAgent
     */
    public static synchronized HazelcastAgent getInstance() {
        return hazelcastAgentInstance;
    }

    /**
     * Initialize HazelcastAgent instance.
     *
     * @param hazelcastInstance obtained hazelcastInstance from the OSGI service
     */
    @SuppressWarnings("unchecked")
    public void init(HazelcastInstance hazelcastInstance) {
        log.info("Initializing Hazelcast Agent");
        this.hazelcastInstance = hazelcastInstance;

        /**
         * membership changes
         */
        this.hazelcastInstance.getCluster().addMembershipListener(new AndesMembershipListener());

        /**
         * subscription changes
         */
        this.subscriptionChangedNotifierChannel = this.hazelcastInstance.getTopic(
                CoordinationConstants.HAZELCAST_SUBSCRIPTION_CHANGED_NOTIFIER_TOPIC_NAME);
        ClusterSubscriptionChangedListener clusterSubscriptionChangedListener = new ClusterSubscriptionChangedListener();
        clusterSubscriptionChangedListener.addSubscriptionListener(new ClusterCoordinationHandler(this));
        this.subscriptionChangedNotifierChannel.addMessageListener(clusterSubscriptionChangedListener);


        /**
         * exchange changes
         */
        this.exchangeChangeNotifierChannel = this.hazelcastInstance.getTopic(
                CoordinationConstants.HAZELCAST_EXCHANGE_CHANGED_NOTIFIER_TOPIC_NAME);
        ClusterExchangeChangedListener clusterExchangeChangedListener = new ClusterExchangeChangedListener();
        clusterExchangeChangedListener.addExchangeListener(new ClusterCoordinationHandler(this));
        this.exchangeChangeNotifierChannel.addMessageListener(clusterExchangeChangedListener);


        /**
         * queue changes
         */
        this.queueChangedNotifierChannel = this.hazelcastInstance.getTopic(
                CoordinationConstants.HAZELCAST_QUEUE_CHANGED_NOTIFIER_TOPIC_NAME);
        ClusterQueueChangedListener clusterQueueChangedListener = new ClusterQueueChangedListener();
        clusterQueueChangedListener.addQueueListener(new ClusterCoordinationHandler(this));
        this.queueChangedNotifierChannel.addMessageListener(clusterQueueChangedListener);

        /**
         * binding changes
         */
        this.bindingChangeNotifierChannel = this.hazelcastInstance.getTopic(
                CoordinationConstants.HAZELCAST_BINDING_CHANGED_NOTIFIER_TOPIC_NAME);
        ClusterBindingChangedListener clusterBindingChangedListener = new ClusterBindingChangedListener();
        clusterBindingChangedListener.addBindingListener(new ClusterCoordinationHandler(this));
        this.bindingChangeNotifierChannel.addMessageListener(clusterBindingChangedListener);


        // generates a unique id for the node unique for the cluster
        IdGenerator idGenerator = hazelcastInstance.getIdGenerator(CoordinationConstants.HAZELCAST_ID_GENERATOR_NAME);
        this.uniqueIdOfLocalMember = (int) idGenerator.newId();

        /**
         * Initialize hazelcast maps for slots
         */
        unAssignedSlotMap = hazelcastInstance.getMap(CoordinationConstants.UNASSIGNED_SLOT_MAP_NAME);
        slotIdMap = hazelcastInstance.getMap(CoordinationConstants.SLOT_ID_MAP_NAME);
        lastAssignedIDMap = hazelcastInstance.getMap(CoordinationConstants.LAST_ASSIGNED_ID_MAP_NAME);
        slotAssignmentMap = hazelcastInstance.getMap(CoordinationConstants.SLOT_ASSIGNMENT_MAP_NAME);

        /**
         * Initialize hazelcast map fot thrift server details
         */
        thriftServerDetailsMap = hazelcastInstance.getMap(CoordinationConstants.THRIFT_SERVER_DETAILS_MAP_NAME);

        /**
         * Initialize distributed lock and boolean related to slot map initialization
         */
        initializationLock = hazelcastInstance.getLock(CoordinationConstants.INITIALIZATION_LOCK);
        initializationDoneIndicator = hazelcastInstance
                .getAtomicLong(CoordinationConstants.INITIALIZATION_DONE_INDICATOR);

        log.info("Successfully initialized Hazelcast Agent");

        if (log.isDebugEnabled()) {
            log.debug("Unique ID generation for message ID generation:" + uniqueIdOfLocalMember);
        }
    }

    /**
     * Node ID is generated in the format of "NODE/<host IP>:<Port>"
     * @return NodeId Identifier of the node in the cluster
     */
    public String getNodeId() {

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
     * All members of the cluster are returned as a Set of Members
     *
     * @return Set of Members
     */
    public Set<Member> getAllClusterMembers() {
        return hazelcastInstance.getCluster().getMembers();
    }

    /**
     * Get node IDs of all nodes available in the cluster.
     *
     * @return List of node IDs.
     */
    public List<String> getMembersNodeIDs() {
        Set<Member> members = this.getAllClusterMembers();
        List<String> nodeIDList = new ArrayList<String>();
        for (Member member : members) {
            nodeIDList.add(CoordinationConstants.NODE_NAME_PREFIX +
                    member.getSocketAddress());
        }

        return nodeIDList;
    }

    /**
     * Get local node.
     *
     * @return local node as a Member.
     */
    public Member getLocalMember() {
        return hazelcastInstance.getCluster().getLocalMember();
    }

    /**
     * Get number of members in the cluster.
     *
     * @return number of members.
     */
    public int getClusterSize() {
        return hazelcastInstance.getCluster().getMembers().size();
    }

    /**
     * Get unique ID to represent local member.
     *
     * @return unique ID.
     */
    public int getUniqueIdForNode() {
        return uniqueIdOfLocalMember;
    }

    /**
     * Get node ID of the given node.
     *
     * @param node cluster node to get the ID
     * @return node ID.
     */
    public String getIdOfNode(Member node) {
        return CoordinationConstants.NODE_NAME_PREFIX +
                node.getSocketAddress();
    }

    /**
     * Each member of the cluster is given an unique UUID and here the UUIDs of all nodes are sorted
     * and the index of the belonging UUID of the given node is returned.
     *
     * @param node node to get the index
     * @return the index of the specified node
     */
    public int getIndexOfNode(Member node) {
        TreeSet<String> membersUniqueRepresentations = new TreeSet<String>();
        for (Member member : this.getAllClusterMembers()) {
            membersUniqueRepresentations.add(member.getUuid());
        }

        return membersUniqueRepresentations.headSet(node.getUuid()).size();
    }

    /**
     * Get the index where the local node is placed when all
     * the cluster nodes are sorted according to their UUID.
     *
     * @return the index of the local node
     */
    public int getIndexOfLocalNode() {
        return this.getIndexOfNode(this.getLocalMember());
    }


    public void notifySubscriptionsChanged(ClusterNotification clusterNotification) {
        log.debug("Sending GOSSIP: " + clusterNotification.getDescription());
        this.subscriptionChangedNotifierChannel.publish(clusterNotification);
    }


    public void notifyQueuesChanged(ClusterNotification clusterNotification) throws AndesException {
        log.debug("Sending GOSSIP: " + clusterNotification.getDescription());
        try {
            this.queueChangedNotifierChannel.publish(clusterNotification);
        } catch (Exception e) {
            log.error("Error while sending queue change notification : " + clusterNotification.getEncodedObjectAsString(), e);
            throw new AndesException("Error while sending queue change notification : " + clusterNotification.getEncodedObjectAsString(), e);
        }
    }

    public void notifyExchangesChanged(ClusterNotification clusterNotification) throws AndesException {
        log.debug("Sending GOSSIP: " + clusterNotification.getDescription());
        try {
            this.exchangeChangeNotifierChannel.publish(clusterNotification);
        } catch (Exception e) {
            log.error("Error while sending exchange change notification" + clusterNotification.getEncodedObjectAsString(), e);
            throw new AndesException("Error while sending exchange change notification" + clusterNotification.getEncodedObjectAsString(), e);
        }
    }

    public void notifyBindingsChanged(ClusterNotification clusterNotification) throws AndesException {
        log.debug("GOSSIP: " + clusterNotification.getDescription());
        try {
            this.bindingChangeNotifierChannel.publish(clusterNotification);
        } catch (Exception e) {
            log.error("Error while sending binding change notification" + clusterNotification.getEncodedObjectAsString(), e);
            throw new AndesException("Error while sending binding change notification" + clusterNotification.getEncodedObjectAsString(), e);
        }
    }

    public IMap<String, TreeSetStringWrapper> getUnAssignedSlotMap() {
        return unAssignedSlotMap;
    }

    public IMap<String, TreeSetLongWrapper> getSlotIdMap() {
        return slotIdMap;
    }

    public IMap<String, Long> getLastAssignedIDMap() {
        return lastAssignedIDMap;
    }

    public IMap<String, HashmapStringListWrapper> getSlotAssignmentMap() {
        return slotAssignmentMap;
    }

    /**
     * This method returns a map containing thrift server port and hostname
     * @return thriftServerDetailsMap
     */
    public IMap<String, String> getThriftServerDetailsMap() {
        return thriftServerDetailsMap;
    }

    /**
     * Acquire the distributed lock related to cluster initialization. This lock is required to
     * avoid two nodes initializing the map twice.
     */
    public void acquireInitializationLock() {
        if (log.isDebugEnabled()) {
            log.debug("Trying to acquire initialization lock.");
        }

        initializationLock.lock();

        if (log.isDebugEnabled()) {
            log.debug("Initialization lock acquired.");
        }
    }

    /**
     * Inform other members in the cluster that the cluster was initialized properly.
     */
    public void indicateSuccessfulInitilization() {
        initializationDoneIndicator.set(INIT_SUCCESSFUL);
    }

    /**
     * Check if a member has already initialized the cluster
     *
     * @return true if cluster is already initialized
     */
    public boolean isClusterInitializedSuccessfully() {
        return initializationDoneIndicator.get() == INIT_SUCCESSFUL;
    }

    /**
     * Release the initialization lock.
     */
    public void releaseInitializationLock() {
        initializationLock.unlock();

        if (log.isDebugEnabled()) {
            log.debug("Initialization lock released.");
        }
    }

}
