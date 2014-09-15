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
package org.wso2.andes.server.cluster.coordination.hazelcast;

import com.hazelcast.core.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.server.slot.Slot;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.MessagePurgeHandler;
import org.wso2.andes.server.cluster.coordination.ClusterCoordinationHandler;
import org.wso2.andes.server.cluster.coordination.ClusterNotification;
import org.wso2.andes.server.cluster.coordination.CoordinationConstants;

import java.util.*;

/**
 * This is a singleton class, which contains all Hazelcast related operations.
 */
public class HazelcastAgent {
    private static Log log = LogFactory.getLog(HazelcastAgent.class);

    /**
     * Singleton HazelcastAgent Instance.
     */
    private static volatile HazelcastAgent hazelcastAgentInstance = null;

    /**
     * Hazelcast instance exposed by Carbon.
     */
    private HazelcastInstance hazelcastInstance;

    /**
     * Distributed topic to communicate subscription change notifications among cluster nodes.
     */
    private ITopic subscriptionChangedNotifierChannel;

    /**
     * Distributed topic to communicate binding change notifications among cluster nodes.
     */
    private ITopic bindingChangeNotifierChannel;

    /**
     * Distributed topic to communicate queue purge notifications among cluster nodes.
     */
    private ITopic queueChangedNotifierChannel;


    private IMap<String, TreeSet<Long>> queueToMessageIdListMap;
    private IMap<String,HashMap<Long,Slot>> slotAssignmentMap;
    private IMap<String, Long> lastProcessedIDs;
    /**
     * Distributed topic to communicate exchange change notification amoung cluster nodes.
     */
    private ITopic exchangeChangeNotifierChannel;

    /**
     * Unique ID generated to represent the node.
     * This ID is used when generating message IDs.
     */
    private int uniqueIdOfLocalMember;

	private IMap<String, TreeSet<Slot>> freeSlotMap;

    /**
     * Get singleton HazelcastAgent.
     *
     * @return HazelcastAgent
     */
    public static HazelcastAgent getInstance() {
        if (hazelcastAgentInstance == null) {
            synchronized (HazelcastAgent.class) {
                if (hazelcastAgentInstance == null) {
                    hazelcastAgentInstance = new HazelcastAgent();
                }
            }
        }

        return hazelcastAgentInstance;
    }

    /**
     * Initialize HazelcastAgent instance.
     *
     * @param hazelcastInstance obtained hazelcastInstance from the OSGI service
     */
    @SuppressWarnings("unchecked")
    public void init(HazelcastInstance hazelcastInstance) {
        if (log.isInfoEnabled()) {
            log.info("Initializing Hazelcast Agent");
        }
        this.hazelcastInstance = hazelcastInstance;
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
        clusterQueueChangedListener.addQueueListener(new MessagePurgeHandler());
        this.queueChangedNotifierChannel.addMessageListener(clusterQueueChangedListener);


        /**
         * binding changes
         */
        this.bindingChangeNotifierChannel = this.hazelcastInstance.getTopic(
                CoordinationConstants.HAZELCAST_BINDING_CHANGED_NOTIFIER_TOPIC_NAME);
        ClusterBindingChangedListener clusterBindingChangedListener = new ClusterBindingChangedListener();
        clusterBindingChangedListener.addBindingListener(new ClusterCoordinationHandler(this));
        this.bindingChangeNotifierChannel.addMessageListener(clusterBindingChangedListener);


        IdGenerator idGenerator = hazelcastInstance.getIdGenerator(CoordinationConstants.HAZELCAST_ID_GENERATOR_NAME);
        this.uniqueIdOfLocalMember = (int) idGenerator.newId();
        freeSlotMap = hazelcastInstance.getMap(CoordinationConstants.FREE_SLOT_MAP_NAME);
        queueToMessageIdListMap = hazelcastInstance.getMap(CoordinationConstants.QUEUE_TO_MESSAGE_ID_LIST_MAP_NAME);
        lastProcessedIDs = hazelcastInstance.getMap(CoordinationConstants.LAST_PROCESSED_IDS_MAP_NAME);
        slotAssignmentMap = hazelcastInstance.getMap(CoordinationConstants.SLOT_ASSIGNMENT_MAP_NAME);
    }

    /**
     * Node ID is generated in the format of "NODE/<host IP>_<Node UUID>"
     *
     * @return NodeId of the local node
     */
    public String getNodeId() {
        Member localMember = hazelcastInstance.getCluster().getLocalMember();
        return CoordinationConstants.NODE_NAME_PREFIX
                + localMember.getInetSocketAddress().getAddress()
                + "_"
                + localMember.getUuid();
    }

    /**
     * All nodes of the cluster are returned as a Set of Members
     *
     * @return all nodes of the cluster
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
            nodeIDList.add(CoordinationConstants.NODE_NAME_PREFIX
                    + member.getInetSocketAddress().getAddress()
                    + "_"
                    + member.getUuid());
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
     * Get number of nodes in the cluster.
     *
     * @return number of nodes.
     */
    public int getClusterSize() {
        return hazelcastInstance.getCluster().getMembers().size();
    }

    /**
     * Get unique ID to represent local member.
     *
     * @return unique ID assigned for the local node.
     */
    public int getUniqueIdForTheNode() {
        return uniqueIdOfLocalMember;
    }

    /**
     * Get node ID of the given node.
     *
     * @param node node
     * @return node ID
     */
    public String getIdOfNode(Member node) {
        return CoordinationConstants.NODE_NAME_PREFIX
                + node.getInetSocketAddress().getAddress()
                + "_"
                + node.getUuid();
    }

    /**
     * Each member of the cluster is given an unique UUID and here the UUIDs of all nodes are sorted
     * and the index of the belonging UUID of the given node is returned.
     *
     * @param node node
     * @return the index given to the node according to its UUID
     */
    public int getIndexOfNode(Member node) {
        List<String> membersUniqueRepresentations = new ArrayList<String>();
        for (Member member : this.getAllClusterMembers()) {
            membersUniqueRepresentations.add(member.getUuid());
        }
        Collections.sort(membersUniqueRepresentations);
        return membersUniqueRepresentations.indexOf(node.getUuid());
    }

    /**
     * Get the index where the local node is placed when all
     * the cluster nodes are sorted according to their UUID.
     *
     * @return index given to the local node according to its UUID
     */
    public int getIndexOfLocalNode() {
        return this.getIndexOfNode(this.getLocalMember());
    }

    public void notifySubscriptionsChanged(ClusterNotification clusterNotification) {
        log.info("GOSSIP: " + clusterNotification.getDescription());
        this.subscriptionChangedNotifierChannel.publish(clusterNotification);
    }

    public void notifyQueuesChanged(ClusterNotification clusterNotification) throws AndesException {
        log.info("GOSSIP: " + clusterNotification.getDescription());
        try {
            this.queueChangedNotifierChannel.publish(clusterNotification);
        } catch (Exception e) {
            log.error("Error while sending queue change notification", e);
            throw new AndesException("Error while sending queue change notification", e);
        }

    }

    public void notifyExchangesChanged(ClusterNotification clusterNotification) throws AndesException {
        log.info("GOSSIP: " + clusterNotification.getDescription());
        try {
            this.exchangeChangeNotifierChannel.publish(clusterNotification);
        } catch (Exception e) {
            log.error("Error while sending exchange change notification", e);
            throw new AndesException("Error while sending exchange change notification", e);
        }
    }

    public void notifyBindingsChanged(ClusterNotification clusterNotification) throws AndesException {
        log.info("GOSSIP: " + clusterNotification.getDescription());
        try {
            this.bindingChangeNotifierChannel.publish(clusterNotification);
        } catch (Exception e) {
            log.error("Error while sending binding change notification", e);
            throw new AndesException("Error while sending binding change notification", e);
        }
    }

    public IMap<String,TreeSet<Slot>> getFreeSlotMap(){
        return freeSlotMap;
    }

    public IMap<String, TreeSet<Long>> getQueueToMessageIdListMap() {
        return queueToMessageIdListMap;
    }

    public IMap<String, Long> getLastProcessedIDs() {
        return lastProcessedIDs;
    }

    public IMap<String,HashMap<Long,Slot>> getSlotAssignmentMap() {
        return slotAssignmentMap;
    }
}
