/*
 *
 *  * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *  *
 *  * WSO2 Inc. licenses this file to you under the Apache License,
 *  * Version 2.0 (the "License"); you may not use this file except
 *  * in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.wso2.andes.kernel;

import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.kernel.router.AndesMessageRouter;
import org.wso2.andes.kernel.slot.Slot;
import org.wso2.andes.kernel.slot.SlotState;
import org.wso2.andes.kernel.subscription.AndesSubscription;
import org.wso2.andes.kernel.subscription.StorageQueue;
import org.wso2.andes.server.cluster.NodeHeartBeatData;
import org.wso2.andes.server.cluster.coordination.rdbms.MembershipEvent;
import org.wso2.andes.server.cluster.coordination.ClusterNotification;
import org.wso2.andes.store.HealthAwareStore;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * AndesContextStore is an abstraction of underlying data base to store information related to
 * exchanges, queues, bindings, durable subscriptions and queue message counts.
 */
public interface AndesContextStore extends HealthAwareStore {

    /**
     * Initialize the storage and makes a connection to the database.
     *
     * @param connectionProperties ConfigurationProperties
     * @return returns the created DurableStoreConnection object created
     * @throws AndesException
     */
    DurableStoreConnection init(ConfigurationProperties connectionProperties) throws AndesException;

    /**
     * Get all durable encoded subscriptions as strings.
     *
     * @return list of <id,subscriptions>
     */
    Map<String, List<String>> getAllStoredDurableSubscriptions() throws AndesException;

    /**
     * Get all stored durable subscriptions as a map with the subscription Ids as the the key
     */
    Map<String, String> getAllDurableSubscriptionsByID() throws AndesException;

    /**
     * Check if a subscription present in database
     *
     * @return True if subscription present, false otherwise
     * @throws AndesException
     */
    boolean isSubscriptionExist(String subscriptionId) throws AndesException;

    /**
     * Store subscription to the durable store.
     *
     * @param subscription The subscription to store
     * @throws AndesException
     */
    void storeDurableSubscription(AndesSubscription subscription)
		    throws AndesException;

    /**
     * Update already existing subscription.
     *
     * @param subscription The subscriptoin object to update
     * @throws AndesException
     * @return If a subscription was updated return 1, else return 0
     */
    int updateDurableSubscription(AndesSubscription subscription) throws AndesException;

    /**
     * Update already the store if there is a matching subscription. Else, insert the subscription.
     *
     * @param subscription The subscriptoin object to update
     * @throws AndesException
     */
    void updateOrInsertDurableSubscription(AndesSubscription subscription) throws AndesException;

    /**
     * Updates a List of subscriptions with a given subscriptionId and given subscription information.
     *
     * @param subscriptions a map which contains the subscription ids and the modified subscription details
     * @throws AndesException
     */
    void updateDurableSubscriptions(Map<String, String> subscriptions) throws AndesException;

    /**
     * Remove stored subscription from durable store.
     *
     * @param subscription The subscription to remove
     */
    void removeDurableSubscription(AndesSubscription subscription) throws AndesException;

    /**
     * Store details of node.
     *
     * @param nodeID id of the node
     * @param data   detail to store
     */
    void storeNodeDetails(String nodeID, String data) throws AndesException;

    /**
     * Get all node information stored.
     *
     * @return map of node information
     */
    Map<String, String> getAllStoredNodeData() throws AndesException;

    /**
     * Remove stored node information.
     *
     * @param nodeID id of the node
     */
    void removeNodeData(String nodeID) throws AndesException;

    /**
     * Add message counting entry for queue. queue count is initialised to zero. The counter for
     * created queue can then be incremented and decremented.
     *
     * @param destinationQueueName name of queue
     * @see this.removeMessageCounterForQueue this.incrementMessageCountForQueue,
     * this.decrementMessageCountForQueue
     */
    void addMessageCounterForQueue(String destinationQueueName) throws AndesException;

    /**
     * Get message count of queue.
     *
     * @param destinationQueueName name of queue
     * @return message count
     */
    long getMessageCountForQueue(String destinationQueueName) throws AndesException;

    /**
     * Store level method to reset the message counter of a given queue to 0.
     *
     * @param storageQueueName name of the queue being purged
     * @throws AndesException
     */
    void resetMessageCounterForQueue(String storageQueueName) throws AndesException;

    /**
     * Remove Message counting entry.
     *
     * @param destinationQueueName name of queue
     */
    void removeMessageCounterForQueue(String destinationQueueName) throws AndesException;

    /**
     * Increment message counter for a queue by a given incrementBy value.
     *
     * @param destinationQueueName name of the queue
     * @param incrementBy          increment counter by
     * @throws AndesException
     */
    void incrementMessageCountForQueue(String destinationQueueName, long incrementBy) throws AndesException;

    /**
     * Decrement message counter for a queue.
     *
     * @param destinationQueueName name of the queue
     * @param decrementBy          decrement counter by
     * @throws AndesException
     */
    void decrementMessageCountForQueue(String destinationQueueName, long decrementBy) throws AndesException;

    /**
     * Store exchange information (amqp).
     *
     * @param exchangeName name of string
     * @param exchangeInfo info for exchange
     */
    void storeExchangeInformation(String exchangeName, String exchangeInfo) throws AndesException;

    /**
     * Get all exchanges stored.
     *
     * @return list of exchanges
     */
    List<AndesMessageRouter> getAllMessageRoutersStored() throws AndesException;

    /**
     * Delete all exchange information.
     *
     * @param exchangeName name of exchange
     */
    void deleteExchangeInformation(String exchangeName) throws AndesException;

    /**
     * Store a queue.
     *
     * @param queueName name of the queue to be stored
     * @param queueInfo string encoded queue information
     * @throws AndesException
     */
    void storeQueueInformation(String queueName, String queueInfo) throws AndesException;

    /**
     * Get all stored queues.
     *
     * @return list of queues
     * @throws AndesException
     */
    List<StorageQueue> getAllQueuesStored() throws AndesException;

    /**
     * Delete a queue from store.
     *
     * @param queueName name of the queue to be removed
     * @throws AndesException
     */
    void deleteQueueInformation(String queueName) throws AndesException;

    /**
     * Store a binding. Bound exchange and bound queue name together will be unique.
     *
     * @param exchange       name of the exchange binding represent
     * @param boundQueueName target queue binding is done
     * @param bindingInfo    binding information as a string
     * @throws AndesException
     */
    void storeBindingInformation(String exchange, String boundQueueName, String bindingInfo) throws AndesException;

    /**
     * Get bindings stored for some exchange.
     *
     * @return a list of bindings belonging to the exchange
     * @throws AndesException
     */
    List<AndesBinding> getBindingsStoredForExchange(String exchangeName) throws AndesException;

    /**
     * Remove a binding from the store.
     *
     * @param exchangeName   name of the exchange
     * @param boundQueueName name of the queue binding relates to
     * @throws AndesException
     */
    void deleteBindingInformation(String exchangeName, String boundQueueName) throws AndesException;

    /**
     * Create a new slot in store.
     *
     * @param startMessageId   start message id of slot
     * @param endMessageId     end message id of slot
     * @param storageQueueName name of storage queue name
     * @param assignedNodeId   id of assigned node
     * @throws AndesException
     */
    void createSlot(long startMessageId, long endMessageId, String storageQueueName, String assignedNodeId)
            throws AndesException;

    /**
     * Delete a slot from store.
     *
     * @param startMessageId start message id of slot
     * @param endMessageId   end message id of slot
     * @return True if slot deletion successful
     * @throws AndesException
     */
    boolean deleteSlot(long startMessageId, long endMessageId) throws AndesException;

    /**
     * Delete all slots by queue name.
     *
     * @param queueName name of queue
     * @throws AndesException
     */
    void deleteSlotsByQueueName(String queueName) throws AndesException;

    /**
     * Delete message ids by queue name.
     *
     * @param queueName name of queue
     * @throws AndesException
     */
    void deleteMessageIdsByQueueName(String queueName) throws AndesException;

    /**
     * Unassign and return slot.
     *
     * @param startMessageId start message id of slot
     * @param endMessageId   end message id of slot
     * @throws AndesException
     */
    void deleteSlotAssignment(long startMessageId, long endMessageId) throws AndesException;

    /**
     * Unassign slots by queue name.
     *
     * @param nodeId    id of node
     * @param queueName name of queue
     * @throws AndesException
     */
    void deleteSlotAssignmentByQueueName(String nodeId, String queueName) throws AndesException;

    /**
     * Update assignment information in slot store.
     *
     * @param nodeId     id of node
     * @param queueName  name of queue
     * @param startMsgId start message id of slot
     * @param endMsgId   end message id of slot
     * @throws AndesException
     */
    void createSlotAssignment(String nodeId, String queueName, long startMsgId, long endMsgId) throws AndesException;

    /**
     * Select unassigned slots for a given queue name.
     *
     * @param queueName name of queue
     * @return unassigned slot object if found
     * @throws AndesException
     */
    Slot selectUnAssignedSlot(String queueName) throws AndesException;

    /**
     * Get last assigned id for a queue.
     *
     * @param queueName name of queue
     * @return last assigned id of queue
     * @throws AndesException
     */
    long getQueueToLastAssignedId(String queueName) throws AndesException;

    /**
     * Set last assigned id for a given queue.
     *
     * @param queueName name of queue
     * @param messageId id of message
     * @throws AndesException
     */
    void setQueueToLastAssignedId(String queueName, long messageId) throws AndesException;

    /**
     * Get local safe zone for a given node.
     *
     * @param nodeId id of node
     * @return local safe zone of node
     * @throws AndesException
     */
    long getLocalSafeZoneOfNode(String nodeId) throws AndesException;

    /**
     * Set local safe zone for a given node.
     *
     * @param nodeId    id of node
     * @param messageId id of message
     * @throws AndesException
     */
    void setLocalSafeZoneOfNode(String nodeId, long messageId) throws AndesException;

    /**
     * Remove entries for a given publishing node ID.
     *
     * @param nodeId id of the leaving node
     */
    void removePublisherNodeId(String nodeId) throws AndesException;

    /**
     * Get all message published nodes.
     *
     * @return set of published nodes
     * @throws AndesException
     */
    TreeSet<String> getMessagePublishedNodes() throws AndesException;

    /**
     * Set slots states.
     *
     * @param startMessageId start message id of slot
     * @param endMessageId   end message id of slot
     * @param slotState      state of slot
     * @throws AndesException
     */
    void setSlotState(long startMessageId, long endMessageId, SlotState slotState) throws AndesException;

    /**
     * Get overlapped slots for a given queue.
     *
     * @param nodeId    node identifier
     * @param queueName name of queue
     * @return overlapped slot object
     * @throws AndesException
     */
    Slot getOverlappedSlot(String nodeId, String queueName) throws AndesException;

    /**
     * Add message ids to store.
     *
     * @param queueName name of queue
     * @param messageId id of message
     * @throws AndesException
     */
    void addMessageId(String queueName, long messageId) throws AndesException;

    /**
     * Get message ids for a given queue.
     *
     * @param queueName name of queue
     * @return set of message ids
     * @throws AndesException
     */
    TreeSet<Long> getMessageIds(String queueName) throws AndesException;

    /**
     * Delete a message id.
     *
     * @param messageId id of message
     * @throws AndesException
     */
    void deleteMessageId(long messageId) throws AndesException;

    /**
     * Get all assigned slots for give node.
     *
     * @param nodeId id of node
     * @return set of assigned slot objects
     * @throws AndesException
     */
    TreeSet<Slot> getAssignedSlotsByNodeId(String nodeId) throws AndesException;

    /**
     * Get all slots for a give queue.
     *
     * @param queueName name of queue
     * @return set of slot object for queue
     * @throws AndesException
     */
    TreeSet<Slot> getAllSlotsByQueueName(String queueName) throws AndesException;

    /**
     * Get all active queue names.
     *
     * @return set of queue names
     * @throws AndesException
     */
    Set<String> getAllQueues() throws AndesException;

    /**
     * Get all queue names for which messages are published
     * @return Set of queue names
     * @throws AndesException
     */
    Set<String> getAllQueuesInSubmittedSlots() throws AndesException;

    /**
     * Clear and reset slot storage
     *
     * @throws AndesException
     */
    void clearSlotStorage() throws AndesException;

    /**
     * Close the context store
     */
    void close();

    /*
     * ============================ Coordination related methods =======================================
     */

    /**
     * Try to create the coordinator entry using local node information.
     *
     * @param nodeId        local nodes ID
     * @param thriftAddress thrift host/IP of the local node
     * @return True if row was created, false otherwise
     */
    boolean createCoordinatorEntry(String nodeId, InetSocketAddress thriftAddress) throws AndesException;

    /**
     * Check if the given node is the coordinator
     *
     * @param nodeId local node ID
     * @return True if the given node is the coordinator, false otherwise
     */
    boolean checkIsCoordinator(String nodeId) throws AndesException;

    /**
     * Update coordinator heartbeat value to current time
     *
     * @param nodeId local node ID
     */
    boolean updateCoordinatorHeartbeat(String nodeId) throws AndesException;

    /**
     * Check if the coordinator is timed out using the heart beat value
     *
     * @param age maximum relative age with respect to current time in milliseconds
     * @return True if timed out, False otherwise
     */
    boolean checkIfCoordinatorValid(int age) throws AndesException;

    /**
     * Get current coordinator's thrift address
     *
     * @return thrift address of the coordinator
     * @throws AndesException
     */
    InetSocketAddress getCoordinatorThriftAddress() throws AndesException;

    /**
     * Remove current Coordinator entry from database
     */
    void removeCoordinator() throws AndesException;

    /**
     * Update Node heartbeat value to current time
     *
     * @param nodeId local node ID
     */
    boolean updateNodeHeartbeat(String nodeId) throws AndesException;

    /**
     * Update Node heartbeat value to current time
     *
     * @param nodeId      local node ID
     * @param nodeAddress Hazelcast bind address of the node
     * @throws AndesException when an error is detected while calling the store (mostly due to a DB error)
     */
    void createNodeHeartbeatEntry(String nodeId, InetSocketAddress nodeAddress) throws AndesException;

    /**
     * Get node heart beat status for all existing nodes
     *
     * @throws AndesException when an error is detected while calling the store (mostly due to a DB error)
     */
    List<NodeHeartBeatData> getAllHeartBeatData() throws AndesException;

    /**
     * Remove heartbeat entry for the given node. This is normally done when the coordinator detects that the node
     * has left the
     *
     * @param nodeId local node ID
     * @throws AndesException when an error is detected while calling the store (mostly due to a DB error)
     */
    void removeNodeHeartbeat(String nodeId) throws AndesException;

    /**
     * Use this method to indicate that the coordinator detected the node addition to cluster.
     *
     * @param nodeId local node ID
     * @throws AndesException when an error is detected while calling the store (mostly due to a DB error)
     */
    void markNodeAsNotNew(String nodeId) throws AndesException;

    /**
     * Get current coordinator's node ID
     *
     * @return node ID of the current coordinator
     * @throws AndesException when an error is detected while calling the store (mostly due to a DB error)
     */
    String getCoordinatorNodeId() throws AndesException;

    /**
     * Clear all heartbeat data present in the database. This is normally done when the cluster is restarted
     *
     * @throws AndesException when an error is detected while calling the store (mostly due to a DB error)
     */
    void clearHeartBeatData() throws AndesException;

    /*
     * ============================ Membership related methods =======================================
     */

    /**
     * Method to store cluster membership based event.
     *
     * @param clusterNodes        nodes by which the event is destined to be read
     * @param membershipEventType the membership change type
     * @param changedMember       member for which the event was triggered
     */
    void storeMembershipEvent(List<String> clusterNodes, int membershipEventType, String changedMember)
            throws AndesException;

    /**
     * Method to read cluster membership changed events for a nodeID.
     *
     * @param nodeID local node ID used to read event for current node
     */
    List<MembershipEvent> readMemberShipEvents(String nodeID) throws AndesException;

    /**
     * Method to remove all membership events from the store.
     */
    void clearMembershipEvents() throws AndesException;

    /**
     * Method to remove all membership events from the store for a particular node.
     */
    void clearMembershipEvents(String nodeID) throws AndesException;

    /**
     * Stores cluster notifications in the store to be read by cluster members.
     *
     * @param clusterNodes             node list for which the cluster notification should be duplicated
     * @param originatedNode           node from which, the cluster notification was sent
     * @param artifactType             broker artifact being notified
     * @param clusterNotificationType  type of the change addressed by the cluster notification. e.g. "QUEUUE_ADDED"
     * @param notification             notification encoded as a string
     * @param description              readable description of the notification
     * @throws AndesException
     */
    void storeClusterNotification(List<String> clusterNodes, String originatedNode, String artifactType, String
            clusterNotificationType, String notification, String description) throws AndesException;

    /**
     * Reads cluster notifications that are destined to a specific node.
     *
     * @param nodeID the destined node id
     * @return the list of cluster notifications bound to the specified node
     * @throws AndesException
     */
    List<ClusterNotification> readClusterNotifications(String nodeID) throws AndesException;

    /**
     * Clears all cluster notifications.
     *
     * @throws AndesException
     */
    void clearClusterNotifications() throws AndesException;

    /**
     * Clears cluster notifications for a particular node.
     *
     * @param nodeID the node id for which, cluster notifications should be removed.
     * @throws AndesException
     */
    void clearClusterNotifications(String nodeID) throws AndesException;

}
