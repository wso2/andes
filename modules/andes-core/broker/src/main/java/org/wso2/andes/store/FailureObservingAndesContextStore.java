/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.store;

import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.kernel.AndesBinding;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.DurableStoreConnection;
import org.wso2.andes.kernel.router.AndesMessageRouter;
import org.wso2.andes.kernel.slot.Slot;
import org.wso2.andes.kernel.slot.SlotState;
import org.wso2.andes.kernel.subscription.AndesSubscription;
import org.wso2.andes.kernel.subscription.StorageQueue;
import org.wso2.andes.server.cluster.NodeHeartBeatData;
import org.wso2.andes.server.cluster.coordination.ClusterNotification;
import org.wso2.andes.server.cluster.coordination.rdbms.MembershipEvent;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Implementation of {@link AndesContextStore} which observes failures such is
 * connection errors. Any {@link AndesContextStore} implementation specified in
 * broker.xml will be wrapped by this class.
 *
 */
public class FailureObservingAndesContextStore extends FailureObservingStore<AndesContextStore> implements AndesContextStore {

    /**
     * {@inheritDoc}
     */
    public FailureObservingAndesContextStore(AndesContextStore contextStore, FailureObservingStoreManager manager) {
        super(contextStore, manager);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DurableStoreConnection init(ConfigurationProperties connectionProperties) throws AndesException {

        try {
            return wrappedInstance.init(connectionProperties);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, List<String>> getAllStoredDurableSubscriptions() throws AndesException {

        try {
            return wrappedInstance.getAllStoredDurableSubscriptions();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> getAllDurableSubscriptionsByID() throws AndesException {
        try {
            return wrappedInstance.getAllDurableSubscriptionsByID();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isSubscriptionExist(String subscriptionId) throws AndesException {
        try {
            return wrappedInstance.isSubscriptionExist(subscriptionId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeDurableSubscription(AndesSubscription subscription) throws AndesException {
        try {
            wrappedInstance.storeDurableSubscription(subscription);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int updateDurableSubscription(AndesSubscription subscription) throws AndesException {
        try {
            return wrappedInstance.updateDurableSubscription(subscription);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateOrInsertDurableSubscription(AndesSubscription subscription) throws AndesException {
        try {
            wrappedInstance.updateOrInsertDurableSubscription(subscription);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateDurableSubscriptions(Map<String, String> subscriptions) throws AndesException {
        try {
            wrappedInstance.updateDurableSubscriptions(subscriptions);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeDurableSubscription(AndesSubscription subscription) throws AndesException {
        try {
            wrappedInstance.removeDurableSubscription(subscription);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeNodeDetails(String nodeID, String data) throws AndesException {
        try {
            wrappedInstance.storeNodeDetails(nodeID, data);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> getAllStoredNodeData() throws AndesException {
        try {
            return wrappedInstance.getAllStoredNodeData();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeNodeData(String nodeID) throws AndesException {
        try {
            wrappedInstance.removeNodeData(nodeID);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMessageCounterForQueue(String destinationQueueName) throws AndesException {
        try {
            wrappedInstance.addMessageCounterForQueue(destinationQueueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getMessageCountForQueue(String destinationQueueName) throws AndesException {
        try {
            return wrappedInstance.getMessageCountForQueue(destinationQueueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void resetMessageCounterForQueue(String storageQueueName) throws AndesException {
        try {
            wrappedInstance.resetMessageCounterForQueue(storageQueueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeMessageCounterForQueue(String destinationQueueName) throws AndesException {
        try {
            wrappedInstance.removeMessageCounterForQueue(destinationQueueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void incrementMessageCountForQueue(String destinationQueueName, long incrementBy) throws AndesException {
        try {
            wrappedInstance.incrementMessageCountForQueue(destinationQueueName, incrementBy);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void decrementMessageCountForQueue(String destinationQueueName, long decrementBy) throws AndesException {
        try {
            wrappedInstance.decrementMessageCountForQueue(destinationQueueName, decrementBy);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeExchangeInformation(String exchangeName, String exchangeInfo) throws AndesException {
        try {
            wrappedInstance.storeExchangeInformation(exchangeName, exchangeInfo);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageRouter> getAllMessageRoutersStored() throws AndesException {
        try {
            return wrappedInstance.getAllMessageRoutersStored();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteExchangeInformation(String exchangeName) throws AndesException {
        try {
            wrappedInstance.deleteExchangeInformation(exchangeName);

        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeQueueInformation(String queueName, String queueInfo) throws AndesException {
        try {
            wrappedInstance.storeQueueInformation(queueName, queueInfo);

        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<StorageQueue> getAllQueuesStored() throws AndesException {
        try {
            return wrappedInstance.getAllQueuesStored();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteQueueInformation(String queueName) throws AndesException {
        try {
            wrappedInstance.deleteQueueInformation(queueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeBindingInformation(String exchange, String boundQueueName, String bindingInfo)
            throws AndesException {
        try {
            wrappedInstance.storeBindingInformation(exchange, boundQueueName, bindingInfo);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesBinding> getBindingsStoredForExchange(String exchangeName) throws AndesException {
        try {
            return wrappedInstance.getBindingsStoredForExchange(exchangeName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteBindingInformation(String exchangeName, String boundQueueName) throws AndesException {
        try {
            wrappedInstance.deleteBindingInformation(exchangeName, boundQueueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        wrappedInstance.close();
    }

    /**
     * Create a new slot in store
     *
     * @param startMessageId   start message id of slot
     * @param endMessageId     end message id of slot
     * @param storageQueueName name of storage queue name
     * @param assignedNodeId Node id of assigned node
     * @throws AndesException
     */
    @Override
    public void createSlot(long startMessageId, long endMessageId, String storageQueueName, String assignedNodeId) throws AndesException {
        try {
            wrappedInstance.createSlot(startMessageId, endMessageId, storageQueueName,
                                       assignedNodeId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean deleteNonOverlappingSlot(long startMessageId, long endMessageId) throws AndesException {
        try {
            return wrappedInstance.deleteNonOverlappingSlot(startMessageId, endMessageId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * Delete a slot from store
     *
     * @param startMessageId start message id of slot
     * @param endMessageId   end message id of slot
     * @throws AndesException
     */
    @Override
    public boolean deleteSlot(long startMessageId, long endMessageId) throws AndesException {
        try {
            return wrappedInstance.deleteSlot(startMessageId, endMessageId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * Delete all slots by queue name
     *
     * @param queueName name of queue
     * @throws AndesException
     */
    @Override
    public void deleteSlotsByQueueName(String queueName) throws AndesException {
        try {
            wrappedInstance.deleteSlotsByQueueName(queueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * Delete message ids by queue name
     *
     * @param queueName name of queue
     * @throws AndesException
     */
    @Override
    public void deleteMessageIdsByQueueName(String queueName) throws AndesException {
        try {
            wrappedInstance.deleteMessageIdsByQueueName(queueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * Unassign and return slot
     *
     * @param startMessageId start message id of slot
     * @param endMessageId   end message id of slot
     * @throws AndesException
     */
    @Override
    public void deleteSlotAssignment(long startMessageId, long endMessageId) throws AndesException {
        try {
            wrappedInstance.deleteSlotAssignment(startMessageId, endMessageId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * Unassign slots by queue name
     *
     * @param nodeId    id of node
     * @param queueName name of queue
     * @throws AndesException
     */
    @Override
    public void deleteSlotAssignmentByQueueName(String nodeId, String queueName) throws AndesException {
        try {
            wrappedInstance.deleteSlotAssignmentByQueueName(nodeId, queueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * Update assignment information in slot store
     *
     * @param nodeId     id of node
     * @param queueName  name of queue
     * @param startMsgId start message id of slot
     * @param endMsgId   end message id of slot
     * @throws AndesException
     */
    @Override
    public void createSlotAssignment(String nodeId, String queueName, long startMsgId, long endMsgId) throws AndesException {
        try {
            wrappedInstance.createSlotAssignment(nodeId, queueName, startMsgId, endMsgId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * Select unassigned slots for a given queue name
     *
     * @param queueName name of queue
     * @return unassigned slot object if found
     * @throws AndesException
     */
    @Override
    public Slot selectUnAssignedSlot(String queueName) throws AndesException {
        try {
            return wrappedInstance.selectUnAssignedSlot(queueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * Get last assigned id for a queue
     *
     * @param queueName name of queue
     * @return last assigned id of queue
     * @throws AndesException
     */
    @Override
    public long getQueueToLastAssignedId(String queueName) throws AndesException {
        try {
            return wrappedInstance.getQueueToLastAssignedId(queueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * Set last assigned id for a given queue
     *
     * @param queueName name of queue
     * @param messageId id of message
     * @throws AndesException
     */
    @Override
    public void setQueueToLastAssignedId(String queueName, long messageId) throws AndesException {
        try {
            wrappedInstance.setQueueToLastAssignedId(queueName, messageId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * Get last published id for a given node
     *
     * @param nodeId id of node
     * @return last published if of node
     * @throws AndesException
     */
    @Override
    public long getLocalSafeZoneOfNode(String nodeId) throws AndesException {
        try {
            return wrappedInstance.getLocalSafeZoneOfNode(nodeId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * Set last published id for a given node
     *
     * @param nodeId    id of node
     * @param messageId id of message
     * @throws AndesException
     */
    @Override
    public void setLocalSafeZoneOfNode(String nodeId, long messageId) throws AndesException {
        try {
            wrappedInstance.setLocalSafeZoneOfNode(nodeId, messageId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removePublisherNodeId(String nodeId) throws AndesException {
        try {
            wrappedInstance.removePublisherNodeId(nodeId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * Get all message published nodes
     *
     * @return set of published nodes
     * @throws AndesException
     */
    @Override
    public TreeSet<String> getMessagePublishedNodes() throws AndesException {
        try {
            return wrappedInstance.getMessagePublishedNodes();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * Set slots states
     *
     * @param startMessageId start message id of slot
     * @param endMessageId   end message id of slot
     * @param slotState      state of slot
     * @throws AndesException
     */
    @Override
    public void setSlotState(long startMessageId, long endMessageId, SlotState slotState) throws AndesException {
        try {
            wrappedInstance.setSlotState(startMessageId, endMessageId, slotState);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Slot getOverlappedSlot(String nodeId, String queueName) throws AndesException {
        try {
            return wrappedInstance.getOverlappedSlot(nodeId, queueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * Add message ids to store
     *
     * @param queueName name of queue
     * @param messageId id of message
     * @throws AndesException
     */
    @Override
    public void addMessageId(String queueName, long messageId) throws AndesException {
        try {
            wrappedInstance.addMessageId(queueName, messageId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * Get message ids for a given queue
     *
     * @param queueName name of queue
     * @return set of message ids
     * @throws AndesException
     */
    @Override
    public TreeSet<Long> getMessageIds(String queueName) throws AndesException {
        try {
            return wrappedInstance.getMessageIds(queueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * Delete a message id
     *
     * @param messageId id of message
     * @throws AndesException
     */
    @Override
    public void deleteMessageId(long messageId) throws AndesException {
        try {
            wrappedInstance.deleteMessageId(messageId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * Get all assigned slots for give node
     *
     * @param nodeId id of node
     * @return set of assigned slot objects
     * @throws AndesException
     */
    @Override
    public TreeSet<Slot> getAssignedSlotsByNodeId(String nodeId) throws AndesException {
        try {
            return wrappedInstance.getAssignedSlotsByNodeId(nodeId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TreeSet<Slot> getOverlappedSlotsByNodeId(String nodeId) throws AndesException {
        try {
            return wrappedInstance.getOverlappedSlotsByNodeId(nodeId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * Get all slots for a give queue
     *
     * @param queueName name of queue
     * @return set of slot object for queue
     * @throws AndesException
     */
    @Override
    public TreeSet<Slot> getAllSlotsByQueueName(String queueName) throws AndesException {
        try {
            return wrappedInstance.getAllSlotsByQueueName(queueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> getAllQueues() throws AndesException {
        try {
            return wrappedInstance.getAllQueues();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> getAllQueuesInSubmittedSlots() throws AndesException {
        try {
            return wrappedInstance.getAllQueuesInSubmittedSlots();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }


    /**
     * Clear and reset slot storage
     *
     * @throws AndesException
     */
    @Override
    public void clearSlotStorage() throws AndesException {
        try {
            wrappedInstance.clearSlotStorage();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean createCoordinatorEntry(String nodeId, InetSocketAddress thriftAddress) throws AndesException {
        try {
            return wrappedInstance.createCoordinatorEntry(nodeId, thriftAddress);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkIsCoordinator(String nodeId) throws AndesException {
        try {
            return wrappedInstance.checkIsCoordinator(nodeId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean updateCoordinatorHeartbeat(String nodeId) throws AndesException {
        try {
            return wrappedInstance.updateCoordinatorHeartbeat(nodeId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkIfCoordinatorValid(int age) throws AndesException {
        try {
            return wrappedInstance.checkIfCoordinatorValid(age);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InetSocketAddress getCoordinatorThriftAddress() throws AndesException {
        try {
            return wrappedInstance.getCoordinatorThriftAddress();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeCoordinator() throws AndesException {
        try {
            wrappedInstance.removeCoordinator();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean updateNodeHeartbeat(String nodeId) throws AndesException {
        try {
            return wrappedInstance.updateNodeHeartbeat(nodeId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createNodeHeartbeatEntry(String nodeId,  InetSocketAddress nodeAddress) throws AndesException {
        try {
            wrappedInstance.createNodeHeartbeatEntry(nodeId, nodeAddress);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<NodeHeartBeatData> getAllHeartBeatData() throws AndesException {
        try {
            return wrappedInstance.getAllHeartBeatData();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeNodeHeartbeat(String nodeId) throws AndesException {
        try {
            wrappedInstance.removeNodeHeartbeat(nodeId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void markNodeAsNotNew(String nodeId) throws AndesException{
        try {
            wrappedInstance.markNodeAsNotNew(nodeId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getCoordinatorNodeId() throws AndesException {
        try {
            return wrappedInstance.getCoordinatorNodeId();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    @Override
    public void clearHeartBeatData() throws AndesException {
        try {
            wrappedInstance.clearHeartBeatData();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeMembershipEvent(List<String> clusterNodes, int membershipEventType, String changedMember)
            throws AndesException {
        try {
            wrappedInstance.storeMembershipEvent(clusterNodes, membershipEventType, changedMember);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<MembershipEvent> readMemberShipEvents(String nodeID) throws AndesException {
        try {
            return wrappedInstance.readMemberShipEvents(nodeID);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearMembershipEvents() throws AndesException {
        try {
            wrappedInstance.clearMembershipEvents();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearMembershipEvents(String nodeID) throws AndesException {
        try {
            wrappedInstance.clearMembershipEvents(nodeID);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    public void storeClusterNotification(List<String> clusterNodes, String originatedNode, String artifactType, String
            clusterNotificationType, String notification, String description) throws AndesException {
        try {
            wrappedInstance.storeClusterNotification(clusterNodes, originatedNode,
                                                     artifactType, clusterNotificationType, notification, description);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}.
     */
    public List<ClusterNotification> readClusterNotifications(String nodeID) throws AndesException {
        try {
            return wrappedInstance.readClusterNotifications(nodeID);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    public void clearClusterNotifications() throws AndesException {
        try {
            wrappedInstance.clearClusterNotifications();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    public void clearClusterNotifications(String nodeID) throws AndesException {
        try {
            wrappedInstance.clearClusterNotifications(nodeID);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }
}
