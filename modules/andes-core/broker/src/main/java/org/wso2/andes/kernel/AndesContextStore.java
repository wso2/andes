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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel;

import org.apache.tools.ant.taskdefs.condition.And;
import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.store.HealthAwareStore;

import org.wso2.andes.kernel.slot.Slot;
import org.wso2.andes.kernel.slot.SlotState;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * AndesContextStore is an abstraction of underlying data base to store information related to
 * exchanges, queues, bindings, durable subscriptions and queue message counts.
 */
public interface AndesContextStore extends HealthAwareStore{

    /**
     * Initialize the storage and makes a connection to the data base
     * @param connectionProperties ConfigurationProperties
     * @return returns the created DurableStoreConnection object created
     * @throws AndesException
     */
    public DurableStoreConnection init(ConfigurationProperties connectionProperties) throws
                                                                                     AndesException;

    /**
     * Get all durable encoded subscriptions as strings
     *
     * @return list of <id,subscriptions>
     */
    public Map<String, List<String>> getAllStoredDurableSubscriptions() throws AndesException;

    /**
     * Store subscription to the durable store
     *
     * @param destinationIdentifier   Identifier of the destination (queue/topic) of the queue this subscription is bound to
     * @param subscriptionID          Id of the subscription
     * @param subscriptionEncodeAsStr String encoded subscription
     * @throws AndesException
     */
    public void storeDurableSubscription(String destinationIdentifier, String subscriptionID, String subscriptionEncodeAsStr) throws AndesException;


    /**
     * Update already existing subscription
     *
     * @param destinationIdentifier     Identifier of the destination (queue/topic) of the queue this subscription is bound to
     * @param subscriptionID            Id of the subscription
     * @param subscriptionEncodeAsStr   String encoded subscription to be updated
     * @throws AndesException
     */
    void updateDurableSubscription(String destinationIdentifier, String subscriptionID, String subscriptionEncodeAsStr) throws AndesException;

    /**
     * Remove stored subscription from durable store
     *
     * @param destinationIdentifier identifier of the destination (queue/topic) of the queue this subscription is bound to
     * @param subscriptionID        id of the subscription
     */
    public void removeDurableSubscription(String destinationIdentifier, String subscriptionID) throws AndesException;

    /**
     * Store details of node
     *
     * @param nodeID id of the node
     * @param data   detail to store
     */
    public void storeNodeDetails(String nodeID, String data) throws AndesException;

    /**
     * Get all node information stored
     *
     * @return map of node information
     */
    public Map<String, String> getAllStoredNodeData() throws AndesException;

    /**
     * Remove stored node information
     *
     * @param nodeID id of the node
     */
    public void removeNodeData(String nodeID) throws AndesException;

    /**
     * Add message counting entry for queue. queue count is initialised to zero. The counter for
     * created queue can then be incremented and decremented.
     * @see this.removeMessageCounterForQueue this.incrementMessageCountForQueue,
     * this.decrementMessageCountForQueue
     *
     * @param destinationQueueName name of queue
     */
    void addMessageCounterForQueue(String destinationQueueName) throws AndesException;

    /**
     * Get message count of queue
     *
     * @param destinationQueueName name of queue
     * @return message count
     */
    long getMessageCountForQueue(String destinationQueueName) throws AndesException;

    /**
     * Store level method to reset the message counter of a given queue to 0.
     * @param storageQueueName name of the queue being purged
     * @throws AndesException
     */
    void resetMessageCounterForQueue(String storageQueueName) throws AndesException;

    /**
     * Remove Message counting entry
     *
     * @param destinationQueueName name of queue
     */
    void removeMessageCounterForQueue(String destinationQueueName) throws AndesException;


    /**
     * Increment message counter for a queue by a given incrementBy value
     * @param destinationQueueName name of the queue
     * @param incrementBy  increment counter by
     * @throws AndesException
     */
    void incrementMessageCountForQueue(String destinationQueueName, long incrementBy) throws AndesException;


    /**
     * Decrement message counter for a queue
     *
     * @param destinationQueueName name of the queue
     * @param decrementBy          decrement counter by
     * @throws AndesException
     */
    void decrementMessageCountForQueue(String destinationQueueName, long decrementBy) throws AndesException;

    /**
     * Store exchange information (amqp)
     *
     * @param exchangeName name of string
     * @param exchangeInfo info for exchange
     */
    public void storeExchangeInformation(String exchangeName, String exchangeInfo) throws AndesException;

    /**
     * Get all exchanges stored
     *
     * @return list of exchanges
     */
    public List<AndesExchange> getAllExchangesStored() throws AndesException;


    /**
     * Delete all exchange information
     *
     * @param exchangeName name of exchange
     */
    public void deleteExchangeInformation(String exchangeName) throws AndesException;


    /**
     * Store a queue
     *
     * @param queueName name of the queue to be stored
     * @param queueInfo string encoded queue information
     * @throws AndesException
     */
    public void storeQueueInformation(String queueName, String queueInfo) throws AndesException;

    /**
     * Get all stored queues
     *
     * @return list of queues
     * @throws AndesException
     */
    public List<AndesQueue> getAllQueuesStored() throws AndesException;

    /**
     * Delete a queue from store
     *
     * @param queueName name of the queue to be removed
     * @throws AndesException
     */
    public void deleteQueueInformation(String queueName) throws AndesException;

    /**
     * Store a binding. Bound exchange and bound queue name together will be unique
     *
     * @param exchange       name of the exchange binding represent
     * @param boundQueueName target queue binding is done
     * @param bindingInfo     binding information as a string
     * @throws AndesException
     */
    public void storeBindingInformation(String exchange, String boundQueueName, String bindingInfo) throws AndesException;

    /**
     * Get bindings stored for some exchange
     *
     * @return a list of bindings belonging to the exchange
     * @throws AndesException
     */
    public List<AndesBinding> getBindingsStoredForExchange(String exchangeName) throws AndesException;

    /**
     * Remove a binding from the store
     *
     * @param exchangeName   name of the exchange
     * @param boundQueueName name of the queue binding relates to
     * @throws AndesException
     */
    public void deleteBindingInformation(String exchangeName, String boundQueueName) throws AndesException;

    /**
     * Create a new slot in store

     * @param startMessageId start message id of slot
     * @param endMessageId end message id of slot
     * @param storageQueueName name of storage queue name
     * @param assignedNodeId
     * @throws AndesException
     */
    public void createSlot(long startMessageId, long endMessageId,
                           String storageQueueName, String assignedNodeId) throws AndesException;

    /**
     * Delete a slot from store
     * @param startMessageId start message id of slot
     * @param endMessageId end message id of slot
     * @throws AndesException
     */
    public void deleteSlot(long startMessageId, long endMessageId) throws AndesException;

    /**
     * Delete all slots by queue name
     * @param queueName name of queue
     * @throws AndesException
     */
    public void deleteSlotsByQueueName(String queueName) throws AndesException;

    /**
     * Delete message ids by queue name
     * @param queueName name of queue
     * @throws AndesException
     */
    public void deleteMessageIdsByQueueName(String queueName) throws AndesException;

    /**
     * Unassign and return slot
     * @param startMessageId start message id of slot
     * @param endMessageId end message id of slot
     * @throws AndesException
     */
    public void deleteSlotAssignment(long startMessageId, long endMessageId) throws AndesException;

    /**
     * Unassign slots by queue name
     * @param nodeId id of node
     * @param queueName name of queue
     * @throws AndesException
     */
    public void deleteSlotAssignmentByQueueName(String nodeId, String queueName)
            throws AndesException;

    /**
     * Update assignment information in slot store
     * @param nodeId id of node
     * @param queueName name of queue
     * @param startMsgId start message id of slot
     * @param endMsgId end message id of slot
     * @throws AndesException
     */
    public void createSlotAssignment(String nodeId, String queueName, long startMsgId,
                                     long endMsgId)
            throws AndesException;

    /**
     * Select unassigned slots for a given queue name
     * @param queueName name of queue
     * @return unassigned slot object if found
     * @throws AndesException
     */
    public Slot selectUnAssignedSlot(String queueName) throws AndesException;

    /**
     * Get last assigned id for a queue
     * @param queueName name of queue
     * @return last assigned id of queue
     * @throws AndesException
     */
    public long getQueueToLastAssignedId(String queueName) throws AndesException;

    /**
     * Set last assigned id for a given queue
     * @param queueName name of queue
     * @param messageId id of message
     * @throws AndesException
     */
    public void setQueueToLastAssignedId(String queueName, long messageId) throws AndesException;

    /**
     * Get last published id for a given node
     * @param nodeId id of node
     * @return last published if of node
     * @throws AndesException
     */
    public long getNodeToLastPublishedId(String nodeId) throws AndesException;

    /**
     * Set last published id for a given node
     * @param nodeId id of node
     * @param messageId id of message
     * @throws AndesException
     */
    public void setNodeToLastPublishedId(String nodeId, long messageId) throws AndesException;

    /**
     * Get all message published nodes
     * @return set of published nodes
     * @throws AndesException
     */
    public TreeSet<String> getMessagePublishedNodes() throws AndesException;

    /**
     * Set slots states
     * @param startMessageId start message id of slot
     * @param endMessageId end message id of slot
     * @param slotState state of slot
     * @throws AndesException
     */
    public void setSlotState(long startMessageId, long endMessageId, SlotState slotState)
            throws AndesException;

    /**
     * Get overlapped slots for a given queue
     * @param queueName name of queue
     * @return overlapped slot object
     * @throws AndesException
     */
    public Slot getOverlappedSlot(String queueName) throws AndesException;

    /**
     * Add message ids to store
     * @param queueName name of queue
     * @param messageId id of message
     * @throws AndesException
     */
    public void addMessageId(String queueName, long messageId) throws AndesException;

    /**
     * Get message ids for a given queue
     * @param queueName name of queue
     * @return set of message ids
     * @throws AndesException
     */
    public TreeSet<Long> getMessageIds(String queueName) throws AndesException;

    /**
     * Delete a message id
     * @param messageId id of message
     * @throws AndesException
     */
    public void deleteMessageId(long messageId) throws AndesException;

    /**
     * Get all assigned slots for give node
     * @param nodeId id of node
     * @return set of assigned slot objects
     * @throws AndesException
     */
    public TreeSet<Slot> getAssignedSlotsByNodeId(String nodeId) throws AndesException;

    /**
     * Get all slots for a give queue
     * @param queueName name of queue
     * @return set of slot object for queue
     * @throws AndesException
     */
    public TreeSet<Slot> getAllSlotsByQueueName(String queueName) throws AndesException;

    /**
     * Get all active queue names
     * @return set of queue names
     * @throws AndesException
     */
    public Set<String> getAllQueues() throws AndesException;

    /**
     * Close the context store
     */
    public void close();

}
