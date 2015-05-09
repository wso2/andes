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

import org.wso2.andes.configuration.util.ConfigurationProperties;
import java.util.List;
import java.util.Map;

/**
 * AndesContextStore is an abstraction of underlying data base to store information related to
 * exchanges, queues, bindings, durable subscriptions and queue message counts.
 */
public interface AndesContextStore {

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
     * Updating the exclusiveConsumer Value of a stored queue
     * @param queueName name of the queue
     * @param isExclusiveConsumer exclusive Consumer Value of the queue
     * @throws AndesException
     */
    public void updateExclusiveConsumerForQueue(String queueName, boolean isExclusiveConsumer) throws AndesException;

    /**
     * Close the context store
     */
    public void close();

}
