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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.kernel.AndesBinding;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesExchange;
import org.wso2.andes.kernel.AndesQueue;
import org.wso2.andes.kernel.DurableStoreConnection;

/**
 * Implementation of {@link AndesContextStore} which observes failures such is
 * connection errors. Any {@link AndesContextStore} implementation specified in
 * broker.xml will be wrapped by this class.
 * 
 */
public class FailureObservingAndesContextStore implements AndesContextStore {

    /**
     * {@link AndesContextStore} specified in broker.xml
     */
    private AndesContextStore wrappedInstance;

    /**
     * Future referring to a scheduled task which check the connectivity to the
     * store.
     * Used to cancel the periodic task after store becomes operational.
     */
    private ScheduledFuture<?> storeHealthDetectingFuture;

    /**
     * {@inheritDoc}
     */
    public FailureObservingAndesContextStore(AndesContextStore wrapped) {
        this.wrappedInstance = wrapped;
        this.storeHealthDetectingFuture = null;
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
    public void storeDurableSubscription(String destinationIdentifier, String subscriptionID,
                                         String subscriptionEncodeAsStr) throws AndesException {
        try {
            wrappedInstance.storeDurableSubscription(destinationIdentifier, subscriptionID, subscriptionEncodeAsStr);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateDurableSubscription(String destinationIdentifier, String subscriptionID,
                                          String subscriptionEncodeAsStr) throws AndesException {
        try {
            wrappedInstance.updateDurableSubscription(destinationIdentifier, subscriptionID, subscriptionEncodeAsStr);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeDurableSubscription(String destinationIdentifier, String subscriptionID) throws AndesException {
        try {
            wrappedInstance.removeDurableSubscription(destinationIdentifier, subscriptionID);
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
    public List<AndesExchange> getAllExchangesStored() throws AndesException {
        try {
            return wrappedInstance.getAllExchangesStored();
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
    public List<AndesQueue> getAllQueuesStored() throws AndesException {
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
     * {@inheritDoc}.
     * <p>
     * Alters the behavior where
     * <ol>
     * <li>checks the operational status of the wrapped context store</li>
     * <li>if context store is operational it will cancel the periodic task</li>
     * </ol>
     */
    @Override
    public boolean isOperational(String testString, long testTime) {
        boolean operational = false;
        if (wrappedInstance.isOperational(testString, testTime)) {
            operational = true;
            if (storeHealthDetectingFuture != null) {
                // we have detected that store is operational therefore
                // we don't need to run the periodic task to check weather store
                // is available.
                storeHealthDetectingFuture.cancel(false);
                storeHealthDetectingFuture = null;
            }

        }
        return operational;
    }

    /**
     * A convenient method to notify all {@link StoreHealthListener}s that
     * context store became offline
     * 
     * @param e
     *            the exception occurred.
     */
    private synchronized void notifyFailures(AndesStoreUnavailableException e) {

        if (storeHealthDetectingFuture == null) {
            // this is the first failure
            FailureObservingStoreManager.notifyStoreNonOperational(e, wrappedInstance);
            storeHealthDetectingFuture = FailureObservingStoreManager.scheduleHealthCheckTask(this);

        }
    }

}
