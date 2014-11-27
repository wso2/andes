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

import org.apache.log4j.Logger;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.coordination.ClusterCoordinationHandler;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;
import org.wso2.andes.subscription.SubscriptionStore;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is for managing control information of
 * Andes. (eg: exchanges/queues/bindings)
 */
public class AndesContextInformationManager {

    private static final Logger log;

    private static AndesContextInformationManager contextManager;

    private AndesContextInformationManager() {
        //register listeners for queue changes
        addQueueListener(new ClusterCoordinationHandler(HazelcastAgent.getInstance()));

        //register listeners for exchange changes
        addExchangeListener(new ClusterCoordinationHandler(HazelcastAgent.getInstance()));

        //register listeners for binding changes
        addBindingListener(new ClusterCoordinationHandler(HazelcastAgent.getInstance()));
    }

    private SubscriptionStore subscriptionStore;

    static {
        log = Logger.getLogger(AndesContextInformationManager.class);
        contextManager = new AndesContextInformationManager();
    }

    public static AndesContextInformationManager getInstance() {
        return contextManager;
    }

    //keep listeners that should be triggered when constructs are updated
    private List<QueueListener> queueListeners = new ArrayList<QueueListener>();
    private List<ExchangeListener> exchangeListeners = new ArrayList<ExchangeListener>();
    private List<BindingListener> bindingListeners = new ArrayList<BindingListener>();

    /**
     * Initialize the context manager
     * @throws AndesException
     */
    public void initialize() throws AndesException {
        subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
    }

    /**
     * Register a listener interested in local binding changes
     *
     * @param listener listener to register
     */
    public void addBindingListener(BindingListener listener) {
        bindingListeners.add(listener);
    }

    /**
     * Register a listener interested on queue changes
     *
     * @param listener listener to be registered
     */
    public void addQueueListener(QueueListener listener) {
        queueListeners.add(listener);
    }

    /**
     * Register a listener interested on exchange changes
     *
     * @param listener listener to be registered
     */
    public void addExchangeListener(ExchangeListener listener) {
        exchangeListeners.add(listener);
    }

    /**
     * Create an exchange in andes kernel
     *
     * @param exchange qpid exchange
     * @throws org.wso2.andes.kernel.AndesException
     */
    public void createExchange(AndesExchange exchange) throws AndesException {
        AndesContext.getInstance().getAMQPConstructStore().addExchange(exchange, true);
        notifyExchangeListeners(exchange, ExchangeListener.ExchangeChange.Added);
    }

    /**
     * Delete exchange from andes kernel
     *
     * @param exchange  exchange to delete
     * @throws org.wso2.andes.kernel.AndesException
     */
    public void deleteExchange(AndesExchange exchange) throws AndesException {
        AndesContext.getInstance().getAMQPConstructStore()
                    .removeExchange(exchange.exchangeName, true);
        notifyExchangeListeners(exchange, ExchangeListener.ExchangeChange.Deleted);
    }

    /**
     * Create queue in andes kernel
     *
     * @param queue queue to create
     *@throws org.wso2.andes.kernel.AndesException
     */
    public void createQueue(AndesQueue queue) throws AndesException {
        AndesContext.getInstance().getAMQPConstructStore().addQueue(queue, true);
        notifyQueueListeners(queue, QueueListener.QueueChange.Added);
    }

    /**
     * Delete the queue from broker. This will purge the queue and
     * delete cluster-wide
     * @param queueName  name of the queue
     * @throws AndesException
     */
    public void deleteQueue(String queueName) throws AndesException {
        //check if there are active subscribers for the queue. It is enough
        //to get queue subscribers as for durable topics we consider queue subscriptions
        //hierarchical topic subscription case is also considered here
        //if a subscription is inactive it is not considered
        List<AndesSubscription> queueSubscriptions = subscriptionStore.getActiveClusterSubscribersForDestination(queueName,false);
        if(queueSubscriptions.isEmpty()) {
            //purge the queue cluster-wide
            MessagingEngine.getInstance().purgeMessages(queueName, null, false);
            //identify queue to delete
            AndesQueue queueToDelete = null;
            List<AndesQueue> queueList = AndesContext.getInstance().getAndesContextStore().getAllQueuesStored();
            for(AndesQueue queue : queueList) {
                if(queue.queueName.equals(queueName)) {
                    queueToDelete = queue;
                    break;
                }
            }
            //delete queue from context store
            AndesContext.getInstance().getAndesContextStore().deleteQueueInformation(queueName);
            AndesContext.getInstance().getAndesContextStore().removeMessageCounterForQueue(queueName);
            //Notify cluster to delete queue
            notifyQueueListeners(queueToDelete, QueueListener.QueueChange.Deleted);

            //delete all subscription entries if remaining (inactive entries)
            ClusterResourceHolder.getInstance().getSubscriptionManager().deleteSubscriptionsOfBoundQueue(queueName);
            log.info("Deleted queue " + queueName);

        } else {
            log.warn("Cannot Delete Queue " + queueName + " During Queue Delete or Unsubscribe," +
                     " as It Has Active Subscribers. Please Stop Them First.");
            throw new AndesException("Cannot Delete Queue " + queueName + " During Queue Delete or Unsubscribe," +
                                     " as It Has Active Subscribers. Please Stop Them First.");
        }
    }

    /**
     * Create andes binding in Andes kernel
     * @param andesBinding  binding to be created
     * @throws AndesException
     */
    public void createBinding(AndesBinding andesBinding) throws AndesException {
        AndesContext.getInstance().getAMQPConstructStore().addBinding(andesBinding, true);
        notifyBindingListeners(andesBinding, BindingListener.BindingChange.Added);
    }

    /**
     * Remove andes binding from andes kernel
     * @param andesBinding binding to be removed
     * @throws AndesException
     */
    public void removeBinding(AndesBinding andesBinding) throws AndesException {
        AndesContext.getInstance().getAMQPConstructStore().removeBinding(andesBinding.boundExchangeName,
                                                                         andesBinding.boundQueue.queueName,
                                                                         true);
        notifyBindingListeners(andesBinding, BindingListener.BindingChange.Deleted);
    }

    private void notifyExchangeListeners(AndesExchange exchange, ExchangeListener.ExchangeChange change) throws AndesException {
        for(ExchangeListener listener : exchangeListeners) {
            listener.handleLocalExchangesChanged(exchange, change);
        }
    }

    private void notifyQueueListeners(AndesQueue queue, QueueListener.QueueChange change) throws AndesException {
        for(QueueListener listener : queueListeners) {
            listener.handleLocalQueuesChanged(queue, change);
        }
    }

    private void notifyBindingListeners(AndesBinding binding, BindingListener.BindingChange change) throws AndesException {
        for(BindingListener listener : bindingListeners) {
            listener.handleLocalBindingsChanged(binding, change);
        }
    }
}
