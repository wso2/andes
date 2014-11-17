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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cluster.coordination.ClusterCoordinationHandler;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;

import java.util.ArrayList;
import java.util.List;

/**
 * This task will periodically load exchanges,queues,bindings,subscriptions from database
 * and simulate cluster notifications. This is implemented to bring the node
 * to the current state of cluster in case some hazlecast notifications are missed
 */
public class AndesRecoveryTask implements Runnable {

    private List<QueueListener> queueListeners = new ArrayList<QueueListener>();
    private List<ExchangeListener> exchangeListeners = new ArrayList<ExchangeListener>();
    private List<BindingListener> bindingListeners = new ArrayList<BindingListener>();

    AndesContextStore andesContextStore;
    AMQPConstructStore amqpConstructStore;

    private static Log log = LogFactory.getLog(AndesRecoveryTask.class);

    public AndesRecoveryTask() {
        queueListeners.add(new ClusterCoordinationHandler(HazelcastAgent.getInstance()));
        exchangeListeners.add(new ClusterCoordinationHandler(HazelcastAgent.getInstance()));
        bindingListeners.add(new ClusterCoordinationHandler(HazelcastAgent.getInstance()));

        andesContextStore = AndesContext.getInstance().getAndesContextStore();
        amqpConstructStore = AndesContext.getInstance().getAMQPConstructStore();
    }

    @Override
    public void run() {
        try {
            reloadExchangesFromDB();
            reloadQueuesFromDB();
            reloadBindingsFromDB();
            reloadSubscriptions();
        } catch (AndesException e) {
            log.error("Error in running andes recovery task", e);
        }
    }

    /**
     * reload and recover exchanges,
     * Queues, Bindings and Subscriptions
     *
     * @throws AndesException
     */
    public void recoverExchangesQueuesBindingsSubscriptions() throws AndesException {
        reloadExchangesFromDB();
        reloadQueuesFromDB();
        reloadBindingsFromDB();
        reloadSubscriptions();
    }

    private void reloadExchangesFromDB() throws AndesException {
        List<AndesExchange> fromDB = andesContextStore.getAllExchangesStored();
        List<AndesExchange> fromMap = amqpConstructStore.getExchanges();
        List<AndesExchange> fromDBDuplicated = new ArrayList<AndesExchange>(fromDB);

        fromDB.removeAll(fromMap);
        for (AndesExchange exchange : fromDB) {
            for (ExchangeListener listener : exchangeListeners) {
                log.warn("Recovering node. Adding exchange " + exchange.toString());
                listener.handleClusterExchangesChanged(exchange, ExchangeListener.ExchangeChange.Added);
            }
        }

        fromMap.removeAll(fromDBDuplicated);
        for (AndesExchange exchange : fromMap) {
            for (ExchangeListener listener : exchangeListeners) {
                log.warn("Recovering node. Removing exchange " + exchange.toString());
                listener.handleClusterExchangesChanged(exchange, ExchangeListener.ExchangeChange.Deleted);
            }
        }
    }

    private void reloadQueuesFromDB() throws AndesException {
        List<AndesQueue> fromDB = andesContextStore.getAllQueuesStored();
        List<AndesQueue> fromMap = amqpConstructStore.getQueues();
        List<AndesQueue> fromDBDuplicated = new ArrayList<AndesQueue>(fromDB);

        fromDB.removeAll(fromMap);
        for (AndesQueue queue : fromDB) {
            for (QueueListener listener : queueListeners) {
                log.warn("Recovering node. Adding queue " + queue.toString());
                listener.handleClusterQueuesChanged(queue, QueueListener.QueueChange.Added);
            }
        }

        fromMap.removeAll(fromDBDuplicated);
        for (AndesQueue queue : fromMap) {
            for (QueueListener listener : queueListeners) {
                log.warn("Recovering node. Removing queue " + queue.toString());
                listener.handleClusterQueuesChanged(queue, QueueListener.QueueChange.Deleted);
            }
        }
    }

    private void reloadBindingsFromDB() throws AndesException {
        List<AndesExchange> exchanges = andesContextStore.getAllExchangesStored();
        for (AndesExchange exchange : exchanges) {
            List<AndesBinding> fromDB = andesContextStore.getBindingsStoredForExchange(exchange.exchangeName);
            List<AndesBinding> fromMap = amqpConstructStore.getBindingsForExchange(exchange.exchangeName);
            List<AndesBinding> fromDBDuplicated = new ArrayList<AndesBinding>(fromDB);
            fromDB.removeAll(fromMap);
            for (AndesBinding binding : fromDB) {
                for (BindingListener listener : bindingListeners) {
                    log.warn("Recovering node. Adding binding " + binding.toString());
                    listener.handleClusterBindingsChanged(binding, BindingListener.BindingChange.Added);
                }
            }

            fromMap.removeAll(fromDBDuplicated);
            for (AndesBinding binding : fromMap) {
                for (BindingListener listener : bindingListeners) {
                    log.warn("Recovering node. removing binding " + binding.toString());
                    listener.handleClusterBindingsChanged(binding, BindingListener.BindingChange.Deleted);
                }
            }
        }
    }

    private void reloadSubscriptions() throws AndesException {
        ClusterResourceHolder.getInstance().getSubscriptionManager().reloadSubscriptionsFromStorage();
    }
}
