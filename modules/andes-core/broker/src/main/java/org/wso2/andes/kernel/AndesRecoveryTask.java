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
import org.wso2.andes.store.HealthAwareStore;
import org.wso2.andes.store.StoreHealthListener;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This task will periodically load exchanges,queues,bindings,subscriptions from database
 * and simulate cluster notifications. This is implemented to bring the node
 * to the current state of cluster in case some hazlecast notifications are missed
 */
public class AndesRecoveryTask implements Runnable, StoreHealthListener {

    private List<QueueListener> queueListeners = new ArrayList<QueueListener>();
    private List<ExchangeListener> exchangeListeners = new ArrayList<ExchangeListener>();
    private List<BindingListener> bindingListeners = new ArrayList<BindingListener>();

    AndesContextStore andesContextStore;
    AMQPConstructStore amqpConstructStore;

	AtomicBoolean isContextStoreOperational = new AtomicBoolean(true);

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
	    if(isContextStoreOperational.compareAndSet(true,true)) {
		    try {
			    reloadExchangesFromDB();
			    reloadQueuesFromDB();
			    reloadBindingsFromDB();
			    reloadSubscriptions();
		    } catch (AndesException e) {
			    log.error("Error in running andes recovery task", e);
		    }
	    } else {
		    log.info("AndesRecoveryTask was paused due to non-operational context store.");
	    }
    }

    /**
     * reload and recover exchanges,
     * Queues, Bindings and Subscriptions
     *
     * @throws AndesException
     */
    public void recoverExchangesQueuesBindingsSubscriptions() throws AndesException {
	    if(isContextStoreOperational.compareAndSet(true,true)) {
		    reloadExchangesFromDB();
		    reloadQueuesFromDB();
		    reloadBindingsFromDB();
		    reloadSubscriptions();
	    } else {
		    log.info("AndesRecoveryTask was paused due to non-operational context store.");
	    }
    }

    private void reloadExchangesFromDB() throws AndesException {
	    if(isContextStoreOperational.compareAndSet(true,true)) {
		    List<AndesExchange> fromDB = andesContextStore.getAllExchangesStored();
		    List<AndesExchange> fromMap = amqpConstructStore.getExchanges();
		    List<AndesExchange> fromDBDuplicated = new ArrayList<AndesExchange>(fromDB);

		    fromDB.removeAll(fromMap);
		    for (AndesExchange exchange : fromDB) {
			    for (ExchangeListener listener : exchangeListeners) {
				    log.warn("Recovering node. Adding exchange " + exchange.toString());
				    listener.handleClusterExchangesChanged(exchange,
				                                           ExchangeListener.ExchangeChange.Added);
			    }
		    }

		    fromMap.removeAll(fromDBDuplicated);
		    for (AndesExchange exchange : fromMap) {
			    for (ExchangeListener listener : exchangeListeners) {
				    log.warn("Recovering node. Removing exchange " + exchange.toString());
				    listener.handleClusterExchangesChanged(exchange,
				                                           ExchangeListener.ExchangeChange.Deleted);
			    }
		    }
	    } else {
		    log.info("Failed to recover exchanges from database" +
		             " due to non-operational context store.");
	    }
    }

    private void reloadQueuesFromDB() throws AndesException {
	    if(isContextStoreOperational.compareAndSet(true,true)) {
		    List<AndesQueue> fromDB = andesContextStore.getAllQueuesStored();
		    List<AndesQueue> fromMap = amqpConstructStore.getQueues();
		    List<AndesQueue> fromDBDuplicated = new ArrayList<AndesQueue>(fromDB);

		    fromDB.removeAll(fromMap);
		    for (AndesQueue queue : fromDB) {
			    for (QueueListener listener : queueListeners) {
				    log.warn("Recovering node. Adding queue " + queue.toString());
				    listener.handleClusterQueuesChanged(queue, QueueListener.QueueEvent.ADDED);
			    }
		    }

		    fromMap.removeAll(fromDBDuplicated);
		    for (AndesQueue queue : fromMap) {
			    for (QueueListener listener : queueListeners) {
				    log.warn("Recovering node. Removing queue " + queue.toString());
				    listener.handleClusterQueuesChanged(queue, QueueListener.QueueEvent.DELETED);
			    }
		    }
	    } else {
		    log.info("Failed to recover queues from database" +
		             " due to non-operational context store.");
	    }
    }

    private void reloadBindingsFromDB() throws AndesException {
	    if(isContextStoreOperational.compareAndSet(true,true)) {
		    List<AndesExchange> exchanges = andesContextStore.getAllExchangesStored();
		    for (AndesExchange exchange : exchanges) {
			    List<AndesBinding> fromDB =
					    andesContextStore.getBindingsStoredForExchange(exchange.exchangeName);
			    List<AndesBinding> fromMap =
					    amqpConstructStore.getBindingsForExchange(exchange.exchangeName);
			    List<AndesBinding> fromDBDuplicated = new ArrayList<AndesBinding>(fromDB);
			    fromDB.removeAll(fromMap);
			    for (AndesBinding binding : fromDB) {
				    for (BindingListener listener : bindingListeners) {
					    log.warn("Recovering node. Adding binding " + binding.toString());
					    listener.handleClusterBindingsChanged(binding,
					                                          BindingListener.BindingEvent.ADDED);
				    }
			    }

			    fromMap.removeAll(fromDBDuplicated);
			    for (AndesBinding binding : fromMap) {
				    for (BindingListener listener : bindingListeners) {
					    log.warn("Recovering node. removing binding " + binding.toString());
					    listener.handleClusterBindingsChanged(binding,
					                                          BindingListener.BindingEvent.DELETED);
				    }
			    }
		    }
	    } else {
		    log.info("Failed to recover bindings from database" +
		             " due to non-operational context store.");
	    }
    }

    private void reloadSubscriptions() throws AndesException {
	    if(isContextStoreOperational.compareAndSet(true,true)) {
		    ClusterResourceHolder.getInstance().getSubscriptionManager()
		                         .reloadSubscriptionsFromStorage();
	    } else {
		    log.info("Failed to recover subscriptions from database" +
		             " due to non-operational context store.");
	    }
    }

	/**
	 * Invoked when specified store becomes non-operational
	 *
	 * @param store the store which went offline.
	 * @param ex exception
	 */
	@Override public void storeNonOperational(HealthAwareStore store, Exception ex) {
		if(store.getClass().isInstance(AndesContextStore.class)) {
			isContextStoreOperational.set(false);
		}
	}

	/**
	 * Invoked when specified store becomes operational
	 *
	 * @param store Reference to the operational store
	 */
	@Override public void storeOperational(HealthAwareStore store) {
		if(store.getClass().isInstance(AndesContextStore.class)) {
			isContextStoreOperational.set(true);
		}
	}
}
