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
import org.wso2.andes.kernel.disruptor.inbound.InboundDBSyncRequestEvent;
import org.wso2.andes.kernel.disruptor.inbound.InboundEventManager;
import org.wso2.andes.kernel.subscription.AndesSubscriptionManager;
import org.wso2.andes.store.FailureObservingStoreManager;
import org.wso2.andes.store.HealthAwareStore;
import org.wso2.andes.store.StoreHealthListener;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This task will periodically load exchanges,queues,bindings,subscriptions from database
 * and simulate cluster notifications. This is implemented to bring the node
 * to the current state of cluster in case some hazlecast notifications are missed
 */
public class AndesRecoveryTask implements Runnable, StoreHealthListener {

	private AndesContextStore andesContextStore;
	private AMQPConstructStore amqpConstructStore;
	private AndesContextInformationManager contextInformationManager;
    private AndesSubscriptionManager subscriptionManager;
    private InboundEventManager inboundEventManager;
    private AtomicBoolean isRunning;

	// set storeOperational to true since it can be assumed that the store is operational at startup
	// if it is non-operational, the value will be updated immediately
	AtomicBoolean isStoreOperational = new AtomicBoolean(true);

	private static final Log log = LogFactory.getLog(AndesRecoveryTask.class);

    /**
     * Create a AndesRecoveryTask instance. For the broker, only one task
     * should be running.
     *
     * @param contextInformationManager manager for updating message routers,
     *                                  queues and bindings
     * @param inboundEventManager       inbound event manager for publishing events to disruptor
     */
    public AndesRecoveryTask(AndesContextInformationManager contextInformationManager, AndesSubscriptionManager
            subscriptionManager, InboundEventManager inboundEventManager) {
        // Register AndesRecoveryTask class as a StoreHealthListener
        FailureObservingStoreManager.registerStoreHealthListener(this);
        this.andesContextStore = AndesContext.getInstance().getAndesContextStore();
        this.amqpConstructStore = AndesContext.getInstance().getAMQPConstructStore();
        this.contextInformationManager = contextInformationManager;
        this.subscriptionManager = subscriptionManager;
        this.inboundEventManager = inboundEventManager;
        this.isRunning = new AtomicBoolean(false);
    }

	@Override
	public void run() {

        if(!isRunning.compareAndSet(false, true)) {
            return;
        }
        try {
            if (isStoreOperational.get()) {
                recoverBrokerArtifacts();
            } else {
                log.warn("AndesRecoveryTask was paused due to non-operational context store.");
            }
        } catch (Throwable e) {
            log.error("Error in running andes recovery task", e);
        } finally {
            isRunning.set(false);
        }
    }

    /**
     * Run Andes recovery task. This will sync message routers, queues and bindings
     * in memory to what in persistent store.
     */
    public void executeNow() {
        run();
    }

	/**
	 * Reload and recover Message Routers,
	 * Queues, Bindings and Subscriptions
	 *
	 * @throws AndesException
	 */
	public void recoverBrokerArtifacts() throws AndesException {
		if (isStoreOperational.get()) {
            InboundDBSyncRequestEvent dbSyncRequestEvent = new InboundDBSyncRequestEvent();
            dbSyncRequestEvent.prepareEvent(andesContextStore, amqpConstructStore,
                    contextInformationManager, subscriptionManager);
            inboundEventManager.publishStateEvent(dbSyncRequestEvent);
		} else {
			log.warn("AndesRecoveryTask was paused due to non-operational context store.");
		}
	}



	/**
	 * Invoked when specified store becomes non-operational
	 *
	 * @param store the store which went offline.
	 * @param ex    exception
	 */
	@Override
	public void storeNonOperational(HealthAwareStore store, Exception ex) {
		isStoreOperational.set(false);
		log.info("AndesRecoveryTask paused due to non-operational context store.");
	}

	/**
	 * Invoked when specified store becomes operational
	 *
	 * @param store Reference to the operational store
	 */
	@Override
	public void storeOperational(HealthAwareStore store) {
		isStoreOperational.set(true);
		log.info("AndesRecoveryTask became operational. Recovering broker artifacts.");
		try {
			recoverBrokerArtifacts();
		} catch (AndesException e) {
			log.error("Error in running andes recovery task", e);
		}
	}
}
