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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.mail.event.StoreListener;

/**
 * 
 * This class keeps track of all the {@link StoreListener}s and notifies when
 * store becomes (non-)operational.
 * 
 */
public class FailureObservingStoreManager {


    /**
     * All the {@link StoreHealthListener} implementations registered to receive
     * callbacks.
     */
    private static Collection<StoreHealthListener> healthListeners =
                                              Collections.synchronizedCollection(new ArrayList<StoreHealthListener>());

    /**
     * A named thread factory build executor.
     */
    private final ThreadFactory namedThreadFactory =
                                  new ThreadFactoryBuilder().setNameFormat("FailureObservingStores-HealthCheckPool")
                                                                                      .build();
    /**
     * Executor used for health checking
     */
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(2, namedThreadFactory);

    private AtomicBoolean isStoreAvailable = new AtomicBoolean(true);
    private AtomicBoolean isContextStoreAvailable = new AtomicBoolean(true);
    private AtomicBoolean isMessageStoreAvailable = new AtomicBoolean(true);
    
    /**
     * Registers specified {@link StoreHealthListener} to receive events.
     * @param healthListener {@link StoreHealthListener}
     */
    public static void registerStoreHealthListener(StoreHealthListener healthListener){
        healthListeners.add(healthListener);
    }
    
    /**
     * Schedules a health check task for a non-operational for a {@link HealthAwareStore}
     * Note: visibility is at package level which is intentional.
     * @param store which became in-operational
     * @return a future referring to the periodic health check task.
     */
    ScheduledFuture<?> scheduleHealthCheckTask(HealthAwareStore store) {
        
        int taskDelay = (Integer) AndesConfigurationManager.readValue(
                                    AndesConfiguration.PERSISTENCE_STORE_HEALTH_CHECK_INTERVAL);
        //initial delay and period interval is set to same for the sake of simplicity
        StoreHealthCheckTask healthCheckTask = new StoreHealthCheckTask(store, healthListeners);
        return executor.scheduleWithFixedDelay(healthCheckTask, taskDelay,
                                               taskDelay, TimeUnit.SECONDS);
   }

    /**
     * A utility method to broadcast that a store became non-operational.
     *
     * @param exception        error occurred
     * @param healthAwareStore in-operational store
     */
    public void notifyContextStoreNonOperational(AndesStoreUnavailableException exception, HealthAwareStore
            healthAwareStore) {
        isContextStoreAvailable.compareAndSet(true, false);
        if (isStoreAvailable.compareAndSet(true, false)) {
            notifyStoreNonOperational(exception, healthAwareStore);
        }
    }

    /**
     * Method to notify all the listeners that the context/message store became non-operational.
     *
     * @param exception        the exception occurred
     * @param healthAwareStore the store that became non-operational
     */
    private void notifyStoreNonOperational(AndesStoreUnavailableException exception, HealthAwareStore
            healthAwareStore) {
        ThreadFactory namedNotifierThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("StoreFailure-NotifierPool").build();
        ScheduledExecutorService listenerNotifyingExecutor
                = Executors.newScheduledThreadPool(healthListeners.size(), namedNotifierThreadFactory);
        for (StoreHealthListener listener : healthListeners) {
            StoreFailureNotifier notifier = new StoreFailureNotifier(healthAwareStore, exception, listener);
            listenerNotifyingExecutor.schedule(notifier, 0, TimeUnit.SECONDS);
        }
    }

    /**
     * A utility method to broadcast that a store became non-operational.
     *
     * @param exception        error occurred
     * @param healthAwareStore in-operational store
     */
    public void notifyMessageStoreNonOperational(AndesStoreUnavailableException exception, HealthAwareStore
            healthAwareStore) {
        isMessageStoreAvailable.compareAndSet(true, false);
        if (isStoreAvailable.compareAndSet(true, false)) {
            notifyStoreNonOperational(exception, healthAwareStore);
        }
    }

    /**
     * A utility method to broadcast that a store became operational.
     *
     * @param healthAwareStore the store which became operational.
     */
    private void notifyStoreOperational(HealthAwareStore healthAwareStore) {
        if (isContextStoreAvailable.get() && isMessageStoreAvailable.get()) {
            if (isStoreAvailable.compareAndSet(false, true)) {
                for (StoreHealthListener listener : healthListeners) {
                    listener.storeOperational(healthAwareStore);
                }
            }
        }
    }

    /**
     * A utility method to broadcast that a store became operational.
     *
     * @param healthAwareStore the store which became operational.
     */
    public void notifyContextStoreOperational(HealthAwareStore healthAwareStore) {
        if (isContextStoreAvailable.compareAndSet(false, true)) {
            notifyStoreOperational(healthAwareStore);
        }
    }

    /**
     * A utility method to broadcast that a store became operational.
     *
     * @param healthAwareStore the store which became operational.
     */
    public void notifyMessageStoreOperational(HealthAwareStore healthAwareStore) {
        if (isMessageStoreAvailable.compareAndSet(false, true)) {
            notifyStoreOperational(healthAwareStore);
        }
    }

    /**
     * Stop the scheduled tasks which are checking for message stores availability.
     */
    void close(){
        executor.shutdown();
    }

    private class StoreFailureNotifier extends Thread {

        /**
         * The store which became operational.
         */
        private HealthAwareStore healthAwareStore;

        /**
         * The store failure exception that was thrown.
         */
        private Exception exception;

        /**
         * The listener to which store failures will be notified.
         */
        private StoreHealthListener listener;

        public StoreFailureNotifier(HealthAwareStore store, Exception exception,
                StoreHealthListener storeHealthListener) {
            healthAwareStore = store;
            this.exception = exception;
            listener = storeHealthListener;
        }

        @Override
        public void run() {
            listener.storeNonOperational(healthAwareStore, exception);
        }
    }
}
