/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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


import org.apache.log4j.Logger;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class FailureObservingStore<T extends HealthAwareStore> implements HealthAwareStore {

    private static final Logger log = Logger.getLogger(FailureObservingStore.class);
    /**
     * Variable that holds the operational status of the context store. True if store is operational.
     */
    private AtomicBoolean isStoreAvailable;

    /**
     * Future referring to a scheduled task which check the connectivity to the
     * store.
     * Used to cancel the periodic task after store becomes operational.
     */
    private ScheduledFuture<?> storeHealthDetectingFuture;

    /**
     * {@link FailureObservingStoreManager} to notify about store's operational status.
     */
    protected FailureObservingStoreManager failureObservingStoreManager;

    protected T wrappedInstance;

    FailureObservingStore(T wrappedInstance, FailureObservingStoreManager storeManager) {
        this.wrappedInstance = wrappedInstance;
        this.failureObservingStoreManager = storeManager;
        isStoreAvailable = new AtomicBoolean(true);
    }

    @Override
    public boolean isOperational(String testString, long testTime) {
        if (wrappedInstance.isOperational(testString, testTime)) {
            isStoreAvailable.set(true);
            if (storeHealthDetectingFuture != null) {
                // we have detected that store is operational therefore
                // we don't need to run the periodic task to check weather store
                // is available.
                storeHealthDetectingFuture.cancel(false);
                storeHealthDetectingFuture = null;
            }
            failureObservingStoreManager.notifyMessageStoreOperational(this);
            return true;
        }
        return false;
    }

    /**
     * A convenient method to notify all {@link StoreHealthListener}s that
     * context store became offline
     *
     * @param e the exception occurred.
     */
    protected void notifyFailures(AndesStoreUnavailableException e) {

        if (isStoreAvailable.compareAndSet(true,false)){
            log.warn("Message store became non-operational");
            failureObservingStoreManager.notifyMessageStoreNonOperational(e, wrappedInstance);
            storeHealthDetectingFuture = failureObservingStoreManager.scheduleHealthCheckTask(this);
        }
    }

    protected T getWrappedInstance() {
        return wrappedInstance;
    }
}
