/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.andes.kernel;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.store.FailureObservingStoreManager;
import org.wso2.andes.store.HealthAwareStore;
import org.wso2.andes.store.StoreHealthListener;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * This task is responsible for delete ths expired messages captured from the message flusher.
 * Those captured messages accumulated in a set for a batch delete. This task is responsible to do
 * that batch delete.
 */
public class PreDeliveryExpiryMessageDeletionTask implements Runnable, StoreHealthListener {

    /**
     * Holds expired messages detected in the message flusher delivery rule check for batch delete
     */
    private BlockingDeque<Long> expiredMessageIds;

    private static Log log = LogFactory.getLog(PreDeliveryExpiryMessageDeletionTask.class);

    /**
     * Indicates and provides a barrier if messages stores become offline.
     * marked as volatile since this value could be set from a different thread
     */
    private volatile SettableFuture<Boolean> messageStoresUnavailable;

    public PreDeliveryExpiryMessageDeletionTask(){

        this.messageStoresUnavailable = null;
        this.expiredMessageIds = new LinkedBlockingDeque<>();
        // Register AndesRecoveryTask class as a StoreHealthListener
        FailureObservingStoreManager.registerStoreHealthListener(this);

    }

    public void addMessageIdToExpiredQueue(long messageId){
        expiredMessageIds.add(messageId);
    }

    @Override
    public void run() {
        // Checks for the message store availability.
        // if its not available deletion task needs to await until message store becomes available
        if (null != messageStoresUnavailable) {

            try {
                log.info("Message store has become unavailable therefore expiry message deletion task is"
                        + "waiting until store becomes available");
                //act as a barrier
                messageStoresUnavailable.get();
                log.info("Message store became available. Resuming expiry message deletion task");
                messageStoresUnavailable = null; // we are passing the blockade
                // (therefore clear the it).
            } catch (InterruptedException e) {
                log.error("Thread interrupted while waiting for message stores to come online", e);
            } catch (ExecutionException e) {
                log.error("Error occurred while waiting for message stores to come online", e);
            } catch (Throwable e) {
                log.error("Error occurred during the pre delivery expiry message deletion task", e);
            }

        }

        //delete the accumulated messages in the list that are filtered out from the message flusher expiration rule
        //should be run in all the nodes
        if (!expiredMessageIds.isEmpty()) {
            try {
                List<Long> expiredMessageIdList = new ArrayList<>();
                //removes the messages from the queue and add them into a list
                expiredMessageIds.drainTo(expiredMessageIdList);
                //delete the messages from the store
                MessagingEngine.getInstance().deleteMessagesById(expiredMessageIdList);
            } catch (AndesException e) {
                log.error("Error running message expiration checker ", e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeNonOperational(HealthAwareStore store, Exception ex) {
        log.warn("Message store became not operational.");
        messageStoresUnavailable = SettableFuture.create();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeOperational(HealthAwareStore store) {
        log.info("Message store became operational.");
        messageStoresUnavailable.set(false);
    }

}
