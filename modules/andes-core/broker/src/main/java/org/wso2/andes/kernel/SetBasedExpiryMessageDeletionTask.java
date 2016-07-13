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
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This task is responsible for delete ths expired messages captured from the message flusher
 * Those captured messages accumulated in a set for a batch delete. This task is responsible to do
 * that batch delete
 */
public class SetBasedExpiryMessageDeletionTask implements Runnable, StoreHealthListener {

    /**
     * hold the expired messages detected in the message flusher delivery rule check for batch delete
     */
    private static Set<DeliverableAndesMetadata> expiredMessageSet = new HashSet<>();

    private static Log log = LogFactory.getLog(SetBasedExpiryMessageDeletionTask.class);

    /**
     * Indicates and provides a barrier if messages stores become offline.
     * marked as volatile since this value could be set from a different thread
     */
    private volatile SettableFuture<Boolean> messageStoresUnavailable;

    public SetBasedExpiryMessageDeletionTask(){

        this.messageStoresUnavailable = null;
        // Register AndesRecoveryTask class as a StoreHealthListener
        FailureObservingStoreManager.registerStoreHealthListener(this);

    }

    public static Set<DeliverableAndesMetadata> getExpiredMessageSet() {
        return expiredMessageSet;
    }

    public static void setExpiredMessageSet(Set<DeliverableAndesMetadata> expiredMessageSet) {
        SetBasedExpiryMessageDeletionTask.expiredMessageSet = expiredMessageSet;
    }


    @Override
    public void run() {

        /*
         * Checks for the message store availability if its not available
         * Deletion task needs to await until message store becomes available
         */
        if (messageStoresUnavailable != null) {

            try {
                log.info("Message store has become unavailable therefore waiting until store becomes available");
                //act as a barrier
                messageStoresUnavailable.get();
                log.info("Message store became available. Resuming ack handler");
                messageStoresUnavailable = null; // we are passing the blockade
                // (therefore clear the it).
            } catch (InterruptedException e) {
                log.error("Thread interrupted while waiting for message stores to come online", e);
            } catch (ExecutionException e) {
                log.error("Error occurred while waiting for message stores to come online", e);
            }

        }

        //delete the accumulated messages in the list that are filtered out from the message flusher expiration rule
        //should be run in all the nodes
        if (!getExpiredMessageSet().isEmpty()) {
            try {
                MessagingEngine.getInstance().deleteMessages(new ArrayList<AndesMessageMetadata>(getExpiredMessageSet()));
            } catch (AndesException e) {
                log.error("Error running Message Expiration Checker " + e.getMessage(), e);
            }
        }
    }

    @Override
    public void storeNonOperational(HealthAwareStore store, Exception ex) {

        log.info("Message store became not operational.");
        messageStoresUnavailable = SettableFuture.create();

    }

    @Override
    public void storeOperational(HealthAwareStore store) {

        log.info("Message store became operational.");
        messageStoresUnavailable.set(false);
    }

}
