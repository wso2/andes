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
import org.wso2.andes.kernel.slot.AbstractSlotManager;
import org.wso2.andes.kernel.slot.SlotManagerClusterMode;
import org.wso2.andes.kernel.slot.SlotManagerStandalone;
import org.wso2.andes.store.FailureObservingStoreManager;
import org.wso2.andes.store.HealthAwareStore;
import org.wso2.andes.store.StoreHealthListener;
import org.wso2.andes.tools.utils.MessageTracer;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * PeriodicExpiryMessageDeletionTask is responsible to delete the expired messages from the database which were not currently
 * allocated to any slots. If we delete any messages that are assigned to a slot delivery worker, the
 * content may got deleted when the content is asked to retrieve. This can mess up the message delivery
 * flow. Because of that the messages not allocated to a slot are considered as safe to delete
 */
public class PeriodicExpiryMessageDeletionTask implements Runnable, StoreHealthListener {

    private static Log log = LogFactory.getLog(PeriodicExpiryMessageDeletionTask.class);

    /**
     * Indicates and provides a barrier if messages stores become offline.
     * marked as volatile since this value could be set from a different thread
     */
    protected volatile SettableFuture<Boolean> messageStoresUnavailable;

    /**
     * Used to perform database operations on the context store.
     */
    private AndesContextStore andesContextStore = AndesContext.getInstance().getAndesContextStore();

    /**
     * Holds the slot manager based on broker running mode
     */
    private AbstractSlotManager abstractSlotManagerSlotManager;

    /**
     * Indicate the cluster mode is enabled or not
     */
    protected boolean isClusteringEnabled;

    public PeriodicExpiryMessageDeletionTask() {

        this.messageStoresUnavailable = null;
        // Register AndesRecoveryTask class as a StoreHealthListener
        FailureObservingStoreManager.registerStoreHealthListener(this);

        isClusteringEnabled = AndesContext.getInstance().isClusteringEnabled();
        // Set the appropriate slot manager
        if (isClusteringEnabled) {
            this.abstractSlotManagerSlotManager = SlotManagerClusterMode.getInstance();
        } else {
            this.abstractSlotManagerSlotManager = SlotManagerStandalone.getInstance();
        }

    }

    /**
     * Get the expiry messages queue wise from unallocated region  and delete those from DB
     */
    protected void deleteExpiredMessages(){

        try {
            /**
             * This logic belongs to an MB run in stand alone mode / the coordinator node run in cluster mode
             */
            if (!isClusteringEnabled
                    || (isClusteringEnabled && AndesContext.getInstance().getClusterAgent().isCoordinator())) {

                Set<String> queues = abstractSlotManagerSlotManager.getAllQueues();

                for (String queueName : queues) {

                    long currentDeletionRangeLowerBoundId = abstractSlotManagerSlotManager
                            .getSafeZoneLowerBoundId(queueName);
                    /**
                     * Get the expired messages for that queue in the range of message ID starting form the lower
                     * bound ID. Lower bound id -1 represents that there is no valid region to perform the delete
                     */
                    if(currentDeletionRangeLowerBoundId != -1) {

                        List<Long> expiredMessages = MessagingEngine.getInstance()
                                .getExpiredMessages(currentDeletionRangeLowerBoundId, queueName);

                        /*
                        * Checks for the message store availability if its not available
                        * Deletion task needs to await until message store becomes available
                        */
                        if (null != messageStoresUnavailable) {
                            log.info("Message store has become unavailable therefore expiry message deletion task waiting"

                                    + " until store becomes available");
                            //act as a barrier
                            messageStoresUnavailable.get();
                            log.info("Message store became available. Resuming expiry message deletion task");
                            messageStoresUnavailable = null; // we are passing the blockade
                            // (therefore clear the it).
                        }

                        if ((null != expiredMessages) && (!expiredMessages.isEmpty())) {

                            //Tracing message activity
                            if (MessageTracer.isEnabled()) {
                                for (Long messageId : expiredMessages) {
                                    MessageTracer.trace(messageId, "", MessageTracer
                                            .EXPIRED_MESSAGE_DETECTED_FROM_DATABASE);
                                }
                            }
                            //delete message metadata, content from the meta data table, content table and expiry table
                            MessagingEngine.getInstance().deleteMessagesById(expiredMessages);

                            if (log.isDebugEnabled()) {
                                log.debug("Expired message count for queue : " + queueName + "is" + expiredMessages.size());
                            }
                        }
                        //clear the safe deletion state in the slot manager after deletion completes
                        abstractSlotManagerSlotManager.clearDeletionTaskState();
                    }
                }
            }
            //sleep the message expiration worker for specified amount of time
            // sleepForWaitInterval(workerWaitInterval);
        } catch (AndesException e) {
            log.error("Error running Message Expiration Checker " + e.getMessage(), e);
        } catch (InterruptedException e) {
            log.error("Thread interrupted while waiting for message stores to come online", e);
        } catch (ExecutionException e) {
            log.error("Error occurred while waiting for message stores to come online", e);
        } catch (Throwable e) {
            log.error("Error occurred during the periodic expiry message deletion task", e);
        }

    }

    @Override
    public void run() {
        //delete the expired messages queue wise from safe deletion range
        deleteExpiredMessages();
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
