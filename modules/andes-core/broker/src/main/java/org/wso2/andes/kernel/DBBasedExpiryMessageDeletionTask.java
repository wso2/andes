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
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.slot.SlotManagerClusterMode;
import org.wso2.andes.kernel.slot.SlotManagerStandalone;
import org.wso2.andes.store.FailureObservingStoreManager;
import org.wso2.andes.store.HealthAwareStore;
import org.wso2.andes.store.StoreHealthListener;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

/**
 * This task is responsible to delete the expired messages from the database which were not currently
 * allocated to any slots
 */
public class DBBasedExpiryMessageDeletionTask implements Runnable, StoreHealthListener {

    private static Log log = LogFactory.getLog(SetBasedExpiryMessageDeletionTask.class);

    /**
     * Indicates and provides a barrier if messages stores become offline.
     * marked as volatile since this value could be set from a different thread
     */
    private volatile SettableFuture<Boolean> messageStoresUnavailable;


    /**
     * Used to perform database operations on the context store.
     */
    private AndesContextStore andesContextStore = AndesContext.getInstance().getAndesContextStore();

    private final Integer safetySlotCount;


    public DBBasedExpiryMessageDeletionTask() {

        this.messageStoresUnavailable = null;
        // Register AndesRecoveryTask class as a StoreHealthListener
        FailureObservingStoreManager.registerStoreHealthListener(this);

        safetySlotCount = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_SAFE_DELETE_REGION_SLOT_COUNT);

    }

    @Override
    public void run() {

        boolean isClusterEnabled = AndesContext.getInstance().isClusteringEnabled();

        try {
            /**
             * This logic belongs to an MB run in stand alone mode / the coordinator node run in cluster mode
             */
            if (!isClusterEnabled || (isClusterEnabled && AndesContext.getInstance().getClusterAgent().isCoordinator())) {

                Set<String> queues = andesContextStore.getAllQueues();

                for (String queueName : queues) {
                    //get the upper bound messageID for each unassigned slots as a set for the specific queue
                    TreeSet<Long> messageIDSet = andesContextStore.getMessageIds(queueName);

                    /**
                     * deletion task run only if there are more messages than the safety slot count upper bound
                     * otherwise deletion task is not run
                     * minimum safetySlotCount is 1
                     */
                    if (safetySlotCount >= 1 && messageIDSet.size() >= safetySlotCount) {

                        //set the lower bound Id for safety delete region as the safety slot count interval upper bound id + 1
                        long currentDeletionRangeLowerBoundId = messageIDSet.
                                toArray(new Long[messageIDSet.size()])[safetySlotCount - 1] + 1;

                        //get the expired messages for that queue in the range of message ID starting form the lower bound ID
                        List<AndesMessageMetadata> expiredMessages = MessagingEngine.getInstance()
                                .getExpiredMessages(currentDeletionRangeLowerBoundId, queueName);

                         /*
                          * Checks for the message store availability if its not available
                          * Deletion task needs to await until message store becomes available
                         */
                        if (messageStoresUnavailable != null) {

                            log.info("Message store has become unavailable therefore waiting until store becomes available");
                            //act as a barrier
                            messageStoresUnavailable.get();
                            log.info("Message store became available. Resuming ack handler");
                            messageStoresUnavailable = null; // we are passing the blockade
                            // (therefore clear the it).
                        }

                        if (null != expiredMessages && !expiredMessages.isEmpty()) {

                            //in order to stop the slot delivery worker to this queue in the deletion range
                            SlotManagerClusterMode.setCurrentDeletionQueue(queueName);
                            SlotManagerClusterMode.setCurrentDeletionRangelowerboundID(currentDeletionRangeLowerBoundId);
                            SlotManagerStandalone.setCurrentDeletionQueue(queueName);
                            SlotManagerStandalone.setCurrentDeletionRangelowerboundID(currentDeletionRangeLowerBoundId);

                            //delete message metadata, content from the meta data table, content table and expiry table
                            MessagingEngine.getInstance().deleteMessages(expiredMessages);

                            //since the deletion task is finished that queue no need to hold the slot delivery worker for that queue
                            SlotManagerClusterMode.setCurrentDeletionQueue("");
                            SlotManagerClusterMode.setCurrentDeletionRangelowerboundID(0L);
                            SlotManagerStandalone.setCurrentDeletionQueue("");
                            SlotManagerStandalone.setCurrentDeletionRangelowerboundID(0L);


                            if (log.isDebugEnabled()) {
                                log.debug("Expired message count for queue : " + queueName + "is" + expiredMessages.size());
                            }
                        }
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
