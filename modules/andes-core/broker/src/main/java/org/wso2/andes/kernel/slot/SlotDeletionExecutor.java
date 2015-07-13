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

package org.wso2.andes.kernel.slot;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.MessagingEngine;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * This class is responsible for deleting slots and scheduling slot deletions.
 */
public class SlotDeletionExecutor {

    private static Log log = LogFactory.getLog(SlotDeletionExecutor.class);


    private LinkedBlockingQueue<Slot> slotsToDelete = new LinkedBlockingQueue<Slot>();

    /**
     * Slot deletion thread factory in one MB node
     */
    private static ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat
            ("SlotDeletionExecutor-%d").build();

    /**
     * Slot deletion executor service
     */
    private ExecutorService slotDeletionExecutorService;

    /**
     * SlotDeletionScheduler instance
     */
    private static SlotDeletionExecutor instance;

    /**
     * SlotDeletionExecutor constructor
     */
    private SlotDeletionExecutor() {

    }

    /**
     * Create slot deletion scheduler
     */
    public void init() {
        this.slotDeletionExecutorService = Executors.newSingleThreadExecutor(namedThreadFactory);
        this.slotDeletionExecutorService.submit(new SlotDeletionTask());

    }

    /**
     * Slot deletion task running and take slot from queue.
     */
    class SlotDeletionTask implements Runnable {

        //Slot which previously attempt to delete
        Slot previouslyAttemptedSlot = null;

        /**
         * Running slot deletion task
         */
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    //Slot to attempt current deletion
                    Slot deletionAttempt;
                    if (previouslyAttemptedSlot != null) {
                        //Previous attempt to deletion is not success. Therefore try again to delete by assign it to
                        //deletionAttempt
                        deletionAttempt = previouslyAttemptedSlot;
                        previouslyAttemptedSlot = null;
                    } else {
                        //Previous attempt to delete slot is success, therefore taking next slot from queue
                        deletionAttempt = slotsToDelete.poll(1, TimeUnit.SECONDS);
                    }
                    //check current slot to delete is not null
                    if (deletionAttempt != null) {
                        //invoke coordinator to delete slot
                        boolean deleteSuccess = deleteSlotAtCoordinator(deletionAttempt);
                        if (!deleteSuccess) {
                            //delete attempt not success, therefore reassign current deletion attempted slot to previous slot
                            previouslyAttemptedSlot = deletionAttempt;
                        }
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Error while trying to delete the slot.");
                }
            }
        }

        /**
         * Delete slot at coordinator and return delete status
         *
         * @param slot slot to be removed from cluster
         * @return slot deletion status
         */
        private boolean deleteSlotAtCoordinator(Slot slot) {
            boolean deleteSuccess = false;
            try {
                deleteSuccess = MessagingEngine.getInstance().getSlotCoordinator().deleteSlot
                        (slot.getStorageQueueName(), slot);
            } catch (ConnectionException e) {
                log.error("Error while trying to delete the slot " + slot + " Thrift connection failed. " +
                        "Rescheduling delete.");

            }
            return deleteSuccess;
        }
    }

    /**
     * Schedule a slot for deletion. This will continuously try to delete the slot
     * until slot manager informs that the slot is successfully removed
     *
     * @param slot slot to be removed from cluster
     */
    public void executeSlotDeletion(Slot slot) {
        slotsToDelete.add(slot);

    }

    /**
     * Shutdown slot deletion executor service
     */
    public void stopSlotDeletionExecutor() {
        if (slotDeletionExecutorService != null) {
            slotDeletionExecutorService.shutdown();
        }
    }

    /**
     * Return slot deletion scheduler object
     *
     * @return SlotDeletionScheduler object
     */
    public static SlotDeletionExecutor getInstance() {
        if (instance == null) {
            instance = new SlotDeletionExecutor();
        }
        return instance;
    }

}
