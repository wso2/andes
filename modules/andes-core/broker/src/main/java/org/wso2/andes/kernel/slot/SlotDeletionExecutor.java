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
import org.wso2.andes.kernel.AndesException;
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

    private int slotCount = 0;

    private SlotDeletionTask slotDeletionTask;

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
        slotDeletionTask = new SlotDeletionTask();
        this.slotDeletionExecutorService.submit(slotDeletionTask);

    }

    /**
     * Slot deletion task running and take slot from queue.
     */
    class SlotDeletionTask implements Runnable {

        //Slot which previously attempt to delete
        Slot previouslyAttemptedSlot = null;

        void setLive(boolean live) {
            isLive = live;
        }

        boolean isLive = true;


        /**
         * Running slot deletion task
         */
        public void run() {
            while (isLive) {
                try {
                    //Slot to attempt current deletion
                    Slot slot;
//                    if (previouslyAttemptedSlot != null) {
//                        //Previous attempt to deletion is not success. Therefore try again to delete by assign it to
//                        //deletionAttempt
//                        slot = previouslyAttemptedSlot;
//                        previouslyAttemptedSlot = null;
//                    } else {
                        //Previous attempt to delete slot is success, therefore taking next slot from queue
                        slot = slotsToDelete.poll(1, TimeUnit.SECONDS);
//                    }
                    //check current slot to delete is not null
                    if (slot != null) {

                        // Check DB for any remaining messages. (JIRA FIX: MB-1612)
                        // If there are any remaining messages wait till overlapped slot delivers the messages
                        if (MessagingEngine.getInstance().getMessageCountForQueueInRange(
                                slot.getStorageQueueName(), slot.getStartMessageId(), slot.getEndMessageId()) == 0) {
                            //invoke coordinator to delete slot
                            boolean deleteSuccess = deleteSlotAtCoordinator(slot);
                            if (!deleteSuccess) {
                                //delete attempt not success, therefore reassign current deletion attempted slot to previous slot
//                                previouslyAttemptedSlot = slot;

                                slotsToDelete.put(slot);
                            } else {
                                SlotDeliveryWorker slotWorker = SlotDeliveryWorkerManager.getInstance()
                                        .getSlotWorker(slot.getStorageQueueName());
                                slotWorker.deleteSlot(slot);
                                log.warn("ASITHA Slot Delete Success from node : " + slot);
                            }
                        } else {
                            log.warn("ASITHA Could not Delete slot because db is not empty !");
                            slotsToDelete.put(slot);
                        }
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Error while trying to delete the slot.", e);
                } catch (AndesException e) {
                    log.error("Error occurred while trying to delete slot", e);
                } catch (Throwable throwable){

                    log.fatal("ASITHA SlotDeletionExecutor occurred a throwable", throwable);

                }finally {
                    if (slotCount % 200 == 0) {
                        log.warn("ASITHA SLOT COUNT AT EXECUTOR : " + slotsToDelete.size());
                    }
                    slotCount++;
                }
            }
            log.fatal("=======================ASITHA SlotDeletionExecutor STOPPED WORKING WITH slots to delete: " + slotsToDelete.size());
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
        log.warn("ASITHA Slot Scheduled for Deletion : " + slot);
        slotsToDelete.add(slot);

    }

    /**
     * Shutdown slot deletion executor service
     */
    public void stopSlotDeletionExecutor() {
        if (slotDeletionExecutorService != null) {
            slotDeletionTask.setLive(false);;
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
