/*
 * Copyright (c) 2017, WSO2 Inc. (http://wso2.com) All Rights Reserved.
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

package org.wso2.andes.kernel.slot;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.kernel.subscription.StorageQueue;

import java.util.UUID;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Task executing the slot delete. It will poll on slots scheduled to delete and
 * coordinate with slot coordinator in cluster to delete slots.
 */
public class SlotDeletingTask implements Runnable {

    private static Log log = LogFactory.getLog(SlotDeletingTask.class);

    /**
     * Unique ID of the task
     */
    private UUID id;

    /**
     * Condition running the task.
     */
    private volatile boolean isLive = true;

    /**
     * Queue to keep slots to be removed. Reason to use a queue is
     * if a slot could not be removed, it is definite later slots cannot be removed
     * as well (because of safe-zone)
     */
    private LinkedBlockingDeque<Slot> slotsToDelete;

    /**
     * Maximum number of slots to keep scheduled for deletion before
     * raising a WARN
     */
    private int maxNumberOfPendingSlots;

    /**
     * Specifically disable slot deleting task. At start up task
     * is enabled by default. Once disabled cannot enable again.
     */
    void stop() {
        isLive = false;
    }

    /**
     * Create an instance of SlotDeletingTask
     *
     * @param maxNumberOfPendingSlots max number of pending slots to be deleted (used to raise a WARN)
     */
    public SlotDeletingTask(int maxNumberOfPendingSlots) {
        this.id = UUID.randomUUID();
        this.maxNumberOfPendingSlots = maxNumberOfPendingSlots;
        this.slotsToDelete = new LinkedBlockingDeque<>();
    }

    @Override
    public void run() {
        while (isLive) {
            try {
                // Slot to attempt current deletion. This call will block if there is no slot
                Slot slot = slotsToDelete.take();

                if (log.isDebugEnabled()) {
                    log.debug("SlotDeletingTask id= " + id + " trying to delete slot " + slot.toString());
                }
                // Check DB for any remaining messages. (JIRA FIX: MB-1612)
                // If there are any remaining messages wait till overlapped slot delivers the messages
                if (MessagingEngine.getInstance().getMessageCountForQueueInRange(
                        slot.getStorageQueueName(), slot.getStartMessageId(), slot.getEndMessageId()) == 0) {
                    // Invoke coordinator to delete slot
                    boolean deleteSuccess = deleteSlotAtCoordinator(slot);
                    if (!deleteSuccess) {
                        // Delete attempt not success, therefore adding slot to the head of queue
                        slotsToDelete.addFirst(slot);
                        if (log.isDebugEnabled()) {
                            log.debug("SlotDeletingTask id= " + id + " could not agree with "
                                    + "coordinator to delete slot " + slot.toString());
                        }
                        //here try again after a second time delay (let coordinator update safe zone)
                        //TODO: is there other good way to fix this? We need to delete in the rate we read slots
                        TimeUnit.SECONDS.sleep(1);
                    } else {
                        StorageQueue storageQueue = slot.getStorageQueue();
                        storageQueue.deleteSlot(slot);
                        if (log.isDebugEnabled()) {
                            log.debug("SlotDeletingTask id= " + id + " deleted slot " + slot.toString());
                        }
                    }
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("SlotDeletingTask id= " + id + " could not deleted slot "
                                + slot.toString() + " as there are messages in range of slot");
                    }
                    slotsToDelete.put(slot); // Not deleted. Hence putting back to tail of queue
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("SlotDeletionTask was interrupted while trying to delete the slot.", e);
            } catch (Throwable throwable) {
                log.error("Unexpected error occurred while trying to delete the slot.", throwable);
            }
        }

        log.info("SlotDeletingTask " + id + " has shutdown with " + slotsToDelete.size() + " slots to delete.");
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
                    "Rescheduling delete.", e);

        }
        return deleteSuccess;
    }

    /**
     * Schedule a slot for deletion. This will continuously try to delete the slot
     * until slot manager informs that the slot is successfully removed
     *
     * @param slot slot to be removed from cluster
     */
    public void scheduleToDelete(Slot slot) {
        int currentNumberOfSlotsToDelete = slotsToDelete.size();
        if (currentNumberOfSlotsToDelete > maxNumberOfPendingSlots) {
            log.warn("Too many slots submitted to delete. Consider increasing <deleteTaskCount> "
                    + "and <thriftClientPoolSize> parameters. Current submit value = "
                    + currentNumberOfSlotsToDelete + " safe zone value = "
                    + SlotMessageCounter.getInstance().getCurrentNodeSafeZoneId());
        }
        slotsToDelete.add(slot);

    }
}
