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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.kernel.OnflightMessageTracker;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * This class is responsible for deleting slots and scheduling slot deletions.
 */
public class SlotDeletionScheduler {

    private long deleteRetryInterval;

    private static Log log = LogFactory.getLog(SlotDeletionScheduler.class);

    /**
     * Create slot deletion scheduler
     *
     * @param deleteRetryInterval interval in milliseconds slot delete re-tries should be
     *                            scheduled.
     */
    SlotDeletionScheduler(long deleteRetryInterval) {
        this.deleteRetryInterval = deleteRetryInterval;
    }

    /**
     * Schedule a slot for deletion. This will continuously try to delete the slot
     * until slot manager informs that the slot is successfully removed
     *
     * @param slotToDelete slot to be removed from cluster
     * @param nodeID       Cluster unique ID of the node
     */
    public void scheduleSlotDeletion(Slot slotToDelete, String nodeID) {
        ScheduledExecutorService deletionTaskScheduler = Executors.newScheduledThreadPool(2);

        SlotDeletionTimedTask slotDeletionTimedTask = new SlotDeletionTimedTask(slotToDelete,nodeID);

        ScheduledFuture deletionTaskHandle = deletionTaskScheduler.scheduleWithFixedDelay(slotDeletionTimedTask, 0, deleteRetryInterval, TimeUnit.MILLISECONDS);

        slotDeletionTimedTask.setTaskHandle(deletionTaskHandle);
    }

    /**
     * SlotDeletionTimedTask class defines a timer task to delete slots
     */
    private class SlotDeletionTimedTask implements Runnable {

        private Slot slotToDelete;
        private String nodeID;
        private ScheduledFuture taskHandle;

        protected SlotDeletionTimedTask(Slot slotToDelete, String nodeID) {
            this.slotToDelete = slotToDelete;
            this.nodeID = nodeID;
        }

        public void run() {
            //perform slot deletion
            boolean deleteSuccess = false;
            if (log.isDebugEnabled()) {
                log.debug("Trying to delete slot : " + slotToDelete);
            }
            try {
                deleteSuccess = MessagingEngine.getInstance().getSlotCoordinator().deleteSlot
                        (slotToDelete.getStorageQueueName(), slotToDelete);
            } catch (ConnectionException e) {
                log.error("Error while trying to delete the slot " + slotToDelete + " Thrift connection failed. Rescheduling delete.");
            }
            if (deleteSuccess) {
                //Terminate the ScheduledExecutor handle. Clear local tracking of slot
                OnflightMessageTracker.getInstance().releaseAllMessagesOfSlotFromTracking(slotToDelete);
                if (log.isDebugEnabled()) {
                    log.debug("Deleted slot of queue : " + slotToDelete.getStorageQueueName() +
                            " : by node : " + nodeID + " | " + slotToDelete);
                }
                taskHandle.cancel(true);
            }
        }

        protected void setTaskHandle(ScheduledFuture taskHandle) {
            this.taskHandle = taskHandle;
        }
    }
}
