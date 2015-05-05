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

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.kernel.OnflightMessageTracker;

import java.util.Timer;
import java.util.TimerTask;

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
        Timer timer = new Timer();
        SlotDeletionTimerTask timerTask = new SlotDeletionTimerTask(timer, slotToDelete, nodeID);
        timer.schedule(timerTask, 0, deleteRetryInterval);
    }

    /**
     * SlotDeletionTimerTask class defines a timer task to delete slots
     */
    private class SlotDeletionTimerTask extends TimerTask {

        private Timer timer;
        private Slot slotToDelete;
        private String nodeID;

        public SlotDeletionTimerTask(Timer timer, Slot slotToDelete, String nodeID) {
            this.timer = timer;
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
            } catch (HazelcastInstanceNotActiveException ignore) {
                //This exception occurred in situation like gracefully shutting down i.e. Hazelcast instance already
                // deactivated and schedule task trying to delete slot after that.
                //Therefore silently ignore exception
            }
            if (deleteSuccess) {
                //Terminate the timer thread. Clear local tracking of slot
                OnflightMessageTracker.getInstance().releaseAllMessagesOfSlotFromTracking(slotToDelete);
                timer.cancel();
                if (log.isDebugEnabled()) {
                    log.debug("Deleted slot of queue : " + slotToDelete.getStorageQueueName() +
                            " : by node : " + nodeID + " | " + slotToDelete);
                }
            }
        }
    }
}
