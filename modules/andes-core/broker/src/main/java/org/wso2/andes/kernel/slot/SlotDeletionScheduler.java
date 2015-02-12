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
import org.wso2.andes.kernel.OnflightMessageTracker;
import org.wso2.andes.thrift.MBThriftClient;

import java.util.Timer;
import java.util.TimerTask;

public class SlotDeletionScheduler {

    private long deleteRetryInterval;

    private static Log log = LogFactory.getLog(SlotDeletionScheduler.class);


    SlotDeletionScheduler(long deleteRetryInterval) {
        this.deleteRetryInterval = deleteRetryInterval;
    }

    public void scheduleSlotDeletion(Slot slotToDelete, String nodeID) {
        Timer timer = new Timer();
        SlotDeletionTimerTask timerTask = new SlotDeletionTimerTask(timer,slotToDelete,nodeID);
        timer.schedule(timerTask, 0, deleteRetryInterval);
    }

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
            log.info("FIXX : " + "Trying to delete slot : " + slotToDelete);
            try {
                deleteSuccess = MBThriftClient
                        .deleteSlot(slotToDelete.getStorageQueueName(), slotToDelete, nodeID);
            } catch (ConnectionException e) {
                log.error("Error while trying to delete the slot " + slotToDelete + " Thrift connection failed. Rescheduling delete.");
            }
            //Terminate the timer thread
            if(deleteSuccess) {
                //clear local tracking of slot
                OnflightMessageTracker.getInstance().releaseAllMessagesOfSlotFromTracking(slotToDelete);
                timer.cancel();
                log.info("FIXX : " + "Deleted slot of queue : " + slotToDelete.getStorageQueueName() + " : by node : " +
                        "" + nodeID + " | " + slotToDelete);
            }
        }
    }
}
