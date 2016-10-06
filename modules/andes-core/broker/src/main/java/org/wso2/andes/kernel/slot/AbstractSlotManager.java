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

package org.wso2.andes.kernel.slot;

import org.apache.commons.lang.StringUtils;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesException;

import java.util.List;
import java.util.Set;

/**
 * AbstractSlotManager holds the common responsibilities for the standalone slot manager and cluster mode slot manager.
 */
public abstract class AbstractSlotManager {

    /**
     * To keep the name of queue on which deletion task is currently running
     */
    protected String currentDeletionQueue = StringUtils.EMPTY;

    /**
     * To keep the lower bound Id of the current expiry message deletion range
     */
    protected long currentDeletionRangeLowerBoundId;

    /**
     *  The slot gap need to be maintained as not reachable for deletion task as they may be allocated
     *  to a slot delivery worker in the near future
     */
    protected int safetySlotCount;

    public AbstractSlotManager() {
        this.safetySlotCount = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_SAFE_DELETE_REGION_SLOT_COUNT);
    }

    /**
     * Get the lower bound id of safe deletion range.
     *
     * @return lower bound id
     */
    public abstract long getSafeZoneLowerBoundId(String queueName) throws AndesException;

    /**
     * Delete all slot associations with a given queue. This is required to handle a queue purge event.
     *
     * @param queueName name of destination queue
     */
    public  abstract  void clearAllActiveSlotRelationsToQueue(String queueName)throws AndesException;

    /**
     * Get all Queues.
     *
     * @return set of queues
     * @throws AndesException
     */
    public abstract Set<String> getAllQueues() throws AndesException;

    /**
     * Set the deletion task's current working range and queue.
     *
     * @param currentDeletionQueueName queue name on which deletion currently happens
     * @param currentDeletionRangeLowerBoundId lower bound Id of deletion range
     */
    public void setDeletionTaskState(String currentDeletionQueueName, long currentDeletionRangeLowerBoundId) {
        this.currentDeletionQueue = currentDeletionQueueName;
        this.currentDeletionRangeLowerBoundId = currentDeletionRangeLowerBoundId;
    }

    /**
     * Clear the state of current deletion range.
     */
    public void clearDeletionTaskState() {
        this.currentDeletionQueue = StringUtils.EMPTY;
        this.currentDeletionRangeLowerBoundId = 0L;
    }

    /**
     * Check the slot which is currently demanded is in the safe deletion zone.
     *
     * @param queueName queue name for which slot is asked
     * @param lastMessageId potential last message id for the current slot
     * @return whether this slot delivery is safe or not
     */
    protected boolean isSafeToDeliverSlots(String queueName, Long lastMessageId) {
        boolean isSafeToDeliverSlots = true;
        // The slot allocation is not safe only if the request is coincides with the queue in which current deletion
        // task is running and message id range is in the current deletion range.
        // If last message id is null then there is no new messages.
        // So Deletion task will not run.An empty slot will be given to Slot delivery worker.
        if ((null != lastMessageId) && currentDeletionQueue.equals(queueName) && (currentDeletionRangeLowerBoundId
                <= lastMessageId)) {
            isSafeToDeliverSlots = false;
        }
        return isSafeToDeliverSlots;
    }

}
