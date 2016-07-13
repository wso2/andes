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

import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesException;

/**
 * SafeDeletion is responsible for selecting the expired messages from the safe deletion zone
 */
public abstract class SafeDeletion {

    /**
     * To keep the name of queue on which deletion task is currently running
     */
    protected String currentDeletionQueue;

    /**
     * To keep the lower bound Id of the current expiry message deletion range
     */
    protected long currentDeletionRangeLowerBoundId;

    /**
     *  The slot gap need to be maintained as not reachable for deletion task as they may be allocated
     *  to a slot delivery worker in the near future
     */
    int safetySlotCount = AndesConfigurationManager.readValue
            (AndesConfiguration.PERFORMANCE_TUNING_SAFE_DELETE_REGION_SLOT_COUNT);

    /**
     * Get the lower bound id of safe deletion range
     * @return lower bound id
     */
    public abstract long getSafeZoneLowerBoundId(String queueName) throws AndesException;

    /**
     * Set the deletion task's current working range and queue
     * @param currentDeletionQueueName Queue name on which deletion currently happens
     * @param currentDeletionRangeLowerBoundId Lower bound Id of deletion range
     */
    public void setDeletionTaskState(String currentDeletionQueueName,
                                     long currentDeletionRangeLowerBoundId){
        this.currentDeletionQueue = currentDeletionQueueName;
        this.currentDeletionRangeLowerBoundId = currentDeletionRangeLowerBoundId;
    }

    /**
     * Clear the state of current deletion range
     */
    public void clearDeletionTaskState(){
        this.currentDeletionQueue = "";
        this.currentDeletionRangeLowerBoundId = 0L;
    }

}
