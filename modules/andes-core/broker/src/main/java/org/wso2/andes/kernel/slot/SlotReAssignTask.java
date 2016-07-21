/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
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

import java.util.TimerTask;

/**
 * This class is a scheduler class to schedule re-assignment of slots when last subscriber leaves a particular
 * queue
 */
public class SlotReAssignTask extends TimerTask {

    /**
     * Storage queue handled by this task
     */
    private String storageQueue;

    /**
     * Class logger
     */
    private static Log log = LogFactory.getLog(SlotReAssignTask.class);

    /**
     * Create a task for re-assign all slots for  given queue
     *
     * @param storageQueue name of queue to release
     */
    public SlotReAssignTask(String storageQueue) {
        this.storageQueue = storageQueue;
    }

    public void run() {
        if (log.isDebugEnabled()) {
            log.debug("Trying to reAssign slots for queue " + storageQueue);
        }
        try {
            MessagingEngine.getInstance().getSlotCoordinator().reAssignSlotWhenNoSubscribers(storageQueue);

            if (log.isDebugEnabled()) {
                log.debug("Re-assigned slots for queue: " + storageQueue);
            }

        } catch (ConnectionException e) {
            log.error("Error occurred while re-assigning the slot to slot manager", e);
        }
    }

}
