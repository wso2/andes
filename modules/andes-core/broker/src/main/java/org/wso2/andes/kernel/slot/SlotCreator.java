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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel.slot;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.MessageStore;

import java.sql.SQLException;

/**
 * SlotCreator is used to recover slots belonging to a storage queue when the cluster is restarted.
 */
public class SlotCreator implements Runnable {

    /**
     * Class logger
     */
    private static Log log = LogFactory.getLog(SlotCreator.class);

    /**
     * Storage queue handled by current instance
     */
    private final String queueName;

    /**
     * Configured Size of a slot
     */
    private final int slotSize;

    /**
     * Message store instance used to read messages
     */
    private final MessageStore messageStore;

    public SlotCreator(MessageStore messageStore, String queueName) {
        this.messageStore = messageStore;
        this.queueName = queueName;
        this.slotSize = AndesConfigurationManager
                .readValue(AndesConfiguration.PERFORMANCE_TUNING_SLOTS_SLOT_WINDOW_SIZE);
    }

    @Override
    public void run() {
        try {
            log.info("Slot restoring started for " + queueName);
            initializeSlotMapForQueue();
            log.info("Slot restoring ended for " + queueName);
        } catch (Throwable e) {
            log.error("Error occurred in slot recovery", e);
        }
    }

    /**
     * Recover messages for the storage queue
     *
     * @throws AndesException
     * @throws SQLException
     */
    private void initializeSlotMapForQueue() throws AndesException, SQLException {

        RecoverySlotCreator.CallBack slotCreatorCallBack = new RecoverySlotCreator.CallBack();
        int restoreMessagesCounter = messageStore.recoverSlotsForQueue(queueName, 0, slotSize, slotCreatorCallBack);

        log.info("Recovered " + restoreMessagesCounter + " messages for queue \"" + queueName + "\".");

    }
}
