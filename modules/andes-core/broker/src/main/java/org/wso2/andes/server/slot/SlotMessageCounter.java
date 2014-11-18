/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.server.slot;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.server.slot.thrift.MBThriftClient;

import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is responsible of counting messages in a slot for each queue
 */
public class SlotMessageCounter {

    private ConcurrentHashMap<String, Slot> queueToSlotMap = new ConcurrentHashMap<String, Slot>();
    private ConcurrentHashMap<String, Long> slotTimeOutMap = new ConcurrentHashMap<String, Long>();
    /**
     * Timeout in milliseconds for messages in the slot. When this timeout is exceeded slot will be
     * submitted to the coordinator
     */
    private Long timeOutForMessagesInQueue;
    private Timer submitSlotToCoordinatorTimer = new Timer();
    private Log log = LogFactory.getLog(SlotMessageCounter.class);
    private static SlotMessageCounter slotMessageCounter = new SlotMessageCounter();
    private Integer slotWindowSize;

    private SlotMessageCounter() {
        scheduleSubmitSlotToCoordinatorTimer();

        try {
            slotWindowSize = AndesConfigurationManager.getInstance().readConfigurationValue
                    (AndesConfiguration.PERFORMANCE_TUNING_SLOTS_SLOT_WINDOW_SIZE);
        } catch (AndesException e) {
            slotWindowSize = Integer.valueOf(AndesConfiguration
                    .PERFORMANCE_TUNING_SLOTS_SLOT_WINDOW_SIZE.get().getDefaultValue());
        }

        try {
            timeOutForMessagesInQueue = AndesConfigurationManager.getInstance()
                    .readConfigurationValue(AndesConfiguration
                            .PERFORMANCE_TUNING_SLOTS_SLOT_RETAIN_TIME_IN_MEMORY);
        } catch (AndesException e) {
            timeOutForMessagesInQueue = Long.valueOf(AndesConfiguration
                    .PERFORMANCE_TUNING_SLOTS_SLOT_RETAIN_TIME_IN_MEMORY.get().getDefaultValue());
        }
    }

    /**
     * This thread is to record message IDs in slot manager when a timeout is passed
     */
    private void scheduleSubmitSlotToCoordinatorTimer() {
        submitSlotToCoordinatorTimer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                for (Map.Entry<String, Long> entry : slotTimeOutMap.entrySet()) {
                    if ((System.currentTimeMillis() - entry
                            .getValue()) > timeOutForMessagesInQueue) {
                        try {
                            submitSlot(entry.getKey());
                        } catch (AndesException e) {
                           /*
                            We do not do anything here since this thread will be run every 3
                            seconds
                             */
                            log.error(
                                    "Error occurred while connecting to the thrift coordinator " + e
                                            .getMessage(), e);
                        }
                    }
                }
            }
        }, 4000, 3000);
    }

    /**
     * Record metadata count in the current slot related to a particular queue.
     *
     * @param metadataList metadata list to be record
     */
    public void recordMetaDataCountInSlot(List<AndesMessageMetadata> metadataList) {
        //If metadata list is null this method is called from time out thread
        for (AndesMessageMetadata md : metadataList) {
            String storageQueueName = md.getStorageQueueName();
            //If this is the first message to that queue
            Slot currentSlot;
            synchronized (this) {
                currentSlot = updateQueueToSlotMap(md);
            }
            if (currentSlot.getMessageCount() >= slotWindowSize) {
                try {
                    submitSlot(storageQueueName);
                } catch (AndesException e) {
                    /*
                    We do not do anything here since this operation will be run by timeout thread also
                     */
                    log.error("Error occurred while connecting to the thrift coordinator " + e
                            .getMessage(), e);
                }
            }
        }
    }

    /**
     * Update in-memory queue to slot map. This method is synchronized since many publishers can
     * be access this thread simultaneously.
     * @param metadata  Andes metadata whose ID needs to be reported to SlotManager
     * @return Current slot which this metadata belongs to
     */
    private synchronized Slot updateQueueToSlotMap(AndesMessageMetadata metadata) {
        String storageQueueName = metadata.getStorageQueueName();
        Slot currentSlot = queueToSlotMap.get(storageQueueName);
        if (currentSlot == null) {
            currentSlot = new Slot();
            currentSlot.setEndMessageId(metadata.getMessageID());
            currentSlot.setMessageCount(1L);
            queueToSlotMap.put(storageQueueName, currentSlot);
            slotTimeOutMap.put(storageQueueName, System.currentTimeMillis());
        } else {
            long currentMsgCount = currentSlot.getMessageCount();
            long newMessageCount = currentMsgCount + 1;
            currentSlot.setMessageCount(newMessageCount);
            currentSlot.setEndMessageId(metadata.getMessageID());
            queueToSlotMap.put(storageQueueName, currentSlot);
        }
        return currentSlot;
    }

    /**
     * Submit last message ID in the slot to SlotManager.
     *
     * @param storageQueueName  name of the queue which this slot belongs to
     */
    public void submitSlot(String storageQueueName) throws AndesException {
        Slot slot = queueToSlotMap.get(storageQueueName);
        if (null != slot) {
            try {
                MBThriftClient.updateMessageId(storageQueueName, slot.getEndMessageId());
                queueToSlotMap.remove(storageQueueName);
                slotTimeOutMap.remove(storageQueueName);

            } catch (ConnectionException e) {
                 /* we only log here since this thread will be run every 3
                seconds*/
                log.error("Error occurred while connecting to the thrift coordinator " + e
                        .getMessage(), e);
            }
        }
    }

    /**
     * @return SlotMessageCounter instance
     */
    public static SlotMessageCounter getInstance() {
        return slotMessageCounter;
    }

}
