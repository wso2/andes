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

import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is  responsible of slot allocating, slot creating, slot re-assigning and slot
 * managing tasks in standalone mode
 */
public class SlotManagerStandalone {

    /**
     * To keep message IDs against queues.
     */
    private ConcurrentHashMap<String, TreeSet<Long>> slotIDMap;

    /**
     * To keep track of last assigned message ID against queue.
     */
    private ConcurrentHashMap<String, Long> queueToLastAssignedIDMap;

    private static Log log = LogFactory.getLog(SlotManagerStandalone.class);


    public SlotManagerStandalone() {

        /**
         * Initialize distributed maps used in this class
         */
        slotIDMap = new ConcurrentHashMap<String, TreeSet<Long>>();
        queueToLastAssignedIDMap = new ConcurrentHashMap<String, Long>();
    }

    /**
     * Get a slot by giving the queue name.
     *
     * @param queueName Name of the queue
     * @return Slot object
     */
    public Slot getSlot(String queueName) {
        Slot slotToBeAssigned;
        String lockKey = queueName + SlotManagerStandalone.class;
        synchronized (lockKey.intern()) {

            slotToBeAssigned = getFreshSlot(queueName);
            if (log.isDebugEnabled()) {
                log.debug("Slot Manager - giving a slot from fresh pool. Slot= " + slotToBeAssigned);
            }

            if (null == slotToBeAssigned) {
                if (log.isDebugEnabled()) {
                    log.debug("Slot Manager - returns empty slot for the queue: " + queueName);
                }
            }
            return slotToBeAssigned;
        }
    }

    /**
     * Get a new slot from slotIDMap
     *
     * @param queueName Name of the queue
     * @return Slot object
     */
    private Slot getFreshSlot(String queueName) {
        Slot slotToBeAssigned = null;
        TreeSet<Long> messageIDSet = slotIDMap.get(queueName);
        if (null != messageIDSet && !messageIDSet.isEmpty()) {
            slotToBeAssigned = new Slot();
            Long lastAssignedId = queueToLastAssignedIDMap.get(queueName);
            if (lastAssignedId != null) {
                slotToBeAssigned.setStartMessageId(lastAssignedId + 1);
            } else {
                slotToBeAssigned.setStartMessageId(0L);
            }
            slotToBeAssigned.setEndMessageId(messageIDSet.pollFirst());
            slotToBeAssigned.setStorageQueueName(queueName);
            slotIDMap.put(queueName, messageIDSet);
            if (log.isDebugEnabled()) {
                log.debug(slotToBeAssigned.getEndMessageId() + " removed to slotIdMap. Current " +
                        "values in " +
                        "map " + messageIDSet);
            }
            queueToLastAssignedIDMap.put(queueName, slotToBeAssigned.getEndMessageId());
        }

        return slotToBeAssigned;

    }

    /**
     * Record Slot's last message ID related to a particular queue
     *
     * @param queueName              Name of the queue which this message ID belongs to
     * @param lastMessageIdInTheSlot Last message ID of the slot
     */
    public void updateMessageID(String queueName, Long lastMessageIdInTheSlot) {


        TreeSet<Long> messageIdSet = slotIDMap.get(queueName);
        if (messageIdSet == null) {
            messageIdSet = new TreeSet<Long>();
        }
        String lockKey = queueName + SlotManagerStandalone.class;
        synchronized (lockKey.intern()) {
            /**
             * Update the slotIDMap
             */
            messageIdSet.add(lastMessageIdInTheSlot);

            slotIDMap.put(queueName, messageIdSet);
            if (log.isDebugEnabled()) {
                log.debug(lastMessageIdInTheSlot + " added to slotIdMap. Current values in " +
                        "map " + messageIdSet);
            }

        }

    }


    /**
     * Delete all slot associations with a given queue. This is required to handle a queue purge event.
     *
     * @param queueName Name of destination queue
     */
    public void clearAllActiveSlotRelationsToQueue(String queueName) {

        if (null != slotIDMap) {
            slotIDMap.remove(queueName);
        }

    }

}
