/*
 *
 *   Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 * /
 */

package org.wso2.andes.server.slot;

import com.hazelcast.core.IMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;

import java.util.*;

/**
 * Slot Manager is responsible of slot allocating, slot creating, slot re-assigning and slot
 * managing tasks
 */
public class SlotManager {

    private static SlotManager slotManager = new SlotManager();

    /**
     * slots which are previously owned and released by another node. Key is the queueName.
     */
    private IMap<String, TreeSet<Slot>> unAssignedSlotMap;

    /**
     * to keep list of message IDs against queues
     */
    private IMap<String, TreeSet<Long>> slotIDMap;

    /**
     * to keep track of last assigned message ID against queue.
     */
    private IMap<String, Long> queueToLastAssignedIDMap;

    /**
     * to keep track of assigned slots up to now. Key of the map contains nodeID. key of the hashmap
     * is queue name
     */
    private IMap<String, HashMap<String, List<Slot>>> slotAssignmentMap;

    private static Log log = LogFactory.getLog(SlotManager.class);


    private SlotManager() {
        if (AndesContext.getInstance().isClusteringEnabled()) {
            HazelcastAgent hazelcastAgent = HazelcastAgent.getInstance();
            /**
             * Initialize distributed maps used in this class
             */
            unAssignedSlotMap = hazelcastAgent.getUnAssignedSlotMap();
            slotIDMap = hazelcastAgent.getSlotIdMap();
            queueToLastAssignedIDMap = hazelcastAgent.getLastAssignedIDMap();
            slotAssignmentMap = hazelcastAgent.getSlotAssignmentMap();

        }
    }

    /**
     * @return SlotManager instance
     */
    public static SlotManager getInstance() {
        return slotManager;
    }

    /**
     * get a slot by giving the queue name. This method first lookup the free slot pool for slots
     * and if there are no slots in the free slot pool then return a newly created slot
     *
     * @param queueName name of the queue
     * @return Slot object
     */
    public Slot getSlot(String queueName, String nodeId) {
        Slot slotToBeAssigned;
        /**
         *first look in the unassigned slots pool for free slots. These slots are previously own by
         * other nodes
         */
        String lockKey = (queueName + SlotManager.class).intern();
        synchronized (lockKey) {
            TreeSet<Slot> slotsFromUnassignedSlotMap = unAssignedSlotMap.get(queueName);
            if (slotsFromUnassignedSlotMap != null && !slotsFromUnassignedSlotMap.isEmpty()) {
                slotToBeAssigned = unAssignedSlotMap.get(queueName).pollFirst();
            } else {
                slotToBeAssigned = getFreshSlot(queueName);
            }
            if (null != slotToBeAssigned) {
                updateSlotAssignmentMap(queueName, slotToBeAssigned, nodeId);
            }
            return slotToBeAssigned;
        }

    }

    /**
     * @param queueName name of the queue
     * @return slot object //todo check if the range is inclusive in message store
     */
    private Slot getFreshSlot(String queueName) {
        Slot slotImpToBeAssigned = null;
        TreeSet<Long> messageIDSet = slotIDMap.get(queueName);
        if (messageIDSet != null && !messageIDSet.isEmpty()) {
            slotImpToBeAssigned = new Slot();
            Long lastAssignedId = queueToLastAssignedIDMap.get(queueName);
            if (lastAssignedId != null) {
                slotImpToBeAssigned.setStartMessageId(lastAssignedId + 1);
            } else {
                slotImpToBeAssigned.setStartMessageId(0L);
            }
            slotImpToBeAssigned.setEndMessageId(messageIDSet.pollFirst());
            slotImpToBeAssigned.setQueueName(queueName);
            slotIDMap.set(queueName, messageIDSet);
            queueToLastAssignedIDMap.set(queueName, slotImpToBeAssigned.getEndMessageId());
        }
        return slotImpToBeAssigned;

    }


    /**
     * update the slot assignment map when a slot is assigned
     *
     * @param queueName     name of the queue
     * @param allocatedSlot slot object which is allocated to a particular node
     */
    private void updateSlotAssignmentMap(String queueName, Slot allocatedSlot, String nodeId) {
        ArrayList<Slot> currentSlotList;
        HashMap<String, List<Slot>> queueToSlotMap = slotAssignmentMap.get(nodeId);
        if (queueToSlotMap == null) {
            queueToSlotMap = new HashMap<String, List<Slot>>();
            slotAssignmentMap.putIfAbsent(nodeId, queueToSlotMap);
        }
        //lock is used because this method will be called by multiple nodes at the same time
        String lockKey = (nodeId + SlotManager.class).intern();
        synchronized (lockKey) {
            queueToSlotMap = slotAssignmentMap.get(nodeId);
            currentSlotList = (ArrayList<Slot>) queueToSlotMap.get(queueName);
            if (currentSlotList == null) {
                currentSlotList = new ArrayList<Slot>();
            }
            currentSlotList.add(allocatedSlot);
            queueToSlotMap.put(queueName, currentSlotList);
            slotAssignmentMap.set(nodeId, queueToSlotMap);
        }

    }


    /**
     * Record Slot's last message ID related to a particular queue
     *
     * @param queueName
     * @param lastMessageIdInTheSlot
     */
    public void updateMessageID(String queueName, Long lastMessageIdInTheSlot) {
        boolean isMessageIdRangeOutdated = false;
        String lockKey = (queueName + SlotManager.class).intern();

        TreeSet<Long> messageIdSet = slotIDMap.get(queueName);
        if (messageIdSet == null) {
            messageIdSet = new TreeSet<Long>();
            slotIDMap.putIfAbsent(queueName, messageIdSet);
            messageIdSet = slotIDMap.get(queueName);
        }
        synchronized (lockKey) {
            /**
             *insert the messageID only if last processed ID of this queue is less than this
             * messageID
             */
            Long lastAssignedMessageId = queueToLastAssignedIDMap.get(queueName);
            if (lastAssignedMessageId != null) {
                if (lastMessageIdInTheSlot < lastAssignedMessageId) {
                    isMessageIdRangeOutdated = true;
                }
            }

            /**
             * update the slotIDMap only if the last assigned message ID is less than the new ID
             */
            if (!isMessageIdRangeOutdated) {
                messageIdSet.add(lastMessageIdInTheSlot);
                slotIDMap.set(queueName, messageIdSet);
            }
        }

    }

    /**
     * This method will reassigned slots which are owned by a node to a free slots pool
     *
     * @param nodeId
     */
    public void reAssignSlotsWhenMemberLeaves(String nodeId) {
        //remove the entry from slot assignment map
        HashMap<String, List<Slot>> queueToSlotMap = slotAssignmentMap.remove(nodeId);
        if (queueToSlotMap != null) {
            for (Map.Entry<String, List<Slot>> entry : queueToSlotMap.entrySet()) {
                List<Slot> slotsToBeReAssigned = entry.getValue();
                TreeSet<Slot> freeSlotTreeSet = new TreeSet<Slot>();
                for (Slot slotToBeReAssigned : slotsToBeReAssigned) {
                    //reassign only if the slot is not empty
                    if (!SlotUtils.checkSlotEmptyFromMessageStore(slotToBeReAssigned)) {
                        unAssignedSlotMap.putIfAbsent(slotToBeReAssigned.getQueueName(),
                                freeSlotTreeSet);
                        //lock key is queuName + SlotManager Class
                        String lockKey = (entry.getKey() + SlotManager.class).intern();
                        synchronized (lockKey) {
                            freeSlotTreeSet = unAssignedSlotMap
                                    .get(slotToBeReAssigned.getQueueName());
                            freeSlotTreeSet.add(slotToBeReAssigned);
                            unAssignedSlotMap
                                    .set(slotToBeReAssigned.getQueueName(), freeSlotTreeSet);
                            if (log.isDebugEnabled()) {
                                log.debug("Reassigned slot " + slotToBeReAssigned
                                        .getStartMessageId() + " - " +
                                        slotToBeReAssigned
                                                .getEndMessageId() + "from node " + nodeId);
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * remove slot entry from slotAssignment map
     *
     * @param queueName
     * @param emptySlot
     */
    public void deleteSlot(String queueName, Slot emptySlot, String nodeId) {
        HashMap<String, List<Slot>> queueToSlotMap = slotAssignmentMap.get(nodeId);
        if (queueToSlotMap != null) {
            String lockKey = (nodeId + SlotManager.class).intern();
            synchronized (lockKey) {
                queueToSlotMap = slotAssignmentMap.get(nodeId);
                ArrayList<Slot> currentSlotList = (ArrayList<Slot>) queueToSlotMap.get(queueName);
                currentSlotList.remove(emptySlot);
                queueToSlotMap.put(queueName, currentSlotList);
                slotAssignmentMap.set(nodeId, queueToSlotMap);
                if (log.isDebugEnabled()) {
                    log.debug("Unassigned slot " + emptySlot.getStartMessageId() + " - " +
                            emptySlot
                                    .getEndMessageId() + "owned by node: " + nodeId + "");
                }
            }

        }

    }

    /**
     * re-assign the slot when there are no local subscribers in the node
     *
     * @param nodeId
     * @param queueName
     */
    public void reAssignSlotWhenNoSubscribers(String nodeId, String queueName) {
        ArrayList<Slot> assignedSlotList = null;
        String lockKey1 = (nodeId + SlotManager.class).intern();
        synchronized (lockKey1) {
            HashMap<String, List<Slot>> queueToSlotMap = slotAssignmentMap.get(nodeId);
            if (queueToSlotMap != null) {
                assignedSlotList = (ArrayList<Slot>) queueToSlotMap.remove(queueName);
                slotAssignmentMap.set(nodeId, queueToSlotMap);
            }
        }
        if (assignedSlotList != null) {
            String lockKey2 = (queueName + SlotManager.class).intern();
            synchronized (lockKey2) {
                TreeSet<Slot> unAssignedSlotSet = unAssignedSlotMap.get(queueName);
                if (unAssignedSlotSet == null) {
                    unAssignedSlotSet = new TreeSet<Slot>();
                }
                for (Slot slotToBeReAssigned : assignedSlotList) {
                    //reassign only if the slot is not empty
                    if (!SlotUtils.checkSlotEmptyFromMessageStore(slotToBeReAssigned)) {
                        unAssignedSlotSet.add(slotToBeReAssigned);
                    }
                }
                unAssignedSlotMap.set(queueName, unAssignedSlotSet);
            }
        }
    }

    /**
     * Delete all the slots belongs to a queue from unAssignedSlotMap and slotIDMap
     *
     * @param queueName
     */
    public void deleteAllSlots(String queueName) {
        unAssignedSlotMap.remove(queueName);
        slotIDMap.remove(queueName);
    }
}
