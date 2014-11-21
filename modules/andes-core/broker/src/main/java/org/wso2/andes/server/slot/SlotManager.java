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
     * Slots which are previously owned and released by another node. Key is the queueName.
     */
    private IMap<String, TreeSet<Slot>> unAssignedSlotMap;

    /**
     * To keep list of message IDs against queues
     */
    private IMap<String, TreeSet<Long>> slotIDMap;

    /**
     * To keep track of last assigned message ID against queue.
     */
    private IMap<String, Long> queueToLastAssignedIDMap;

    /**
     * To keep track of assigned slots up to now. Key of the map contains nodeID. key of the
     * hashmap
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
     * Get a slot by giving the queue name. This method first lookup the free slot pool for slots
     * and if there are no slots in the free slot pool then return a newly created slot
     *
     * @param queueName name of the queue
     * @return Slot object
     */
    public Slot getSlot(String queueName, String nodeId) {
        Slot slotToBeAssigned;
        /**
         *First look in the unassigned slots pool for free slots. These slots are previously own by
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
            } else {
                log.debug("Slot Manager - returns empty slot for the queue: " + queueName);
            }
            return slotToBeAssigned;
        }

    }

    /**
     * Get a new slot from slotIDMap
     * @param queueName name of the queue
     * @return slot object
     */
    private Slot getFreshSlot(String queueName) {
        Slot slotToBeAssigned = null;
        TreeSet<Long> messageIDSet = slotIDMap.get(queueName);
        if (messageIDSet != null && !messageIDSet.isEmpty()) {
            slotToBeAssigned = new Slot();
            Long lastAssignedId = queueToLastAssignedIDMap.get(queueName);
            if (lastAssignedId != null) {
                slotToBeAssigned.setStartMessageId(lastAssignedId + 1);
            } else {
                slotToBeAssigned.setStartMessageId(0L);
            }
            slotToBeAssigned.setEndMessageId(messageIDSet.pollFirst());
            slotToBeAssigned.setStorageQueueName(queueName);
            slotIDMap.set(queueName, messageIDSet);
            if (log.isDebugEnabled()) {
                log.debug(slotToBeAssigned.getEndMessageId() + " removed to slotIdMap. Current " +
                        "values in " +
                        "map " + messageIDSet);
            }
            queueToLastAssignedIDMap.set(queueName, slotToBeAssigned.getEndMessageId());
        }
        return slotToBeAssigned;

    }


    /**
     * Update the slot assignment map when a slot is assigned
     *
     * @param queueName     Name of the queue
     * @param allocatedSlot Slot object which is allocated to a particular node
     */
    private void updateSlotAssignmentMap(String queueName, Slot allocatedSlot, String nodeId) {
        ArrayList<Slot> currentSlotList;
        HashMap<String, List<Slot>> queueToSlotMap = slotAssignmentMap.get(nodeId);
        if (queueToSlotMap == null) {
            queueToSlotMap = new HashMap<String, List<Slot>>();
            slotAssignmentMap.putIfAbsent(nodeId, queueToSlotMap);
        }
        //Lock is used because this method will be called by multiple nodes at the same time
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
     * @param queueName name of the queue which this message ID belongs to
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
             *Insert the messageID only if last processed ID of this queue is less than this
             * messageID
             */
            Long lastAssignedMessageId = queueToLastAssignedIDMap.get(queueName);
            if (lastAssignedMessageId != null) {
                if (lastMessageIdInTheSlot < lastAssignedMessageId) {
                    isMessageIdRangeOutdated = true;
                }
            }

            /**
             * Update the slotIDMap only if the last assigned message ID is less than the new ID
             */
            if (!isMessageIdRangeOutdated) {
                messageIdSet.add(lastMessageIdInTheSlot);
                slotIDMap.set(queueName, messageIdSet);
                if (log.isDebugEnabled()) {
                    log.debug(lastMessageIdInTheSlot + " added to slotIdMap. CUrrent values in " +
                            "map " + messageIdSet);
                }
            }
        }

    }

    /**
     * This method will reassigned slots which are owned by a node to a free slots pool
     *
     * @param nodeId node ID of the leaving node
     */
    public void reAssignSlotsWhenMemberLeaves(String nodeId) {
        //Remove the entry from slot assignment map
        HashMap<String, List<Slot>> queueToSlotMap = slotAssignmentMap.remove(nodeId);
        if (queueToSlotMap != null) {
            for (Map.Entry<String, List<Slot>> entry : queueToSlotMap.entrySet()) {
                List<Slot> slotsToBeReAssigned = entry.getValue();
                TreeSet<Slot> freeSlotTreeSet = new TreeSet<Slot>();
                for (Slot slotToBeReAssigned : slotsToBeReAssigned) {
                    //Re-assign only if the slot is not empty
                    if (!SlotUtils.checkSlotEmptyFromMessageStore(slotToBeReAssigned)) {
                        unAssignedSlotMap.putIfAbsent(slotToBeReAssigned.getStorageQueueName(),
                                freeSlotTreeSet);
                        //Lock key is queuName + SlotManager Class
                        String lockKey = (entry.getKey() + SlotManager.class).intern();
                        synchronized (lockKey) {
                            freeSlotTreeSet = unAssignedSlotMap
                                    .get(slotToBeReAssigned.getStorageQueueName());
                            freeSlotTreeSet.add(slotToBeReAssigned);
                            unAssignedSlotMap
                                    .set(slotToBeReAssigned.getStorageQueueName(), freeSlotTreeSet);
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
     * Remove slot entry from slotAssignment map
     *
     * @param queueName name of the queue which is owned by the slot to be deleted
     * @param emptySlot reference of the slot to be deleted
     */
    public void deleteSlot(String queueName, Slot emptySlot, String nodeId) {
        String lockKey = (nodeId + SlotManager.class).intern();
        synchronized (lockKey) {
            HashMap<String, List<Slot>> queueToSlotMap = slotAssignmentMap.get(nodeId);
            if (queueToSlotMap != null) {
                queueToSlotMap = slotAssignmentMap.get(nodeId);
                ArrayList<Slot> currentSlotList = (ArrayList<Slot>) queueToSlotMap.get(queueName);
                if (currentSlotList != null) {
                    currentSlotList.remove(emptySlot);
                    queueToSlotMap.put(queueName, currentSlotList);
                    slotAssignmentMap.set(nodeId, queueToSlotMap);
                }
                if (log.isDebugEnabled()) {
                    log.debug("Unassigned slot " + emptySlot.getStartMessageId() + " - " +
                            emptySlot
                                    .getEndMessageId() + "owned by node: " + nodeId + "");
                }
            }
        }
    }

    /**
     * Re-assign the slot when there are no local subscribers in the node
     *
     * @param nodeId node ID of the node without subscribers
     * @param queueName  name of the queue whose slots to be reassigned
     */
    public void reAssignSlotWhenNoSubscribers(String nodeId, String queueName) {
        ArrayList<Slot> assignedSlotList = null;
        String lockKeyForNodeId = (nodeId + SlotManager.class).intern();
        synchronized (lockKeyForNodeId) {
            HashMap<String, List<Slot>> queueToSlotMap = slotAssignmentMap.get(nodeId);
            if (queueToSlotMap != null) {
                assignedSlotList = (ArrayList<Slot>) queueToSlotMap.remove(queueName);
                slotAssignmentMap.set(nodeId, queueToSlotMap);
            }
        }
        if (assignedSlotList != null) {
            String lockKeyForQueueName = (queueName + SlotManager.class).intern();
            synchronized (lockKeyForQueueName) {
                TreeSet<Slot> unAssignedSlotSet = unAssignedSlotMap.get(queueName);
                if (unAssignedSlotSet == null) {
                    unAssignedSlotSet = new TreeSet<Slot>();
                }
                for (Slot slotToBeReAssigned : assignedSlotList) {
                    //Reassign only if the slot is not empty
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
     * @param queueName name of the queue whose slots to be deleted
     */
    public void deleteAllSlots(String queueName) {
        unAssignedSlotMap.remove(queueName);
        slotIDMap.remove(queueName);
    }

    /**
     * Delete all slot associations with a given queue. This is required to handle a queue purge event.
     * @param queueName name of destination queue
     */
    public void clearAllActiveSlotRelationsToQueue(String queueName) {
        unAssignedSlotMap.remove(queueName);
        slotIDMap.remove(queueName);

        // Clear slots assigned to the queue
        String nodeId = HazelcastAgent.getInstance().getNodeId();
        String lockKey = (nodeId + SlotManager.class).intern();

        synchronized (lockKey) {
            // The requirement here is to clear slot associations for the queue on all nodes.
            Set<Map.Entry<String,HashMap<String,List<Slot>>>> nodeEntries = slotAssignmentMap.entrySet();

            Iterator<Map.Entry<String,HashMap<String,List<Slot>>>> iterator = nodeEntries.iterator();

            while(iterator.hasNext()) {
                iterator.next().getValue().put(queueName,new ArrayList<Slot>());
            }
        }


    }
}
