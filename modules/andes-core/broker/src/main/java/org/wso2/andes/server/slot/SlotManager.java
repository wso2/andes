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
import org.wso2.andes.server.cluster.coordination.hazelcast.custom.serializer.wrapper.HashmapStringListWrapper;
import org.wso2.andes.server.cluster.coordination.hazelcast.custom.serializer.wrapper.TreeSetLongWrapper;
import org.wso2.andes.server.cluster.coordination.hazelcast.custom.serializer.wrapper.TreeSetSlotWrapper;


import java.util.*;

/**
 * Slot Manager is responsible of slot allocating, slot creating, slot re-assigning and slot
 * managing tasks
 */
public class SlotManager {


    private static SlotManager slotManager = new SlotManager();

    /**
     * Slots which are previously owned and released by another node. Key is the queueName. Value
     * is TreeSetSlotWrapper objects. TreeSetSlotWrapper is a wrapper class for TreeSet<Slot>.
     */
    private IMap<String, TreeSetSlotWrapper> unAssignedSlotMap;

    /**
     * To keep TreeSetLongWrapper objects against queues. TreeSetLongWrapper is a wrapper class
     * for a Long TreeSet. Long TreeSet inside TreeSetLongWrapper is the list of message IDs.
     */
    private IMap<String, TreeSetLongWrapper> slotIDMap;

    /**
     * To keep track of last assigned message ID against queue.
     */
    private IMap<String, Long> queueToLastAssignedIDMap;

    /**
     * To keep track of assigned slots up to now. Key of the map contains nodeID. Value is
     * HashmapStringListWrapper object. HashmapStringListWrapper is a wrapper class for
     * HashMap<String,List<Slot>>. Key in that hash map is queue name. value is List of Slot
     * objects.
     */
    private IMap<String, HashmapStringListWrapper> slotAssignmentMap;

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
        String lockKey = queueName + SlotManager.class;
        synchronized (lockKey.intern()) {
            TreeSetSlotWrapper treeSetSlotWrapper = unAssignedSlotMap.get(queueName);
            if(treeSetSlotWrapper != null){
                TreeSet<Slot> slotsFromUnassignedSlotMap = treeSetSlotWrapper.getSlotTreeSet();
                if (slotsFromUnassignedSlotMap != null && !slotsFromUnassignedSlotMap.isEmpty()) {
                    slotToBeAssigned = slotsFromUnassignedSlotMap.pollFirst();
                    //update hazelcast map
                    treeSetSlotWrapper.setSlotTreeSet(slotsFromUnassignedSlotMap);
                    unAssignedSlotMap.set(queueName, treeSetSlotWrapper);
                    if (log.isDebugEnabled()) {
                        log.debug("Slot Manager - giving a slot from unAssignedSlotMap. Slot= " + slotToBeAssigned);
                    }
                }else {
                    slotToBeAssigned = getFreshSlot(queueName);
                    if (log.isDebugEnabled()) {
                        log.debug("Slot Manager - giving a slot from fresh pool. Slot= " + slotToBeAssigned);
                    }
                }
            }else {
                slotToBeAssigned = getFreshSlot(queueName);
                if (log.isDebugEnabled()) {
                    log.debug("Slot Manager - giving a slot from fresh pool. Slot= " + slotToBeAssigned);
                }
            }
            if (null != slotToBeAssigned) {
                updateSlotAssignmentMap(queueName, slotToBeAssigned, nodeId);
            } else {
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
     * @param queueName name of the queue
     * @return slot object
     */
    private Slot getFreshSlot(String queueName) {
        Slot slotToBeAssigned = null;
        TreeSetLongWrapper wrapper = slotIDMap.get(queueName);
        TreeSet<Long> messageIDSet;
        if (wrapper != null) {
            messageIDSet = wrapper.getLongTreeSet();
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
                wrapper.setLongTreeSet(messageIDSet);
                slotIDMap.set(queueName, wrapper);
                if (log.isDebugEnabled()) {
                    log.debug(slotToBeAssigned.getEndMessageId() + " removed to slotIdMap. Current " +
                            "values in " +
                            "map " + messageIDSet);
                }
                queueToLastAssignedIDMap.set(queueName, slotToBeAssigned.getEndMessageId());
            }
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
        HashMap<String, List<Slot>> queueToSlotMap;
        HashmapStringListWrapper wrapper = slotAssignmentMap.get(nodeId);
        if (wrapper == null) {
            wrapper = new HashmapStringListWrapper();
            queueToSlotMap = new HashMap<String, List<Slot>>();
            wrapper.setStringListHashMap(queueToSlotMap);
            slotAssignmentMap.putIfAbsent(nodeId, wrapper);
        }
        //Lock is used because this method will be called by multiple nodes at the same time
        String lockKey = nodeId + SlotManager.class;
        synchronized (lockKey.intern()) {
            wrapper = slotAssignmentMap.get(nodeId);
            queueToSlotMap = wrapper.getStringListHashMap();
            currentSlotList = (ArrayList<Slot>) queueToSlotMap.get(queueName);
            if (currentSlotList == null) {
                currentSlotList = new ArrayList<Slot>();
            }
            currentSlotList.add(allocatedSlot);
            queueToSlotMap.put(queueName, currentSlotList);
            wrapper.setStringListHashMap(queueToSlotMap);
            slotAssignmentMap.set(nodeId, wrapper);
        }

    }


    /**
     * Record Slot's last message ID related to a particular queue
     *
     * @param queueName              name of the queue which this message ID belongs to
     * @param lastMessageIdInTheSlot  last message ID of the slot
     */
    public void updateMessageID(String queueName, Long lastMessageIdInTheSlot) {

        boolean isMessageIdRangeOutdated = false;
        TreeSet<Long> messageIdSet = new TreeSet<Long>();
        TreeSetLongWrapper wrapper= slotIDMap.get(queueName);
        if (wrapper == null) {
            wrapper = new TreeSetLongWrapper();
            wrapper.setLongTreeSet(messageIdSet);
            slotIDMap.putIfAbsent(queueName, wrapper);
            messageIdSet = slotIDMap.get(queueName).getLongTreeSet();
        }
        String lockKey = queueName + SlotManager.class;
        synchronized (lockKey.intern()) {
            /**
             *Insert the messageID only if last processed ID of this queue is less than this
             * messageID
             */
            Long lastAssignedMessageId = queueToLastAssignedIDMap.get(queueName);
            if (lastAssignedMessageId != null) {
                if (lastMessageIdInTheSlot <= lastAssignedMessageId) {
                    isMessageIdRangeOutdated = true;
                }
            }

            /**
             * Update the slotIDMap only if the last assigned message ID is less than the new ID
             */
            if (!isMessageIdRangeOutdated) {
                messageIdSet.add(lastMessageIdInTheSlot);
                wrapper.setLongTreeSet(messageIdSet);
                slotIDMap.set(queueName, wrapper);
                if (log.isDebugEnabled()) {
                    log.debug(lastMessageIdInTheSlot + " added to slotIdMap. Current values in " +
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
        HashmapStringListWrapper wrapper = slotAssignmentMap.remove(nodeId);
        HashMap<String, List<Slot>> queueToSlotMap = null;
        if (wrapper!=null) {
            queueToSlotMap = wrapper.getStringListHashMap();
        }
        if (queueToSlotMap!= null) {
            for (Map.Entry<String, List<Slot>> entry : queueToSlotMap.entrySet()) {
                List<Slot> slotsToBeReAssigned = entry.getValue();
                TreeSet<Slot> freeSlotTreeSet = new TreeSet<Slot>();
                TreeSetSlotWrapper treeSetStringWrapper = new TreeSetSlotWrapper();
                for (Slot slotToBeReAssigned : slotsToBeReAssigned) {

                    //Re-assign only if the slot is not empty
                    if (!SlotUtils.checkSlotEmptyFromMessageStore(slotToBeReAssigned)) {
                        treeSetStringWrapper.setSlotTreeSet(freeSlotTreeSet);
                        unAssignedSlotMap.putIfAbsent(slotToBeReAssigned.getStorageQueueName(),
                                treeSetStringWrapper);
                        //Lock key is queuName + SlotManager Class
                        String lockKey = entry.getKey() + SlotManager.class;
                        synchronized (lockKey.intern()) {
                            treeSetStringWrapper = unAssignedSlotMap
                                    .get(slotToBeReAssigned.getStorageQueueName());
                            freeSlotTreeSet = treeSetStringWrapper.getSlotTreeSet();
                            //String jsonSlotString = gson.toJson(slotsToBeReAssigned);
                            freeSlotTreeSet.add(slotToBeReAssigned);
                            treeSetStringWrapper.setSlotTreeSet(freeSlotTreeSet);
                            unAssignedSlotMap
                                    .set(slotToBeReAssigned.getStorageQueueName(), treeSetStringWrapper);
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
        String lockKey = nodeId + SlotManager.class;
        synchronized (lockKey.intern()) {
            HashMap<String, List<Slot>> queueToSlotMap = null;
            HashmapStringListWrapper wrapper = slotAssignmentMap.get(nodeId);
            if(wrapper!=null){
              queueToSlotMap = wrapper.getStringListHashMap();
            }
            if (queueToSlotMap != null) {
                ArrayList<Slot> currentSlotList = (ArrayList<Slot>) queueToSlotMap.get
                        (queueName);
                if (currentSlotList != null) {
                   // com.google.gson.Gson gson = new GsonBuilder().create();
                    currentSlotList.remove(emptySlot);
                    queueToSlotMap.put(queueName, currentSlotList);
                    wrapper.setStringListHashMap(queueToSlotMap);
                    slotAssignmentMap.set(nodeId, wrapper);
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
     * @param nodeId    node ID of the node without subscribers
     * @param queueName name of the queue whose slots to be reassigned
     */
    public void reAssignSlotWhenNoSubscribers(String nodeId, String queueName) {
        ArrayList<Slot> assignedSlotList = null;
        String lockKeyForNodeId = nodeId + SlotManager.class;
        synchronized (lockKeyForNodeId.intern()) {
            HashmapStringListWrapper wrapper = slotAssignmentMap.get(nodeId);
            HashMap<String, List<Slot>> queueToSlotMap = null;
            if(wrapper!=null){
               queueToSlotMap = wrapper.getStringListHashMap();
            }
            if (queueToSlotMap != null) {
                assignedSlotList = (ArrayList<Slot>) queueToSlotMap.remove(queueName);
                wrapper.setStringListHashMap(queueToSlotMap);
                slotAssignmentMap.set(nodeId, wrapper);
            }
        }
        if (assignedSlotList != null && !assignedSlotList.isEmpty()) {
            String lockKeyForQueueName = queueName + SlotManager.class;
            synchronized (lockKeyForQueueName.intern()) {
                TreeSetSlotWrapper treeSetStringWrapper = unAssignedSlotMap.get(queueName);

                TreeSet<Slot> unAssignedSlotSet = new TreeSet<Slot>();
                if (treeSetStringWrapper!=null) {
                    unAssignedSlotSet= treeSetStringWrapper.getSlotTreeSet();
                } else{
                    treeSetStringWrapper = new TreeSetSlotWrapper();
                }
                if (unAssignedSlotSet == null) {
                    unAssignedSlotSet = new TreeSet<Slot>();
                }
                for (Slot slotToBeReAssigned : assignedSlotList) {
                    //Reassign only if the slot is not empty
                    if (!SlotUtils.checkSlotEmptyFromMessageStore(slotToBeReAssigned)) {
                        unAssignedSlotSet.add(slotToBeReAssigned);
                    }
                }
                treeSetStringWrapper.setSlotTreeSet(unAssignedSlotSet);
                unAssignedSlotMap.set(queueName, treeSetStringWrapper);
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
     *
     * @param queueName name of destination queue
     */
    public void clearAllActiveSlotRelationsToQueue(String queueName) {

        if (null != unAssignedSlotMap) {
            unAssignedSlotMap.remove(queueName);
        }

        if (null != slotIDMap) {
            slotIDMap.remove(queueName);
        }

        // Clear slots assigned to the queue
        if (AndesContext.getInstance().isClusteringEnabled()) {
            String nodeId = HazelcastAgent.getInstance().getNodeId();

            // The requirement here is to clear slot associations for the queue on all nodes.
            List<String> nodeIDs = HazelcastAgent.getInstance().getMembersNodeIDs();

            for (String nodeID : nodeIDs) {
                String lockKey = nodeID + SlotManager.class;

                synchronized (lockKey.intern()) {
                    HashmapStringListWrapper wrapper = slotAssignmentMap.get(nodeId);
                    HashMap<String, List<Slot>> queueToSlotMap = null;
                    if(wrapper != null){
                       queueToSlotMap = wrapper.getStringListHashMap();
                    }
                    if (queueToSlotMap != null) {
                        queueToSlotMap.remove(queueName);
                        wrapper.setStringListHashMap(queueToSlotMap);
                        slotAssignmentMap.set(nodeId, wrapper);
                    }
                }
            }
        }

    }
}
