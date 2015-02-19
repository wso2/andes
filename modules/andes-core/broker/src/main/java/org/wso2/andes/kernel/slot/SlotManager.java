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

package org.wso2.andes.kernel.slot;

import com.hazelcast.core.IMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;
import org.wso2.andes.server.cluster.coordination.hazelcast.custom.serializer.wrapper.HashmapStringTreeSetWrapper;


import org.wso2.andes.server.cluster.coordination.hazelcast.custom.serializer.wrapper.TreeSetLongWrapper;
import org.wso2.andes.server.cluster.coordination.hazelcast.custom.serializer.wrapper.TreeSetSlotWrapper;


import java.util.*;

/**
 * Slot Manager is responsible of slot allocating, slot creating, slot re-assigning and slot
 * managing tasks
 */
public class SlotManager {


    private static SlotManager slotManager = new SlotManager();

    private static final int SAFE_ZONE_EVALUATION_INTERVAL = 5*1000;

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
     * To keep track of last published ID against node
     */
    private IMap<String, Long> nodeToLastPublishedIDMap;

    /**
     * To keep track of assigned slots up to now. Key of the map contains nodeID. Value is
     * HashmapStringListWrapper object. HashmapStringListWrapper is a wrapper class for
     * HashMap<String,List<Slot>>. Key in that hash map is queue name. value is List of Slot
     * objects.
     */
    private IMap<String, HashmapStringTreeSetWrapper> slotAssignmentMap;

    private IMap<String, HashmapStringTreeSetWrapper> overLappedSlotMap;

    private Map<String, Long> nodeInformedSlotDeletionSafeZones;

    private SlotDeleteSafeZoneCalc slotDeleteSafeZoneCalc;

    private static Log log = LogFactory.getLog(SlotManager.class);


    private SlotManager() {
        if (AndesContext.getInstance().isClusteringEnabled()) {
            HazelcastAgent hazelcastAgent = HazelcastAgent.getInstance();
            /**
             * Initialize distributed maps used in this class
             */
            unAssignedSlotMap = hazelcastAgent.getUnAssignedSlotMap();
            overLappedSlotMap = hazelcastAgent.getOverLappedSlotMap();
            slotIDMap = hazelcastAgent.getSlotIdMap();
            queueToLastAssignedIDMap = hazelcastAgent.getLastAssignedIDMap();
            nodeToLastPublishedIDMap = hazelcastAgent.getLastPublishedIDMap();
            slotAssignmentMap = hazelcastAgent.getSlotAssignmentMap();
            nodeInformedSlotDeletionSafeZones = new HashMap<String, Long>();
            //start a thread to calculate slot delete safe zone
            slotDeleteSafeZoneCalc = new SlotDeleteSafeZoneCalc(SAFE_ZONE_EVALUATION_INTERVAL);

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
            slotToBeAssigned = getOverlappedSlot(nodeId, queueName);

            if (null == slotToBeAssigned) {
                slotToBeAssigned = getUnassignedSlot(queueName);
            }
            if (null == slotToBeAssigned) {
                slotToBeAssigned = getFreshSlot(queueName);
            }
        }

        if (null != slotToBeAssigned) {
            updateSlotAssignmentMap(queueName, slotToBeAssigned, nodeId);
            log.info("FIXX : Assigning slot for node : " + nodeId + " ||| " + slotToBeAssigned);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Slot Manager - returns empty slot for the queue: " + queueName);
            }
        }

        return slotToBeAssigned;

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
                            "values in " + "map " + messageIDSet);
                }
                queueToLastAssignedIDMap.set(queueName, slotToBeAssigned.getEndMessageId());
                if (log.isDebugEnabled()) {
                    log.debug("Slot Manager - giving a slot from fresh pool. Slot= " + slotToBeAssigned);
                }
                log.info("Slot Manager - giving a slot from fresh pool. Slot= " + slotToBeAssigned);
            }
        }
        return slotToBeAssigned;

    }

    /**
     * Get an unassigned slot (slots dropped by sudden subscription closes)
     * @param queueName name of the queue slot is required
     * @return slot or null if cannot find
     */
    private Slot getUnassignedSlot(String queueName) {
        Slot slotToBeAssigned = null;
        TreeSetSlotWrapper unAssignedSlotWrapper = unAssignedSlotMap.get(queueName);
        if(unAssignedSlotWrapper != null){
            TreeSet<Slot> slotsFromUnassignedSlotMap = unAssignedSlotWrapper.getSlotTreeSet();
            if (slotsFromUnassignedSlotMap != null && !slotsFromUnassignedSlotMap.isEmpty()) {
                slotToBeAssigned = slotsFromUnassignedSlotMap.pollFirst();
                //update hazelcast map
                unAssignedSlotWrapper.setSlotTreeSet(slotsFromUnassignedSlotMap);
                unAssignedSlotMap.set(queueName, unAssignedSlotWrapper);
                if (log.isDebugEnabled()) {
                    log.debug("Slot Manager - giving a slot from unAssignedSlotMap. Slot= " + slotToBeAssigned);
                }
                log.info("Slot Manager - giving a slot from unAssignedSlotMap. Slot= " + slotToBeAssigned);
            }
        }
        return slotToBeAssigned;
    }

    /**
     * Get an overlapped slot by nodeId and the queue name. These are slots
     * which are overlapped with some slots that were acquired by given node
     * @param nodeId id of the node
     * @param queueName  name of the queue slot is required
     * @return  slot or null if not found
     */
    private Slot getOverlappedSlot(String nodeId, String queueName) {
        //TODO: for this lock key should be by nodeID?
        Slot slotToBeAssigned = null;
        TreeSet<Slot> currentSlotList;
        HashMap<String, TreeSet<Slot>> queueToSlotMap;
        HashmapStringTreeSetWrapper wrapper = overLappedSlotMap.get(nodeId);

        if(null != wrapper) {
            queueToSlotMap = wrapper.getStringListHashMap();
            currentSlotList = queueToSlotMap.get(queueName);
            if (null != currentSlotList && !currentSlotList.isEmpty()) {
                slotToBeAssigned = currentSlotList.pollFirst();
                queueToSlotMap.put(queueName, currentSlotList);
                wrapper.setStringListHashMap(queueToSlotMap);
                overLappedSlotMap.set(nodeId, wrapper);
                //if (log.isDebugEnabled()) {
                    log.info("Slot Manager - giving a slot from overlapped slot pool. Slot= " + slotToBeAssigned);
                //}
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
        TreeSet<Slot> currentSlotList;
        HashMap<String, TreeSet<Slot>> queueToSlotMap;
        HashmapStringTreeSetWrapper wrapper = slotAssignmentMap.get(nodeId);
        if (wrapper == null) {
            wrapper = new HashmapStringTreeSetWrapper();
            queueToSlotMap = new HashMap<String, TreeSet<Slot>>();
            wrapper.setStringListHashMap(queueToSlotMap);
            slotAssignmentMap.putIfAbsent(nodeId, wrapper);
        }
        //Lock is used because this method will be called by multiple nodes at the same time
        String lockKey = nodeId + SlotManager.class;
        synchronized (lockKey.intern()) {
            wrapper = slotAssignmentMap.get(nodeId);
            queueToSlotMap = wrapper.getStringListHashMap();
            currentSlotList = queueToSlotMap.get(queueName);
            if (currentSlotList == null) {
                currentSlotList = new TreeSet<Slot>();
            }
            currentSlotList.add(allocatedSlot);
            queueToSlotMap.put(queueName, currentSlotList);
            allocatedSlot.addState(SlotState.ASSIGNED);
            wrapper.setStringListHashMap(queueToSlotMap);
            slotAssignmentMap.set(nodeId, wrapper);
        }

    }


    /**
     * Record Slot's last message ID related to a particular queue
     *
     * @param queueName name of the queue which this message ID belongs to
     * @param lastMessageIdInTheSlot  last message ID of the slot
     * @param startMessageIdInTheSlot start message ID of the slot
     * @param nodeId Node ID of the node that is sending the request.
     */
    public void updateMessageID(String queueName, String nodeId, long startMessageIdInTheSlot, long lastMessageIdInTheSlot) {

        TreeSet<Long> messageIdSet = new TreeSet<Long>();
        TreeSetLongWrapper wrapper= slotIDMap.get(queueName);
        if (wrapper == null) {
            wrapper = new TreeSetLongWrapper();
            wrapper.setLongTreeSet(messageIdSet);
            slotIDMap.putIfAbsent(queueName, wrapper);
        }
        // Read message Id set for slots
        messageIdSet = wrapper.getLongTreeSet();

        String lockKey = queueName + SlotManager.class;
        synchronized (lockKey.intern()) {

            Long lastAssignedMessageId = queueToLastAssignedIDMap.get(queueName);

            // Check if input slot's start message ID is less than ast assigned message ID
            if ((null != lastAssignedMessageId) && startMessageIdInTheSlot < lastAssignedMessageId) {
                log.info("FIXX : " + "Found an overlapping slot : " + startMessageIdInTheSlot + " to : " + lastMessageIdInTheSlot + ". Comparing to lastAssignedID : " +lastAssignedMessageId );
                // Find overlapping slots
                TreeSet<Slot> overlappingSlots = getOverlappedAssignedSlots(queueName,startMessageIdInTheSlot,lastMessageIdInTheSlot);

                if (overlappingSlots.size() > 0) {
                    log.info("FIXX : " + "Found " + overlappingSlots.size() + " overlapping slots.");
                    if (startMessageIdInTheSlot < overlappingSlots.first().getStartMessageId()) {
                        // This means that we have a piece of the slot exceeding the earliest assigned slot.
                        // breaking that piece and adding it as a new,unassigned slot.
                        Slot leftExtraSlot = new Slot(startMessageIdInTheSlot, overlappingSlots.first().getStartMessageId()-1, queueName);
                        //TODO add to collection
                        log.info("FIXX : " + "LeftExtra in overlapping slot : " + leftExtraSlot);
                    }
                    if (lastMessageIdInTheSlot > overlappingSlots.last().getEndMessageId()) {
                        // This means that we have a piece of the slot exceeding the latest assigned slot.
                        // breaking that piece and adding it as a new,unassigned slot.
                        Slot rightExtraSlot = new Slot(overlappingSlots.last().getEndMessageId()+1,lastMessageIdInTheSlot,queueName);

                        log.info("FIXX : " + "RightExtra in overlapping slot : " + rightExtraSlot);
                        //Update last message ID - expand ongoing slot to cater this leftover part.
                        messageIdSet.add(lastMessageIdInTheSlot);
                        wrapper.setLongTreeSet(messageIdSet);
                        slotIDMap.set(queueName, wrapper);
                        nodeToLastPublishedIDMap.set(nodeId,lastMessageIdInTheSlot);
                        //if (log.isDebugEnabled()) {
                            log.info(lastMessageIdInTheSlot + " added to slotIdMap (RightExtraSlot). Current values in " +
                                    "map " + messageIdSet);
                        //}
                    }

                    //Add newly found overlaps to global overlapping slots tree.
                    if (!overLappedSlotMap.containsKey(nodeId)) {
                        overLappedSlotMap.put(nodeId,new HashmapStringTreeSetWrapper());
                        log.info("FIXX : overlappedSlotMap before add queue entry : " + overLappedSlotMap.get(nodeId));
                    }
                    HashmapStringTreeSetWrapper olWrapper = overLappedSlotMap.get(nodeId);
                    HashMap<String,TreeSet<Slot>> olSlotMap = olWrapper.getStringListHashMap();

                    if (!olSlotMap.containsKey(queueName)) {
                        olSlotMap.put(queueName,overlappingSlots);
                        olWrapper.setStringListHashMap(olSlotMap);
                        overLappedSlotMap.set(nodeId,olWrapper);
                        log.info("FIXX : overlappedSlotMap created slots : " + overLappedSlotMap.get(nodeId));
                    } else {
                        olSlotMap.get(queueName).addAll(overlappingSlots);
                        olWrapper.setStringListHashMap(olSlotMap);
                        overLappedSlotMap.set(nodeId, olWrapper);
                        log.info("FIXX : overlappedSlotMap updated slots : " + overLappedSlotMap.get(nodeId));
                    }

                }
                //log.info("FIXX : Going out of updateMessageID : overlapping slot search");
            } else {
                /**
                 * Update the slotIDMap only if the last assigned message ID is less than the new start message ID
                 */
                messageIdSet.add(lastMessageIdInTheSlot);
                wrapper.setLongTreeSet(messageIdSet);
                slotIDMap.set(queueName, wrapper);
                nodeToLastPublishedIDMap.set(nodeId,lastMessageIdInTheSlot);
                //if (log.isDebugEnabled()) {
                    log.info(lastMessageIdInTheSlot + " added to slotIdMap. Current values in " +
                            "map " + messageIdSet);
                //}
            }
        }

        //log.info("FIXX : Going out of synchronized block : UpdateMessageID");

    }

    /**
     * This method will reassigned slots which are owned by a node to a free slots pool
     *
     * @param nodeId node ID of the leaving node
     */
    public void reAssignSlotsWhenMemberLeaves(String nodeId) {
        //Remove the entry from slot assignment map
        HashmapStringTreeSetWrapper wrapper = slotAssignmentMap.remove(nodeId);
        HashMap<String, TreeSet<Slot>> queueToSlotMap = null;
        if (wrapper!=null) {
            queueToSlotMap = wrapper.getStringListHashMap();
        }
        if (queueToSlotMap!= null) {
            for (Map.Entry<String, TreeSet<Slot>> entry : queueToSlotMap.entrySet()) {
                TreeSet<Slot> slotsToBeReAssigned = entry.getValue();
                TreeSet<Slot> freeSlotTreeSet = new TreeSet<Slot>();
                TreeSetSlotWrapper treeSetStringWrapper = new TreeSetSlotWrapper();
                //TODO: improve the method to update the wrapper with assigned slots and set it in one go
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
                            slotToBeReAssigned.addState(SlotState.RETURNED);
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
    public boolean deleteSlot(String queueName, Slot emptySlot, String nodeId) {
        long startMsgId = emptySlot.getStartMessageId();
        long slotDeleteSafeZone = getSlotDeleteSafeZone();
        log.info("Trying to delete slot. safeZone= " + getSlotDeleteSafeZone() + " startMsgID= " + startMsgId);
        if (slotDeleteSafeZone > startMsgId) {
            String lockKey = nodeId + SlotManager.class;
            synchronized (lockKey.intern()) {
                HashMap<String, TreeSet<Slot>> queueToSlotMap = null;
                HashmapStringTreeSetWrapper wrapper = slotAssignmentMap.get(nodeId);
                if (wrapper != null) {
                    queueToSlotMap = wrapper.getStringListHashMap();
                }
                if (queueToSlotMap != null) {
                    TreeSet<Slot> currentSlotList = queueToSlotMap.get
                            (queueName);
                    if (currentSlotList != null) {
                        // com.google.gson.Gson gson = new GsonBuilder().create();
                        //get the actual reference of the slot to be removed
                        Slot slotInAssignmentMap = currentSlotList.ceiling(emptySlot);
                        slotInAssignmentMap.addState(SlotState.DELETED);
                        currentSlotList.remove(emptySlot);
                        queueToSlotMap.put(queueName, currentSlotList);
                        wrapper.setStringListHashMap(queueToSlotMap);
                        slotAssignmentMap.set(nodeId, wrapper);
                        log.info("Deleted slot " + slotInAssignmentMap);
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("Unassigned slot " + emptySlot.getStartMessageId() + " - " +
                                emptySlot.getEndMessageId() + "owned by node: " + nodeId + "");
                    }
                }
            }
            return true;
        } else {
            log.info("Cannot delete slot as it is within safe zone startMsgID= " + startMsgId + "" +
                    " safeZone= " + slotDeleteSafeZone + " slotToDelete= " + emptySlot);
            return false;
        }
    }

    /**
     * Re-assign the slot when there are no local subscribers in the node
     *
     * @param nodeId    node ID of the node without subscribers
     * @param queueName name of the queue whose slots to be reassigned
     */
    public void reAssignSlotWhenNoSubscribers(String nodeId, String queueName) {
        TreeSet<Slot> assignedSlotList = null;
        String lockKeyForNodeId = nodeId + SlotManager.class;
        synchronized (lockKeyForNodeId.intern()) {
            HashmapStringTreeSetWrapper wrapper = slotAssignmentMap.get(nodeId);
            HashMap<String, TreeSet<Slot>> queueToSlotMap = null;
            if(wrapper!=null){
               queueToSlotMap = wrapper.getStringListHashMap();
            }
            if (queueToSlotMap != null) {
                assignedSlotList = queueToSlotMap.remove(queueName);
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
                        slotToBeReAssigned.addState(SlotState.RETURNED);
                    }
                }
                treeSetStringWrapper.setSlotTreeSet(unAssignedSlotSet);
                unAssignedSlotMap.set(queueName, treeSetStringWrapper);
            }
        }
    }


    protected Map<String, HashMap<String, TreeSet<Slot>>> getAllAssignedSlotInfo() {
        Map<String, HashMap<String, TreeSet<Slot>>> slotAssignmentMap =
                new HashMap<String, HashMap<String, TreeSet<Slot>>>();

        List<String> nodeIDs = HazelcastAgent.getInstance().getMembersNodeIDs();

        for (String nodeID : nodeIDs) {
            String lockKey = nodeID + SlotManager.class;

            synchronized (lockKey.intern()) {
                HashmapStringTreeSetWrapper wrapper = this.slotAssignmentMap.get(nodeID);
                HashMap<String, TreeSet<Slot>> queueToSlotMap = null;
                if(wrapper != null){
                    queueToSlotMap = wrapper.getStringListHashMap();
                }
                if (queueToSlotMap != null) {
                    slotAssignmentMap.put(nodeID, queueToSlotMap);
                }
            }
        }

        return slotAssignmentMap;
    }

    protected Map<String, Long> getNodeInformedSlotDeletionSafeZones() {
        return nodeInformedSlotDeletionSafeZones;
    }

    protected long getLastPublishedIDByNode(String nodeID) {
        return nodeToLastPublishedIDMap.get(nodeID);
    }

    protected Set<String> getMessagePublishedNodes() {
        return nodeToLastPublishedIDMap.keySet();
    }

    public long getSlotDeleteSafeZone() {
        return slotDeleteSafeZoneCalc.getSlotDeleteSafeZone();
    }

    public long updateAndReturnSlotDeleteSafeZone(String nodeID, long safeZoneOfNode) {
        nodeInformedSlotDeletionSafeZones.put(nodeID, safeZoneOfNode);
        return slotDeleteSafeZoneCalc.getSlotDeleteSafeZone();
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
                    HashmapStringTreeSetWrapper wrapper = slotAssignmentMap.get(nodeId);
                    HashMap<String, TreeSet<Slot>> queueToSlotMap = null;
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

    public void shutDownSlotManager() {
        slotDeleteSafeZoneCalc.setRunning(false);
    }

    /**
     * Get an ordered set of existing, assigned slots that overlap with the input slot range.
     * @param queueName name of destination queue
     * @param startMsgID start message ID of input slot
     * @param endMsgID end message ID of input slot
     * @return TreeSet<Slot>
     */
    private TreeSet<Slot> getOverlappedAssignedSlots(String queueName, long startMsgID, long endMsgID) {
        TreeSet<Slot> overlappedSlots = new TreeSet<Slot>();

        // Sweep all assigned slots to find overlaps.
        //TODO add safe zone to filter certain slots.
        //TODO Can maintain the slot list as a seperate collection to avoid iterating over slotAssignmentMap, cos its optimized for node,queue-wise iteration.
        if (AndesContext.getInstance().isClusteringEnabled()) {
            String nodeId = HazelcastAgent.getInstance().getNodeId();

            // The requirement here is to clear slot associations for the queue on all nodes.
            List<String> nodeIDs = HazelcastAgent.getInstance().getMembersNodeIDs();

            for (String nodeID : nodeIDs) {
                String lockKey = nodeID + SlotManager.class;

                synchronized (lockKey.intern()) {
                    HashmapStringTreeSetWrapper wrapper = slotAssignmentMap.get(nodeId);
                    if(wrapper != null) {
                        HashMap<String, TreeSet<Slot>> queueToSlotMap = wrapper.getStringListHashMap();
                        if (queueToSlotMap != null) {
                            TreeSet<Slot> slotListForQueueOnNode = queueToSlotMap.get(queueName);
                            if (null != slotListForQueueOnNode) {
                                for (Slot slot : slotListForQueueOnNode) {
                                    if (endMsgID < slot.getStartMessageId())
                                        continue; // skip this one, its below our range
                                    if (startMsgID > slot.getEndMessageId())
                                        continue; // skip this one, its above our range
                                    slot.setAnOverlappingSlot(true);
                                    overlappedSlots.add(slot);
                                }
                            }
                        }
                        wrapper.setStringListHashMap(queueToSlotMap);
                        slotAssignmentMap.set(nodeId, wrapper);
                    }
                }
            }
        }

        return overlappedSlots;
    }

}
