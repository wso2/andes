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

import com.hazelcast.core.HazelcastInstanceNotActiveException;
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
 * Slot Manager Cluster Mode is responsible of slot allocating, slot creating,
 * slot re-assigning and slot managing tasks in cluster mode
 */
public class SlotManagerClusterMode {


    private static SlotManagerClusterMode slotManager = new SlotManagerClusterMode();

    private static final int SAFE_ZONE_EVALUATION_INTERVAL = 5 * 1000;

    private static final String HAZELCAST_INACTIVE_WARNING = "Hazelcast instance is not active. Could not proceed with ";

    private static final String HAZELCAST_INACTIVE_DURING_SHUTDOWN = "The server is shutting down and therefore the cluster is inactive at the moment for ";

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

    /**
     * HazelCast map keeping overlapped slots. Overlapped slots are eligible to be assigned again.
     * Key of the map contains nodeID. Value is
     * HashMapStringListWrapper object. HashMapStringListWrapper is a wrapper class for
     * HashMap<String,List<Slot>>. Key in that hash map is queue name. value is List of Slot
     * objects.
     */
    private IMap<String, HashmapStringTreeSetWrapper> overLappedSlotMap;

    /**
     * Keeps slot deletion safe zones for each node, as they are informed by nodes via thrift
     * communication. Used for safe zone calculation for cluster by Slot Manager
     */
    private Map<String, Long> nodeInformedSlotDeletionSafeZones;

    //safe zone calculator
    private SlotDeleteSafeZoneCalc slotDeleteSafeZoneCalc;

    private static Log log = LogFactory.getLog(SlotManagerClusterMode.class);


    private SlotManagerClusterMode() {

        HazelcastAgent hazelcastAgent = HazelcastAgent.getInstance();

        //Initialize distributed maps used in this class
        unAssignedSlotMap = hazelcastAgent.getUnAssignedSlotMap();
        overLappedSlotMap = hazelcastAgent.getOverLappedSlotMap();
        slotIDMap = hazelcastAgent.getSlotIdMap();
        queueToLastAssignedIDMap = hazelcastAgent.getLastAssignedIDMap();
        nodeToLastPublishedIDMap = hazelcastAgent.getLastPublishedIDMap();
        slotAssignmentMap = hazelcastAgent.getSlotAssignmentMap();

        nodeInformedSlotDeletionSafeZones = new HashMap<String, Long>();

        //start a thread to calculate slot delete safe zone
        //TODO: use a common  thread pool for tasks like this?
        slotDeleteSafeZoneCalc = new SlotDeleteSafeZoneCalc(SAFE_ZONE_EVALUATION_INTERVAL);
        new Thread(slotDeleteSafeZoneCalc).start();

    }

    /**
     * @return SlotManagerClusterMode instance
     */
    public static SlotManagerClusterMode getInstance() {
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
        String lockKey = queueName + SlotManagerClusterMode.class;

        synchronized (lockKey.intern()) {
            slotToBeAssigned = getUnassignedSlot(queueName);

            if (null == slotToBeAssigned) {
                slotToBeAssigned = getOverlappedSlot(nodeId, queueName);
            }
            if (null == slotToBeAssigned) {
                slotToBeAssigned = getFreshSlot(queueName);
            }
        }

        if (null != slotToBeAssigned) {
            updateSlotAssignmentMap(queueName, slotToBeAssigned, nodeId);
            if (log.isDebugEnabled()) {
                log.debug("Assigning slot for node : " + nodeId + " ||| " + slotToBeAssigned);
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Slot Manager - returns empty slot for the queue: " + queueName);
            }
        }

        return slotToBeAssigned;

    }

    /**
     * Create a new slot from slotIDMap
     *
     * @param queueName name of the queue
     * @return slot object
     */
    private Slot getFreshSlot(String queueName) {
        Slot slotToBeAssigned = null;
        TreeSet<Long> messageIDSet;
        try {
            TreeSetLongWrapper wrapper = slotIDMap.get(queueName);
            if (null != wrapper) {
                messageIDSet = wrapper.getLongTreeSet();
                if (messageIDSet != null && !messageIDSet.isEmpty()) {

                    //create a new slot
                    slotToBeAssigned = new Slot();

                    //start msgID will be last assigned ID + 1 so that slots are created with no
                    // message ID gaps in-between
                    Long lastAssignedId = queueToLastAssignedIDMap.get(queueName);
                    if (lastAssignedId != null) {
                        slotToBeAssigned.setStartMessageId(lastAssignedId + 1);
                    } else {
                        slotToBeAssigned.setStartMessageId(0L);
                    }

                    //end messageID will be the lowest in published message ID list. Get and remove
                    slotToBeAssigned.setEndMessageId(messageIDSet.pollFirst());

                    //set storage queue name (db queue to read messages from)
                    slotToBeAssigned.setStorageQueueName(queueName);

                    //set modified published ID map to hazelcast
                    wrapper.setLongTreeSet(messageIDSet);
                    slotIDMap.set(queueName, wrapper);

                    //modify last assigned ID by queue to hazelcast
                    queueToLastAssignedIDMap.set(queueName, slotToBeAssigned.getEndMessageId());

                    if (log.isDebugEnabled()) {
                        log.debug("Slot Manager - giving a slot from fresh pool. Slot= " + slotToBeAssigned);
                    }
                }
            }
        } catch (HazelcastInstanceNotActiveException ex) {
            if (AndesKernelBoot.isKernelShuttingDown()) {
                if (log.isDebugEnabled()) {
                    log.debug(HAZELCAST_INACTIVE_DURING_SHUTDOWN + "getFreshSlot for queue : " + queueName);
                }
            }
            log.error(HAZELCAST_INACTIVE_WARNING + "getFreshSlot for queue : " + queueName);
        }
        return slotToBeAssigned;

    }

    /**
     * Get an unassigned slot (slots dropped by sudden subscription closes)
     *
     * @param queueName name of the queue slot is required
     * @return slot or null if cannot find
     */
    private Slot getUnassignedSlot(String queueName) {
        Slot slotToBeAssigned = null;
        try {

            String lockKey = queueName + SlotManagerClusterMode.class;

            synchronized (lockKey.intern()) {

                TreeSetSlotWrapper unAssignedSlotWrapper = unAssignedSlotMap.get(queueName);

                if (null != unAssignedSlotWrapper) {
                    TreeSet<Slot> slotsFromUnassignedSlotMap = unAssignedSlotWrapper.getSlotTreeSet();
                    if (slotsFromUnassignedSlotMap != null && !slotsFromUnassignedSlotMap.isEmpty()) {

                        //Get and remove slot and update hazelcast map
                        slotToBeAssigned = slotsFromUnassignedSlotMap.pollFirst();
                        unAssignedSlotWrapper.setSlotTreeSet(slotsFromUnassignedSlotMap);
                        unAssignedSlotMap.set(queueName, unAssignedSlotWrapper);

                        if (log.isDebugEnabled()) {
                            log.debug("Slot Manager - giving a slot from unAssignedSlotMap. Slot= " + slotToBeAssigned);
                        }
                    }
                }
            }
        } catch (HazelcastInstanceNotActiveException ex) {
            if (AndesKernelBoot.isKernelShuttingDown()) {
                if (log.isDebugEnabled()) {
                    log.debug(HAZELCAST_INACTIVE_DURING_SHUTDOWN + "getUnassignedSlot for queue : " + queueName);
                }
            }
            log.error(HAZELCAST_INACTIVE_WARNING + "getUnassignedSlot for queue : " + queueName);
        }
        return slotToBeAssigned;
    }

    /**
     * Get an overlapped slot by nodeId and the queue name. These are slots
     * which are overlapped with some slots that were acquired by given node
     *
     * @param nodeId    id of the node
     * @param queueName name of the queue slot is required
     * @return slot or null if not found
     */
    private Slot getOverlappedSlot(String nodeId, String queueName) {
        Slot slotToBeAssigned = null;
        TreeSet<Slot> currentSlotList;
        HashMap<String, TreeSet<Slot>> queueToSlotMap;
        try {
            HashmapStringTreeSetWrapper wrapper = overLappedSlotMap.get(nodeId);

            String lockKey = nodeId + SlotManagerClusterMode.class;

            synchronized (lockKey.intern()) {
                if (null != wrapper) {
                    queueToSlotMap = wrapper.getStringListHashMap();
                    currentSlotList = queueToSlotMap.get(queueName);
                    if (null != currentSlotList && !currentSlotList.isEmpty()) {
                        //get and remove slot
                        slotToBeAssigned = currentSlotList.pollFirst();
                        queueToSlotMap.put(queueName, currentSlotList);
                        //update hazelcast map
                        wrapper.setStringListHashMap(queueToSlotMap);
                        overLappedSlotMap.set(nodeId, wrapper);
                        if (log.isDebugEnabled()) {
                            log.debug("Slot Manager - giving a slot from overlapped slot pool. Slot= " + slotToBeAssigned);
                        }
                    }
                }
            }
        } catch (HazelcastInstanceNotActiveException ex) {
            if (AndesKernelBoot.isKernelShuttingDown()) {
                if (log.isDebugEnabled()) {
                    log.debug(HAZELCAST_INACTIVE_DURING_SHUTDOWN + "getOverlappedSlot for queue : " + queueName + " from node " + nodeId);
                }
            }
            log.error(HAZELCAST_INACTIVE_WARNING + "getOverlappedSlot for queue : " + queueName + " from node " + nodeId);
        }
        return slotToBeAssigned;
    }


    /**
     * Update the slot assignment map when a slot is assigned for a node
     *
     * @param queueName     Name of the queue
     * @param allocatedSlot Slot object which is allocated to a particular node
     * @param nodeId        ID of the node to which slot is Assigned
     */
    private void updateSlotAssignmentMap(String queueName, Slot allocatedSlot, String nodeId) {
        TreeSet<Slot> currentSlotList;
        HashMap<String, TreeSet<Slot>> queueToSlotMap;

        //Lock is used because this method will be called by multiple nodes at the same time
        String lockKey = nodeId + SlotManagerClusterMode.class;
        synchronized (lockKey.intern()) {

            HashmapStringTreeSetWrapper wrapper = slotAssignmentMap.get(nodeId);
            if (wrapper == null) {
                wrapper = new HashmapStringTreeSetWrapper();
                queueToSlotMap = new HashMap<String, TreeSet<Slot>>();
                wrapper.setStringListHashMap(queueToSlotMap);
                slotAssignmentMap.putIfAbsent(nodeId, wrapper);
            }

            wrapper = slotAssignmentMap.get(nodeId);
            queueToSlotMap = wrapper.getStringListHashMap();
            currentSlotList = queueToSlotMap.get(queueName);
            if (currentSlotList == null) {
                currentSlotList = new TreeSet<Slot>();
            }

            //update slot state
            if (allocatedSlot.addState(SlotState.ASSIGNED)) {
                //remove any similar slot from hazelcast and add the updated one
                currentSlotList.remove(allocatedSlot);
                currentSlotList.add(allocatedSlot);
                queueToSlotMap.put(queueName, currentSlotList);
                wrapper.setStringListHashMap(queueToSlotMap);
                slotAssignmentMap.set(nodeId, wrapper);
            }
        }
    }


    /**
     * Record Slot's last message ID related to a particular queue
     *
     * @param queueName               name of the queue which this message ID belongs to
     * @param lastMessageIdInTheSlot  last message ID of the slot
     * @param startMessageIdInTheSlot start message ID of the slot
     * @param nodeId                  Node ID of the node that is sending the request.
     */
    public void updateMessageID(String queueName, String nodeId, long startMessageIdInTheSlot, long lastMessageIdInTheSlot) {

        try {
            // Read message Id set for slots from hazelcast
            TreeSet<Long> messageIdSet;
            TreeSetLongWrapper wrapper = slotIDMap.get(queueName);
            if (wrapper == null) {
                wrapper = new TreeSetLongWrapper();
                slotIDMap.putIfAbsent(queueName, wrapper);
            }
            messageIdSet = wrapper.getLongTreeSet();

            String lockKey = queueName + SlotManagerClusterMode.class;
            synchronized (lockKey.intern()) {

                Long lastAssignedMessageId = queueToLastAssignedIDMap.get(queueName);

                // Check if input slot's start message ID is less than last assigned message ID
                if ((null != lastAssignedMessageId) && startMessageIdInTheSlot < lastAssignedMessageId) {

                    if(log.isDebugEnabled()) {
                        log.debug("Found overlapping slots during slot submit: " +
                                startMessageIdInTheSlot + " to : " + lastMessageIdInTheSlot +
                                ". Comparing to lastAssignedID : " + lastAssignedMessageId);
                    }

                    // Find overlapping slots
                    TreeSet<Slot> overlappingSlots = getOverlappedAssignedSlots(queueName, startMessageIdInTheSlot,
                            lastMessageIdInTheSlot);

                    if (overlappingSlots.size() > 0) {

                        if(log.isDebugEnabled()) {
                            log.debug("Found " + overlappingSlots.size() + " overlapping slots.");
                        }

                        // Following means that we have a piece of the slot exceeding the earliest
                        // assigned slot. breaking that piece and adding it as a new,unassigned slot.
                        if (startMessageIdInTheSlot < overlappingSlots.first().getStartMessageId()) {
                            if(log.isDebugEnabled()) {
                                Slot leftExtraSlot = new Slot(startMessageIdInTheSlot, overlappingSlots.first().
                                        getStartMessageId() - 1, queueName);
                                log.debug("LeftExtra Slot in overlapping slots : " + leftExtraSlot);
                            }
                        }

                        // This means that we have a piece of the slot exceeding the latest assigned slot.
                        // breaking that piece and adding it as a new,unassigned slot.
                        if (lastMessageIdInTheSlot > overlappingSlots.last().getEndMessageId()) {
                            if(log.isDebugEnabled()) {
                                Slot rightExtraSlot = new Slot(overlappingSlots.last().getEndMessageId() + 1,
                                        lastMessageIdInTheSlot, queueName);
                                log.debug("RightExtra in overlapping slot : " + rightExtraSlot);
                            }

                            //Update last message ID - expand ongoing slot to cater this leftover part.
                            messageIdSet.add(lastMessageIdInTheSlot);
                            wrapper.setLongTreeSet(messageIdSet);
                            slotIDMap.set(queueName, wrapper);
                            if (log.isDebugEnabled()) {
                                log.debug(lastMessageIdInTheSlot + " added to slotIdMap " +
                                       "(RightExtraSlot). Current values in " +
                                    "map " + messageIdSet);
                            }

                            //record last published message id
                            nodeToLastPublishedIDMap.set(nodeId, lastMessageIdInTheSlot);
                        }



                    }

                } else {
                    /**
                     * Update the slotIDMap only if the last assigned message ID is less than the new start message ID
                     */
                    messageIdSet.add(lastMessageIdInTheSlot);
                    wrapper.setLongTreeSet(messageIdSet);
                    slotIDMap.set(queueName, wrapper);
                    if (log.isDebugEnabled()) {
                        log.debug("No overlapping slots found during slot submit "+ startMessageIdInTheSlot + " to : " +
                                lastMessageIdInTheSlot+". Added msgID " +
                                lastMessageIdInTheSlot + " to slotIDMap");
                    }

                    //record last published message ID
                    nodeToLastPublishedIDMap.set(nodeId, lastMessageIdInTheSlot);
                }
            }
        } catch (HazelcastInstanceNotActiveException ex) {
            if (AndesKernelBoot.isKernelShuttingDown()) {
                if (log.isDebugEnabled()) {
                    log.debug(HAZELCAST_INACTIVE_DURING_SHUTDOWN + "updateMessageID for queue : " + queueName + " from node " + nodeId);
                }
            }
            log.error(HAZELCAST_INACTIVE_WARNING + "updateMessageID for queue : " + queueName + " from node " + nodeId);
        }
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
        if (null != wrapper) {
            queueToSlotMap = wrapper.getStringListHashMap();
        }
        if (queueToSlotMap != null) {
            for (Map.Entry<String, TreeSet<Slot>> entry : queueToSlotMap.entrySet()) {
                TreeSet<Slot> slotsToBeReAssigned = entry.getValue();
                TreeSet<Slot> freeSlotTreeSet = new TreeSet<Slot>();
                TreeSetSlotWrapper treeSetStringWrapper = new TreeSetSlotWrapper();

                for (Slot slotToBeReAssigned : slotsToBeReAssigned) {

                    //Re-assign only if the slot is not empty
                    if (!SlotUtils.checkSlotEmptyFromMessageStore(slotToBeReAssigned)) {
                        treeSetStringWrapper.setSlotTreeSet(freeSlotTreeSet);
                        unAssignedSlotMap.putIfAbsent(slotToBeReAssigned.getStorageQueueName(),
                                treeSetStringWrapper);
                        //Lock key is queueName + SlotManagerClusterMode Class
                        String lockKey = entry.getKey() + SlotManagerClusterMode.class;
                        synchronized (lockKey.intern()) {
                            if (slotToBeReAssigned.addState(SlotState.RETURNED)) {
                                treeSetStringWrapper = unAssignedSlotMap
                                        .get(slotToBeReAssigned.getStorageQueueName());
                                freeSlotTreeSet = treeSetStringWrapper.getSlotTreeSet();
                                //String jsonSlotString = gson.toJson(slotsToBeReAssigned);
                                freeSlotTreeSet.add(slotToBeReAssigned);
                                treeSetStringWrapper.setSlotTreeSet(freeSlotTreeSet);
                                unAssignedSlotMap
                                        .set(slotToBeReAssigned.getStorageQueueName(), treeSetStringWrapper);

                                if (log.isDebugEnabled()) {
                                    log.debug("Returned slot " + slotToBeReAssigned + "from node " +
                                            nodeId + " as member left");
                                }
                            }
                        }
                    }
                }
            }
        }

        //delete all overlapped slots for the node
        overLappedSlotMap.remove(nodeId);
        if(log.isDebugEnabled()) {
            log.debug("Removed all overlapped slots for node " + nodeId);
        }

    }

    /**
     * Remove slot entry from slotAssignment map
     *
     * @param queueName name of the queue which is owned by the slot to be deleted
     * @param emptySlot reference of the slot to be deleted
     */
    public boolean deleteSlot(String queueName, Slot emptySlot, String nodeId) {

        try {

            long startMsgId = emptySlot.getStartMessageId();
            long endMsgId = emptySlot.getEndMessageId();
            long slotDeleteSafeZone = getSlotDeleteSafeZone();
            if(log.isDebugEnabled()) {
                log.debug("Trying to delete slot. safeZone= " + getSlotDeleteSafeZone() + " startMsgID= "
                        + startMsgId);
            }
            if (slotDeleteSafeZone > endMsgId) {
                String lockKey = nodeId + SlotManagerClusterMode.class;
                synchronized (lockKey.intern()) {
                    HashMap<String, TreeSet<Slot>> queueToSlotMap = null;
                    HashmapStringTreeSetWrapper wrapper = slotAssignmentMap.get(nodeId);
                    if (null != wrapper) {
                        queueToSlotMap = wrapper.getStringListHashMap();
                    }
                    if (queueToSlotMap != null) {
                        TreeSet<Slot> currentSlotList = queueToSlotMap.get
                                (queueName);
                        if (currentSlotList != null) {
                            // com.google.gson.Gson gson = new GsonBuilder().create();
                            //get the actual reference of the slot to be removed
                            Slot slotInAssignmentMap = null; //currentSlotList.ceiling(emptySlot);

                            for (Slot slot : currentSlotList) {
                                if (slot.getStartMessageId() == emptySlot.getStartMessageId()) {
                                    slotInAssignmentMap = slot;
                                }
                            }

                            if (null != slotInAssignmentMap) {
                                if (slotInAssignmentMap.addState(SlotState.DELETED)) {
                                    currentSlotList.remove(slotInAssignmentMap);
                                    queueToSlotMap.put(queueName, currentSlotList);
                                    wrapper.setStringListHashMap(queueToSlotMap);
                                    slotAssignmentMap.set(nodeId, wrapper);
                                    if(log.isDebugEnabled()) {
                                        log.debug("Deleted slot from Slot Assignment Map : Slot= " +
                                                slotInAssignmentMap);
                                    }
                                }
                            }
                        }
                    }
                }
                return true;
            } else {
                if(log.isDebugEnabled()) {
                    log.debug("Cannot delete slot as it is within safe zone startMsgID= " + startMsgId +
                            " safeZone= " + slotDeleteSafeZone + " endMsgId= " + endMsgId + " slotToDelete= " + emptySlot);
                }
            }
        } catch (HazelcastInstanceNotActiveException ex) {
            if (AndesKernelBoot.isKernelShuttingDown()) {
                if (log.isDebugEnabled()) {
                    log.debug(HAZELCAST_INACTIVE_DURING_SHUTDOWN + "deleteSlot" + emptySlot + " for queue : " + queueName + " from node " + nodeId);
                }
            }
            log.error(HAZELCAST_INACTIVE_WARNING + "deleteSlot" + emptySlot + " for queue : " + queueName + " from node " + nodeId);
        }

        return false;
    }

    /**
     * Re-assign the slot when there are no local subscribers in the node
     *
     * @param nodeId    node ID of the node without subscribers
     * @param queueName name of the queue whose slots to be reassigned
     */
    public void reAssignSlotWhenNoSubscribers(String nodeId, String queueName) {

        // Hazelcast could be shut down before hitting this method in case of a server shutdown event. Therefore the check.
        try {
            TreeSet<Slot> assignedSlotList = null;
            String lockKeyForNodeId = nodeId + SlotManagerClusterMode.class;
            synchronized (lockKeyForNodeId.intern()) {

                //Get assigned slots from Hazelcast, delete all belonging to queue
                //and set back
                HashmapStringTreeSetWrapper wrapper = slotAssignmentMap.get(nodeId);
                HashMap<String, TreeSet<Slot>> queueToSlotMap = null;
                if (null != wrapper) {
                    queueToSlotMap = wrapper.getStringListHashMap();
                }
                if (queueToSlotMap != null) {
                    assignedSlotList = queueToSlotMap.remove(queueName);
                    wrapper.setStringListHashMap(queueToSlotMap);
                    slotAssignmentMap.set(nodeId, wrapper);
                }

                if(log.isDebugEnabled()) {
                    log.debug("Cleared assigned slots of queue " + queueName + " Assigned to node " +
                            nodeId);
                }

                //Get overlapped slots from Hazelcast, delete all belonging to queue and
                //set back
                HashmapStringTreeSetWrapper overlappedSlotWrapper = overLappedSlotMap.get(nodeId);
                HashMap<String, TreeSet<Slot>> queueToOverlappedSlotMap = null;
                if (null != overlappedSlotWrapper) {
                    queueToOverlappedSlotMap = overlappedSlotWrapper.getStringListHashMap();
                }
                if (queueToOverlappedSlotMap != null) {
                    assignedSlotList = queueToOverlappedSlotMap.remove(queueName);
                    overlappedSlotWrapper.setStringListHashMap(queueToOverlappedSlotMap);
                    overLappedSlotMap.set(nodeId, overlappedSlotWrapper);
                }

                if(log.isDebugEnabled()) {
                    log.debug("Cleared overlapped slots of queue " + queueName + " to be assigned to " +
                            "node " +
                            nodeId);
                }

            }

            //add the deleted slots to un-assigned slot map, so that they can be assigned again.
            if (assignedSlotList != null && !assignedSlotList.isEmpty()) {
                String lockKeyForQueueName = queueName + SlotManagerClusterMode.class;
                synchronized (lockKeyForQueueName.intern()) {
                    TreeSetSlotWrapper treeSetStringWrapper = unAssignedSlotMap.get(queueName);

                    TreeSet<Slot> unAssignedSlotSet = new TreeSet<Slot>();
                    if (null != treeSetStringWrapper) {
                        unAssignedSlotSet = treeSetStringWrapper.getSlotTreeSet();
                    } else {
                        treeSetStringWrapper = new TreeSetSlotWrapper();
                    }
                    if (unAssignedSlotSet == null) {
                        unAssignedSlotSet = new TreeSet<Slot>();
                    }
                    for (Slot slotToBeReAssigned : assignedSlotList) {
                        //Reassign only if the slot is not empty
                        if (!SlotUtils.checkSlotEmptyFromMessageStore(slotToBeReAssigned)) {
                            if (slotToBeReAssigned.addState(SlotState.RETURNED)) {
                                unAssignedSlotSet.add(slotToBeReAssigned);
                                if(log.isDebugEnabled()) {
                                    log.debug("Slot is returned by node " + nodeId + " slot = " + slotToBeReAssigned);
                                }
                            }
                        }
                    }
                    treeSetStringWrapper.setSlotTreeSet(unAssignedSlotSet);
                    unAssignedSlotMap.set(queueName, treeSetStringWrapper);
                }
            }
        } catch (HazelcastInstanceNotActiveException ex) {
            if (AndesKernelBoot.isKernelShuttingDown()) {
                if (log.isDebugEnabled()) {
                    log.debug(HAZELCAST_INACTIVE_DURING_SHUTDOWN + "reAssignSlotWhenNoSubscribers for queue : " + queueName + " from node " + nodeId);
                }
            }
            log.error(HAZELCAST_INACTIVE_WARNING + "reAssignSlotWhenNoSubscribers for queue : " + queueName + " from node " + nodeId);
        }
    }

    protected Map<String, Long> getNodeInformedSlotDeletionSafeZones() {
        return nodeInformedSlotDeletionSafeZones;
    }

    protected Long getLastPublishedIDByNode(String nodeID) {
        return nodeToLastPublishedIDMap.get(nodeID);
    }

    protected Set<String> getMessagePublishedNodes() {
        return nodeToLastPublishedIDMap.keySet();
    }

    /**
     * Get slotDeletion safe zone. Slots can only be removed if their start message id is
     * beyond this zone.
     * @return current safe zone value
     */
    public long getSlotDeleteSafeZone() {
        return slotDeleteSafeZoneCalc.getSlotDeleteSafeZone();
    }

    /**
     * Record safe zone by node. This ping comes from nodes as messages are not published by them
     * so that safe zone value keeps moving ahead.
     * @param nodeID ID of the node
     * @param safeZoneOfNode safe zone value of the node
     * @return current calculated safe zone
     */
    public long updateAndReturnSlotDeleteSafeZone(String nodeID, long safeZoneOfNode) {
        nodeInformedSlotDeletionSafeZones.put(nodeID, safeZoneOfNode);
        return slotDeleteSafeZoneCalc.getSlotDeleteSafeZone();
    }

    /**
     * Delete all slot associations with a given queue. This is required to handle a queue purge event.
     *
     * @param queueName name of destination queue
     */
    public void clearAllActiveSlotRelationsToQueue(String queueName) {
        try {
            if(log.isDebugEnabled()) {
                log.debug("Clearing all slots for queue " + queueName);
            }

            if (null != unAssignedSlotMap) {
                unAssignedSlotMap.remove(queueName);
            }

            if (null != slotIDMap) {
                slotIDMap.remove(queueName);
            }

            // Clear slots assigned to the queue along with overlapped slots
            String nodeId = HazelcastAgent.getInstance().getNodeId();

            // The requirement here is to clear slot associations for the queue on all nodes.
            List<String> nodeIDs = HazelcastAgent.getInstance().getMembersNodeIDs();

            for (String nodeID : nodeIDs) {
                String lockKey = nodeID + SlotManagerClusterMode.class;

                synchronized (lockKey.intern()) {

                    //clear slot assignment map
                    HashmapStringTreeSetWrapper wrapper = slotAssignmentMap.get(nodeId);
                    HashMap<String, TreeSet<Slot>> queueToSlotMap = null;
                    if (null != wrapper) {
                        queueToSlotMap = wrapper.getStringListHashMap();
                    }
                    if (queueToSlotMap != null) {
                        queueToSlotMap.remove(queueName);
                        wrapper.setStringListHashMap(queueToSlotMap);
                        slotAssignmentMap.set(nodeId, wrapper);
                    }

                    //clear overlapped slot map
                    HashmapStringTreeSetWrapper overlappedSlotsWrapper = overLappedSlotMap.get
                            (nodeId);
                    if (null != overlappedSlotsWrapper) {
                        HashMap<String, TreeSet<Slot>> queueToOverlappedSlotMap = null;
                        if (null != wrapper) {
                            queueToOverlappedSlotMap = overlappedSlotsWrapper.getStringListHashMap();
                        }
                        if (queueToSlotMap != null) {
                            queueToOverlappedSlotMap.remove(queueName);
                            overlappedSlotsWrapper.setStringListHashMap(queueToOverlappedSlotMap);
                            overLappedSlotMap.set(nodeId, overlappedSlotsWrapper);
                        }
                    }
                }
            }
        } catch (HazelcastInstanceNotActiveException ex) {
            if (AndesKernelBoot.isKernelShuttingDown()) {
                if (log.isDebugEnabled()) {
                    log.debug(HAZELCAST_INACTIVE_DURING_SHUTDOWN + "clearAllActiveSlotRelationsToQueue for queue : " + queueName);
                }
            }
            log.error(HAZELCAST_INACTIVE_WARNING + "clearAllActiveSlotRelationsToQueue for queue : " + queueName);
        }

    }

    /**
     * Used to shut down the Slot manager in order before closing any dependent services.
     */
    public void shutDownSlotManager() {
        slotDeleteSafeZoneCalc.setRunning(false);
    }

    /**
     * Get an ordered set of existing, assigned slots that overlap with the input slot range.
     *
     * @param queueName  name of destination queue
     * @param startMsgID start message ID of input slot
     * @param endMsgID   end message ID of input slot
     * @return TreeSet<Slot>
     */
    private TreeSet<Slot> getOverlappedAssignedSlots(String queueName, long startMsgID, long endMsgID) {
        TreeSet<Slot> overlappedSlots = new TreeSet<Slot>();

        // Sweep all assigned slots to find overlaps using slotAssignmentMap, cos its optimized for node,queue-wise iteration.
        // The requirement here is to clear slot associations for the queue on all nodes.
        List<String> nodeIDs = HazelcastAgent.getInstance().getMembersNodeIDs();

        for (String nodeID : nodeIDs) {
            String lockKey = nodeID + SlotManagerClusterMode.class;

            TreeSet<Slot> overlappingSlotsOnNode = new TreeSet<Slot>();

            synchronized (lockKey.intern()) {
                HashmapStringTreeSetWrapper wrapper = slotAssignmentMap.get(nodeID);

                if (!overLappedSlotMap.containsKey(nodeID)) {
                    overLappedSlotMap.put(nodeID, new HashmapStringTreeSetWrapper());
                }
                HashmapStringTreeSetWrapper olWrapper = overLappedSlotMap.get(nodeID);

                HashMap<String, TreeSet<Slot>> olSlotMap = olWrapper.getStringListHashMap();

                if (!olSlotMap.containsKey(queueName)) {
                    olSlotMap.put(queueName, new TreeSet<Slot>());
                    olWrapper.setStringListHashMap(olSlotMap);
                    overLappedSlotMap.set(nodeID, olWrapper);
                }

                if (null != wrapper) {
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
                                if(log.isDebugEnabled()) {
                                    log.debug("Marked already assigned slot as an overlapping" +
                                            " slot. Slot= " + slot);
                                }
                                overlappingSlotsOnNode.add(slot);

                                if (log.isDebugEnabled()) {
                                    log.debug("Found an overlapping slot : " + slot);
                                }

                                //Add to global overlappedSlotMap
                                olSlotMap.get(queueName).remove(slot);
                                olSlotMap.get(queueName).add(slot);

                            }
                        }
                    }
                    wrapper.setStringListHashMap(queueToSlotMap);
                    slotAssignmentMap.set(nodeID, wrapper);
                }

                // Add all marked slots collected into the olSlot to global overlappedSlotsMap.
                olWrapper.setStringListHashMap(olSlotMap);
                overLappedSlotMap.set(nodeID, olWrapper);

                // Add to return collection
                overlappedSlots.addAll(overlappingSlotsOnNode);
            }
        }

        return overlappedSlots;
    }

}
