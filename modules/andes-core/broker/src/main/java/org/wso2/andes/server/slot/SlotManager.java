/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
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
 * Slot Manager is responsible of slot allocating, slot creating, slot re-assigning and slot managing tasks
 */
public class SlotManager {

    private static SlotManager slotManager = new SlotManager();
    private IMap<String, TreeSet<Slot>> freedSlotsMap; // slots which are previously owned and released by another node
    private IMap<String, TreeSet<Long>> queueToMessageIdsMap; //to keep list of message IDs against queues
    private IMap<String, Long> queueToLastAssignedIDMap; //to keep track of last assigned message ID against queue.
    private IMap<String, HashMap<Long,Slot>> slotAssignmentMap; // to keep track of assigned slots up to now.
    private MessageStore messageStore;
    private long slotThresholdValue = 100;
    private HazelcastAgent hazelcastAgent;
    private static Log log = LogFactory.getLog(SlotManager.class);


    private SlotManager() {
        if (AndesContext.getInstance().isClusteringEnabled()) {
            hazelcastAgent = HazelcastAgent.getInstance();
            freedSlotsMap = hazelcastAgent.getFreeSlotMap();
            queueToMessageIdsMap = hazelcastAgent.getQueueToMessageIdListMap();
            queueToLastAssignedIDMap = hazelcastAgent.getLastProcessedIDs();
            slotAssignmentMap = hazelcastAgent.getSlotAssignmentMap();
            this.messageStore = MessagingEngine.getInstance().getDurableMessageStore();

        }
    }

    public static SlotManager getInstance() {
        return slotManager;
    }

    /**
     * get a slot by giving the queue name. This method first lookup the free slot pool for
     * slots and if there are no slots in the free slot pool then return a newly created slot
     * @param queueName name of the queue
     * @return  Slot object
     */
    public Slot getSlot(String queueName) {
        Slot slotImpToBeAssigned = new Slot();
        slotImpToBeAssigned.setQueue(queueName);
        if (freedSlotsMap.get(queueName) != null && !freedSlotsMap.get(queueName).isEmpty()) {
            freedSlotsMap.lock(queueName);
            try {
                if (freedSlotsMap.get(queueName) != null && !freedSlotsMap.get(queueName).isEmpty()) {
                    slotImpToBeAssigned = freedSlotsMap.get(queueName).pollFirst();
                    freedSlotsMap.unlock(queueName);
                    return slotImpToBeAssigned;
                } else {
                    slotImpToBeAssigned = getFreshSlot(queueName);
                    return slotImpToBeAssigned;
                }
            } finally {
                freedSlotsMap.unlock(queueName);
            }
        } else {
            slotImpToBeAssigned = getFreshSlot(queueName);
            return slotImpToBeAssigned;
        }

    }

    /**
     *
     * @param queueName
     * @return
     */
    private Slot getFreshSlot(String queueName) {
        TreeSet<Long> messageIDSet;
        Slot slotImpToBeAssigned = new Slot();
        slotImpToBeAssigned.setQueue(queueName);
        queueToMessageIdsMap.lock(queueName);
        try {
            if (queueToMessageIdsMap.get(queueName) != null && !queueToMessageIdsMap.get(queueName).isEmpty()) {
                messageIDSet = queueToMessageIdsMap.get(queueName);
                if (queueToLastAssignedIDMap.get(queueName) != null) {
                    slotImpToBeAssigned.setStartMessageId(queueToLastAssignedIDMap.get(queueName) + 1);
                } else {
                    slotImpToBeAssigned.setStartMessageId(0L);
                }
                slotImpToBeAssigned.setEndMessageId(messageIDSet.pollFirst());
                slotImpToBeAssigned.setQueue(queueName);
                queueToMessageIdsMap.replace(queueName, messageIDSet);
                queueToLastAssignedIDMap.put(queueName, slotImpToBeAssigned.getEndMessageId());
                return slotImpToBeAssigned;
            } else {
                return null;
            }
        } finally {
            queueToMessageIdsMap.unlock(queueName);
        }
    }

    public void updateSlotAssignmentMap(String queue, Slot allocatedSlotImp) {
        String nodeId = hazelcastAgent.getNodeId();
        String slotAssignmentMapKey = nodeId + "_" + queue;
        HashMap<Long,Slot> startIdToSlotMap = new HashMap<Long, Slot>();
        slotAssignmentMap.putIfAbsent(slotAssignmentMapKey, startIdToSlotMap);
        slotAssignmentMap.lock(slotAssignmentMapKey);
        try {
            startIdToSlotMap = slotAssignmentMap.get(slotAssignmentMapKey);
            startIdToSlotMap.put(allocatedSlotImp.getStartMessageId(), allocatedSlotImp);
            slotAssignmentMap.put(slotAssignmentMapKey, startIdToSlotMap);
            log.info("Updated the slotAssignmentMap with new Slot");
        } finally {
            slotAssignmentMap.unlock(slotAssignmentMapKey);
        }
        //slotAssignmentMap.putIfAbsent(slotAssignmentMapKey,allocatedSlotImp);
    }

    public void deleteEntryFromSlotAssignmentMap(String queue) {
        String nodeId = hazelcastAgent.getNodeId();
        String slotAssignmentMapKey = nodeId + "_" + queue;
        slotAssignmentMap.remove(slotAssignmentMapKey);
    }


    /**
     * Record Slot's last message ID related to a particular queue
     *
     * @param queueName
     * @param lastMessageIdInTheSlot
     */
    public void updateMessageID(String queueName, Long lastMessageIdInTheSlot) {
        boolean isMessageIdRangeOutdated = false;
        TreeSet<Long> messageIdSet = new TreeSet<Long>();
        queueToMessageIdsMap.putIfAbsent(queueName, messageIdSet);

        queueToMessageIdsMap.lock(queueName);
        try {
            //insert the messageID only if last processed ID of this queue is less than this messageID
            if (queueToLastAssignedIDMap.get(queueName) != null) {
                if (queueToLastAssignedIDMap.get(queueName) > lastMessageIdInTheSlot) {
                    isMessageIdRangeOutdated = true;
                }
            }
            if (!isMessageIdRangeOutdated) {
                messageIdSet = queueToMessageIdsMap.get(queueName);
                messageIdSet.add(lastMessageIdInTheSlot);
                queueToMessageIdsMap.put(queueName, messageIdSet);
            }
        } finally {
            queueToMessageIdsMap.unlock(queueName);
        }
    }

    public void reAssignSlotsToFreeSlotsPool(String nodeId) {
        for (Object o : slotAssignmentMap.keySet()) {
            String slotAssignmentMapKey = (String) o;
            if (slotAssignmentMapKey.contains(nodeId)) {
                //slots list for a particular queue
                List<Slot> slotsToBeReAssigned = new ArrayList(slotAssignmentMap.get(slotAssignmentMapKey).values());
                TreeSet<Slot> freeSlotImpTreeSet = new TreeSet<Slot>();
                for (Slot slotImpToBeReAssigned : slotsToBeReAssigned) {
                    if (!isThisSlotEmpty(slotImpToBeReAssigned)) {
                        freedSlotsMap.putIfAbsent(slotImpToBeReAssigned.getQueue(), freeSlotImpTreeSet);
                        freedSlotsMap.lock(slotImpToBeReAssigned.getQueue());
                        try {
                            freeSlotImpTreeSet = freedSlotsMap.get(slotImpToBeReAssigned.getQueue());
                            freeSlotImpTreeSet.add(slotImpToBeReAssigned);
                            freedSlotsMap.put(slotImpToBeReAssigned.getQueue(), freeSlotImpTreeSet);
                            log.info("Reassigned slot " + slotImpToBeReAssigned.getStartMessageId() + " - " +
                                    slotImpToBeReAssigned.getEndMessageId() + "from node " + nodeId );
                        } finally {
                            freedSlotsMap.unlock(slotImpToBeReAssigned.getQueue());
                        }
                    }
                }
                slotAssignmentMap.remove(slotAssignmentMapKey);
            }
        }
    }

    public boolean isThisSlotEmpty(Slot slotImp) {
        try {
            List<AndesMessageMetadata> messagesReturnedFromCassandra =
                    messageStore.getMetaDataList(slotImp.getQueue(), slotImp.getStartMessageId(), slotImp.getEndMessageId());
            if (messagesReturnedFromCassandra == null || messagesReturnedFromCassandra.isEmpty()) {
                return true;
            } else {
                return false;
            }
        } catch (AndesException e) {
            log.error("Error occurred while querying metadata from cassandra", e);
            return false;
        }
    }

    public void unAssignSlot(String queueName,long startMessageIdOfSlot) {
        String slotAssignmentMapKey = hazelcastAgent.getNodeId() + "_" + queueName;
        slotAssignmentMap.lock(slotAssignmentMapKey);
        try {
            HashMap<Long,Slot> startIdToSlotMap = slotAssignmentMap.get(slotAssignmentMapKey);
            startIdToSlotMap.remove(startMessageIdOfSlot);
            slotAssignmentMap.replace(slotAssignmentMapKey,startIdToSlotMap);
            log.info("Unassigned slot start with: " + startMessageIdOfSlot);
        } finally {
            slotAssignmentMap.unlock(slotAssignmentMapKey);
        }
    }

    public long getSlotThreshold() {
        return slotThresholdValue;
    }


}
