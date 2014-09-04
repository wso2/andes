package org.wso2.andes.server.cluster;

import com.hazelcast.core.IMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.cassandra.Slot;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;

import java.util.*;

public class SlotManager {

    private static SlotManager slotManager;
    private IMap<String, TreeSet<Slot>> freedSlotsMap;
    private IMap<String, TreeSet<Long>> queueToMessageIdsMap;
    private IMap<String, Long> queueToLastAssignedIDMap;
    private IMap<String, List<Slot>> slotAssignmentMap;
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
        if (slotManager == null) {
            synchronized (SlotManager.class) {
                if (slotManager == null) {
                    slotManager = new SlotManager();
                }
            }
        }
        return slotManager;
    }

    public Slot getASlotFromSlotManager(String queueName) {
        //long[] slotRangeIDs = new long[2];
        Slot slotToBeAssigned = new Slot();
        slotToBeAssigned.setQueue(queueName);
        if (freedSlotsMap.get(queueName) != null && !freedSlotsMap.get(queueName).isEmpty() ) {
           freedSlotsMap.lock(queueName);
            try {
                if (freedSlotsMap.get(queueName) != null && !freedSlotsMap.get(queueName).isEmpty()) {
                    slotToBeAssigned = freedSlotsMap.get(queueName).pollFirst();
                    freedSlotsMap.unlock(queueName);
                    return slotToBeAssigned;
                }else{
                    slotToBeAssigned = getAFreshSlot(queueName);
                    return slotToBeAssigned;
                }
            } finally {
                freedSlotsMap.unlock(queueName);
            }
        } else {
            slotToBeAssigned = getAFreshSlot(queueName);
            return slotToBeAssigned;
        }

    }

    private Slot getAFreshSlot(String queueName){
        TreeSet<Long> messageIDSet;
        Slot slotToBeAssigned = new Slot();
        slotToBeAssigned.setQueue(queueName);
        queueToMessageIdsMap.lock(queueName);
        try {
            if (queueToMessageIdsMap.get(queueName) != null && !queueToMessageIdsMap.get(queueName).isEmpty()) {
                messageIDSet = queueToMessageIdsMap.get(queueName);
                if (queueToLastAssignedIDMap.get(queueName) != null) {
                    slotToBeAssigned.setStartMessageId(queueToLastAssignedIDMap.get(queueName) + 1);
                } else {
                    slotToBeAssigned.setStartMessageId(0L);
                }
                slotToBeAssigned.setEndMessageId(messageIDSet.pollFirst());
                queueToMessageIdsMap.replace(queueName, messageIDSet);
                queueToLastAssignedIDMap.put(queueName, slotToBeAssigned.getEndMessageId());
                return slotToBeAssigned;
            } else {
                return null;
            }
        } finally {
            queueToMessageIdsMap.unlock(queueName);
        }
    }

    public void addEntryToSlotAssignmentMap(String queue, Slot allocatedSlot) {
        String nodeId = hazelcastAgent.getNodeId();
        String slotAssignmentMapKey = nodeId + "_" + queue;
        List<Slot> slotListForQueue = new ArrayList<Slot>();
        slotAssignmentMap.putIfAbsent(slotAssignmentMapKey,slotListForQueue);
        slotAssignmentMap.lock(slotAssignmentMapKey);
        try {
            slotListForQueue = slotAssignmentMap.get(slotAssignmentMapKey);
            slotListForQueue.add(allocatedSlot);
            slotAssignmentMap.put(slotAssignmentMapKey, slotListForQueue);
        } finally {
            slotAssignmentMap.unlock(slotAssignmentMapKey);
        }
        //slotAssignmentMap.putIfAbsent(slotAssignmentMapKey,allocatedSlot);
    }

    public void deleteEntryFromSlotAssignmentMap(String queue){
        String nodeId = hazelcastAgent.getNodeId();
        String slotAssignmentMapKey = nodeId + "_" + queue;
        slotAssignmentMap.remove(slotAssignmentMapKey);
    }


    /**
     * Record Slot's last message ID related to a particular queue
     * @param queueName
     * @param lastMessageIdInTheSlot
     */
    public void recordMySlotLastMessageId(String queueName, Long lastMessageIdInTheSlot) {
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
                messageIdSet= queueToMessageIdsMap.get(queueName);
                messageIdSet.add(lastMessageIdInTheSlot);
                queueToMessageIdsMap.put(queueName, messageIdSet);
            }
        } finally {
            queueToMessageIdsMap.unlock(queueName);
        }
    }

    public void reAssignAssignedSlotsToFreeSlotsPool(String nodeId) {
        for (Object o : slotAssignmentMap.keySet()) {
            String slotAssignmentMapKey = (String) o;
            if (slotAssignmentMapKey.contains(nodeId)) {
                //slots list for a particular queue
                List<Slot> slotsToBeReAssigned = slotAssignmentMap.get(slotAssignmentMapKey);
                TreeSet<Slot> freeSlotTreeSet = new TreeSet<Slot>();
                freedSlotsMap.putIfAbsent(freeSlotTreeSet.first().getQueue(), freeSlotTreeSet);
                for (Slot slotToBeReAssigned:slotsToBeReAssigned) {

                    if (!isSlotEmpty(slotToBeReAssigned)) {
                        freedSlotsMap.lock(slotToBeReAssigned.getQueue());
                        try {
                            freeSlotTreeSet = freedSlotsMap.get(slotToBeReAssigned.getQueue());
                             freeSlotTreeSet.add(slotToBeReAssigned);
                             freedSlotsMap.put(slotToBeReAssigned.getQueue(), freeSlotTreeSet);
                             slotAssignmentMap.remove(slotAssignmentMapKey);
                         } finally {
                             freedSlotsMap.unlock(slotToBeReAssigned.getQueue());
                         }
                    }
                }
            }
        }
    }

    public boolean isSlotEmpty(Slot slot){
        try {
            List<AndesMessageMetadata> messagesReturnedFromCassandra =
                    messageStore.getMetaDataList(slot.getQueue(), slot.getStartMessageId(), slot.getEndMessageId());
            if(messagesReturnedFromCassandra == null){
                return true;
            } else {
                return false;
            }
        } catch (AndesException e) {
            log.error("Error occurred while querying metadata from cassandra", e);
            return false;
        }
    }

    public long getSlotThreshold() {
        return slotThresholdValue;
    }


}
