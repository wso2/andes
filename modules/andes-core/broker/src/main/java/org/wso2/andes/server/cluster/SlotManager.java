package org.wso2.andes.server.cluster;

import com.hazelcast.core.IMap;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.server.cassandra.Slot;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;

import java.util.*;

public class SlotManager {

    private static SlotManager slotManager;
    private IMap<String, TreeSet<Slot>> freeSlotsMap;
    private IMap<String, TreeSet<Long>> queueToMessageIdListMap;
    private IMap<String, Long> lastProcessedIDs;
    private IMap<String, Slot> slotAssignmentMap;
    private long slotThresholdValue = 100;
    private HazelcastAgent hazelcastAgent;

    private SlotManager() {
        if (AndesContext.getInstance().isClusteringEnabled()) {
            hazelcastAgent = HazelcastAgent.getInstance();
            freeSlotsMap = hazelcastAgent.getFreeSlotMap();
            queueToMessageIdListMap = hazelcastAgent.getQueueToMessageIdListMap();
            lastProcessedIDs = hazelcastAgent.getLastProcessedIDs();
            slotAssignmentMap = hazelcastAgent.getSlotAssignmentMap();
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

    public long[] getSlotRangeForQueue(String queueName) {
        long[] slotRangeIDs = new long[2];
        if (freeSlotsMap.get(queueName) != null && !freeSlotsMap.get(queueName).isEmpty() ) {
           freeSlotsMap.lock(queueName);
            try {
                if (freeSlotsMap.get(queueName) != null && !freeSlotsMap.get(queueName).isEmpty()) {
                    Slot slot = freeSlotsMap.get(queueName).pollFirst();
                    slotRangeIDs[0] = slot.getStartMessageId();
                    slotRangeIDs[1] = slot.getEndMessageId();
                    freeSlotsMap.unlock(queueName);
                    return slotRangeIDs;
                }else{
                    slotRangeIDs = getAFreshSlot(queueName);
                    return slotRangeIDs;
                }
            } finally {
                freeSlotsMap.unlock(queueName);
            }
        } else {
            slotRangeIDs = getAFreshSlot(queueName);
            return slotRangeIDs;
        }

    }

    private long[] getAFreshSlot(String queueName){
        TreeSet<Long> messageIDSet;
        long[] slotRangeIDs = new long[2];
        queueToMessageIdListMap.lock(queueName);
        try {
            if (queueToMessageIdListMap.get(queueName) != null && !queueToMessageIdListMap.get(queueName).isEmpty()) {
                messageIDSet = queueToMessageIdListMap.get(queueName);
                if (lastProcessedIDs.get(queueName) != null) {
                    slotRangeIDs[0] = lastProcessedIDs.get(queueName) + 1;
                } else {
                    slotRangeIDs[0] = 0;
                }
                slotRangeIDs[1] = messageIDSet.pollFirst();
                queueToMessageIdListMap.replace(queueName, messageIDSet);
                lastProcessedIDs.put(queueName, slotRangeIDs[1]);
                return slotRangeIDs;
            } else {
                return null;
            }
        } finally {
            queueToMessageIdListMap.unlock(queueName);
        }
    }

    public void addEntryToSlotAssignmentMap(String queue, long startMsgId, long endMsgId) {
        Slot slot = new Slot();
        slot.setMessageRange(startMsgId, endMsgId);
        slot.setQueue(queue);
        String nodeId = hazelcastAgent.getNodeId();
        String slotAssignmentMapKey = nodeId + "_" + queue;
        slotAssignmentMap.putIfAbsent(slotAssignmentMapKey,slot);
    }

    public void deleteEntryFromSlotAssignmentMap(String queue){
        String nodeId = hazelcastAgent.getNodeId();
        String slotAssignmentMapKey = nodeId + "_" + queue;
        slotAssignmentMap.remove(slotAssignmentMapKey);
    }

    public void updateMessageIdList(String queue, Long lastMessageIdInTheSlot) {
        boolean IsMessageIdRangeOutdated = false;
        TreeSet<Long> messageIdSet = new TreeSet<Long>();
        queueToMessageIdListMap.putIfAbsent(queue, messageIdSet);

        queueToMessageIdListMap.lock(queue);
        try {
            //insert the messageID only if last processed ID of this queue is less than this messageID
            if (lastProcessedIDs.get(queue) != null) {
                if (lastProcessedIDs.get(queue) > lastMessageIdInTheSlot) {
                    IsMessageIdRangeOutdated = true;
                }
            }
            if (!IsMessageIdRangeOutdated) {
                messageIdSet = queueToMessageIdListMap.get(queue);
                messageIdSet.add(lastMessageIdInTheSlot);
                queueToMessageIdListMap.put(queue, messageIdSet);
            }
        } finally {
            queueToMessageIdListMap.unlock(queue);
        }
    }

    public void reAssignAssignedSlotsToFreeSlotsPool(String nodeId) {
        for (Object o : slotAssignmentMap.keySet()) {
            String slotAssignmentMapKey = (String) o;
            if (slotAssignmentMapKey.contains(nodeId)) {
                Slot slotToBeReAssigned = slotAssignmentMap.get(slotAssignmentMapKey);
                TreeSet<Slot> freeSlotTreeSet = new TreeSet<Slot>();
                freeSlotsMap.putIfAbsent(slotToBeReAssigned.getQueue(), freeSlotTreeSet);
                freeSlotsMap.lock(slotToBeReAssigned.getQueue());
                try {
                    freeSlotTreeSet = freeSlotsMap.get(slotToBeReAssigned.getQueue());
                    freeSlotTreeSet.add(slotToBeReAssigned);
                    freeSlotsMap.put(slotToBeReAssigned.getQueue(), freeSlotTreeSet);
                    slotAssignmentMap.remove(slotAssignmentMapKey);
                } finally {
                    freeSlotsMap.unlock(slotToBeReAssigned.getQueue());
                }
            }
        }
    }

    public long getSlotThreshold() {
        return slotThresholdValue;
    }


}
