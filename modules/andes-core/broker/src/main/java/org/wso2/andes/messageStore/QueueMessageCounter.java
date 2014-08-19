package org.wso2.andes.messageStore;

import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.server.cassandra.Slot;
import org.wso2.andes.server.cluster.SlotManager;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


public class QueueMessageCounter {

    private static ConcurrentHashMap<String, Slot> queueToSlotMap = new ConcurrentHashMap<String, Slot>();
    private static ConcurrentHashMap<String, Long> slotTimeOutMap = new ConcurrentHashMap<String, Long>();
    //TODO what should be these value
    private static long timeOutForMessagesInQueue = 1000; // In milliseconds
    private static SlotManager slotManager;


    static {
        slotManager = SlotManager.getInstance();
    }

    static {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(3000);
                        for (String queue : slotTimeOutMap.keySet()) {
                            if((System.currentTimeMillis() - slotTimeOutMap.get(queue))> timeOutForMessagesInQueue){
                                recordMetaDataCount(null, queue);
                            }
                        }

                    } catch (InterruptedException ignored) {
                    }
                }
            }
        }).start();
    }

    public static synchronized void recordMetaDataCount(List<AndesMessageMetadata> metadataList,String queue) {
        if (metadataList!=null) {
            for (AndesMessageMetadata md : metadataList) {
                String queueName = md.getDestination();
                //if this is the first message to that queue
                if (queueToSlotMap.get(queueName) == null) {
                    Slot slot = new Slot();
                    slot.setEndMessageId(md.getMessageID());
                    slot.setMessageCount((long) 1);
                    queueToSlotMap.put(queueName, slot);
                    slotTimeOutMap.put(queueName,System.currentTimeMillis());
                } else {
                    long currentMsgCount = queueToSlotMap.get(queueName).getMessageCount();
                    long newMessageCount = currentMsgCount + 1;
                    queueToSlotMap.get(queueName).setMessageCount(newMessageCount);
                    queueToSlotMap.get(queueName).setEndMessageId(md.getMessageID());

                    if (queueToSlotMap.get(queueName).getMessageCount() >= slotManager.getSlotThreshold()) {
                        recordLastMessageId(queueName);
                    }
                }
            }
        } else {
            recordLastMessageId(queue);

        }
    }

//    public static void updateDistributedFreeSlotMap(String queue) {
//
//        if (queueToSlotMap.get(queue)!=null) {
//            Slot slot = queueToSlotMap.get(queue);
//            if (slotManager.getFreeSlotsMap().get(queue) == null) {
//                slotManager.updateFreeSlotMap(queue, slot, (long) 1);
//            } else {
//                //if there is already an entry for this queue in freeSlotMap
//                long lastSlotRecordId = slotManager.getLastSlotId(queue);
//                Slot oldSlot = slotManager.getLastSlot(queue);
//                Slot newSlot = slot;
//                oldSlot.setMessageCount(oldSlot.getMessageCount() + newSlot.getMessageCount());
//                if (oldSlot.getMessageCount() <= slotManager.getSlotThreshold()) {
//                    //can update the current slot with new data
//                    if (oldSlot.getStartMessageId() > newSlot.getStartMessageId()) {
//                        oldSlot.setStartMessageId(newSlot.getStartMessageId());
//                    }
//                    if (oldSlot.getEndMessageId() < newSlot.getEndMessageId()) {
//                        oldSlot.setEndMessageId(newSlot.getEndMessageId());
//                    }
//                    if (oldSlot.getLastUpdateTime() < newSlot.getLastUpdateTime()) {
//                        oldSlot.setLastUpdateTime(newSlot.getLastUpdateTime());
//                    }
//                    slotManager.updateFreeSlotMap(queue, oldSlot, lastSlotRecordId);
//                } else {
//                    //create a new slot
//                    slotManager.updateFreeSlotMap(queue, newSlot, ++lastSlotRecordId);
//                }
//            }
//            queueToSlotMap.remove(queue);
//            slotTimeOutMap.remove(queue);
//
//        }
//    }

    public static void recordLastMessageId(String queue){
        if (queueToSlotMap.get(queue)!=null) {
            Slot slot = queueToSlotMap.get(queue);
            slotManager.updateMessageIdList(queue, slot.getEndMessageId());
            queueToSlotMap.remove(queue);
            slotTimeOutMap.remove(queue);
        }

    }

}
