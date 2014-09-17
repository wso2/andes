package org.wso2.andes.messageStore;

import org.apache.thrift.TException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.server.slot.Slot;
import org.wso2.andes.server.slot.SlotManager;
import org.wso2.andes.server.slot.thrift.MBThriftClient;
import org.wso2.andes.server.slot.thrift.MBUtils;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


public class QueueMessageCounter {

    private static ConcurrentHashMap<String, Slot> queueToSlotMap = new ConcurrentHashMap<String, Slot>();
    private static ConcurrentHashMap<String, Long> slotTimeOutMap = new ConcurrentHashMap<String, Long>();
    //TODO what should be these value
    private static long timeOutForMessagesInQueue = 1000; // In milliseconds
    private static SlotManager slotManager;
    private static MBThriftClient mbThriftClient;
    static {
        slotManager = SlotManager.getInstance();
        mbThriftClient= new MBThriftClient(MBUtils.getCGThriftClient("localhost",7611)) ;
    }

    //TODO USE FUTURES
    static {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(3000);
                        for (String queue : slotTimeOutMap.keySet()) {
                            if ((System.currentTimeMillis() - slotTimeOutMap.get(queue)) > timeOutForMessagesInQueue) {
                                recordMetaDataCountInSlot(null, queue);

                            }
                        }

                    } catch (InterruptedException ignored) {
                    }
                }
            }
        }).start();
    }


    /**
     * Record metadata count in the slot so far
     *
     * @param metadataList
     * @param queue
     */
    public static synchronized void recordMetaDataCountInSlot(List<AndesMessageMetadata> metadataList, String queue) {
        if (metadataList != null) {

            for (AndesMessageMetadata md : metadataList) {
                String queueName = md.getDestination();
                //if this is the first message to that queue
                if (queueToSlotMap.get(queueName) == null) {
                    Slot slot = new Slot();
                    slot.setEndMessageId(md.getMessageID());
                    slot.setMessageCount(1L);
                    queueToSlotMap.put(queueName, slot);
                    slotTimeOutMap.put(queueName, System.currentTimeMillis());
                } else {
                    long currentMsgCount = queueToSlotMap.get(queueName).getMessageCount();
                    long newMessageCount = currentMsgCount + 1;
                    queueToSlotMap.get(queueName).setMessageCount(newMessageCount);
                    queueToSlotMap.get(queueName).setEndMessageId(md.getMessageID());

                    if (queueToSlotMap.get(queueName).getMessageCount() >= 100) {
                        recordLastMessageIdOfSlotInDistributedMap(queueName);
                    }
                }
            }
        } else {
            recordLastMessageIdOfSlotInDistributedMap(queue);

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


    /**
     * Record last message ID in the slot in the distributed map
     *
     * @param queueName
     */

    public static void recordLastMessageIdOfSlotInDistributedMap(String queueName) {
        if (queueToSlotMap.get(queueName) != null) {
            Slot slot = queueToSlotMap.get(queueName);
          //  try {
                //mbThriftClient.updateMessageId(queueName,slot.getEndMessageId());
                slotManager.updateMessageID(queueName, slot.getEndMessageId());
                queueToSlotMap.remove(queueName);
                slotTimeOutMap.remove(queueName);

           // } catch (TException e) {
               // e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
          //  }

        }

    }

}
