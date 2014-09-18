package org.wso2.andes.store;

import org.apache.thrift.TException;
import org.wso2.andes.kernel.AndesException;
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
    //TODO what should be these values? is this values should be configurable
    private static long timeOutForMessagesInQueue = 1000; // In milliseconds
    private static SlotManager slotManager;
    //todo this comment should be removed when thrift communications are enabled
    // private static MBThriftClient mbThriftClient;
    static {
        slotManager = SlotManager.getInstance();
       //todo this comment should be removed when thrift communications are enabled
       // mbThriftClient= new MBThriftClient(MBUtils.getCGThriftClient("localhost",7611)) ;
    }

    //TODO USE FUTURES
    //This thread is to record message IDs in slot manager when a timeout is passed
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
     * @param metadataList metadata list to be recorded
     * @param queue  queue name
     */
    public static synchronized void recordMetaDataCountInSlot(List<AndesMessageMetadata> metadataList, String queue) {
        //if metadata list is null this method is called from time out thread
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
            //record the last message id of the slot in hazelcast distributed map
            recordLastMessageIdOfSlotInDistributedMap(queue);
        }
    }

    /**
     * Record last message ID in the slot in the distributed map
     *
     * @param queueName
     */
    public static void recordLastMessageIdOfSlotInDistributedMap(String queueName) {
        if (queueToSlotMap.get(queueName) != null) {
            Slot slot = queueToSlotMap.get(queueName);
            //TODO what should be these values? is this values should be configurable
            //  try {
                //mbThriftClient.updateMessageId(queueName,slot.getEndMessageId());
                slotManager.updateMessageID(queueName, slot.getEndMessageId());
                queueToSlotMap.remove(queueName);
                slotTimeOutMap.remove(queueName);

           // } catch (TException e) {
              // throw new AndesException("Error occurred while trying to update message IDs in slot manager" + e);
          //  }
        }
    }
}
