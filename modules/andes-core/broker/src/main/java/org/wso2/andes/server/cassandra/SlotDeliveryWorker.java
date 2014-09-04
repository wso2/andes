package org.wso2.andes.server.cassandra;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.cluster.SlotManager;
import org.wso2.andes.subscription.SubscriptionStore;


import java.util.*;


public class SlotDeliveryWorker extends Thread {

    private List<String> queueList;
    private SlotManager slotManager;
    private MessageStore messageStore;
    private SubscriptionStore subscriptionStore;
    private Map<String, QueueDeliveryInfo> subscriptionCursar4QueueMap = new HashMap<String, QueueDeliveryInfo>();
    private int maxNumberOfUnAckedMessages = 20000;
    private HashMap<String, Long> localLastProcessedIdMap;
    private static HashMap<String,QueueDeliveryWorker> queueToQueueDeliveryWorkerMap;
    private static boolean isClusteringEnabled;
    private static Log log = LogFactory.getLog(SlotDeliveryWorker.class);
    private Map<String,List<Slot>> localQueueToSlotsMap;


    public SlotDeliveryWorker() {
        this.queueList = new ArrayList<String>();
        slotManager = SlotManager.getInstance();
        this.messageStore = MessagingEngine.getInstance().getDurableMessageStore();
        this.subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
        isClusteringEnabled = AndesContext.getInstance().isClusteringEnabled();
        localLastProcessedIdMap = new HashMap<String, Long>();
        queueToQueueDeliveryWorkerMap = new HashMap<String, QueueDeliveryWorker>();
        localQueueToSlotsMap = new HashMap<String, List<Slot>>();
        startSlotDeletingThread();

    }

    public class QueueDeliveryInfo {
        String queueName;
        Iterator<LocalSubscription> iterator;
    }

    /**
     * Get the next subscription for the given queue. If at end of the subscriptions, it circles around to the first one
     *
     * @param queueName           name of queue
     * @param subscriptions4Queue subscriptions registered for the queue
     * @return subscription to deliver
     * @throws AndesException
     */
    private LocalSubscription findNextSubscriptionToSent(String queueName, Collection<LocalSubscription> subscriptions4Queue) throws AndesException {
        if (subscriptions4Queue == null || subscriptions4Queue.size() == 0) {
            subscriptionCursar4QueueMap.remove(queueName);
            return null;
        }

        QueueDeliveryInfo queueDeliveryInfo = getQueueDeliveryInfo(queueName);
        Iterator<LocalSubscription> it = queueDeliveryInfo.iterator;
        if (it.hasNext()) {
            return it.next();
        } else {
            it = subscriptions4Queue.iterator();
            queueDeliveryInfo.iterator = it;
            if (it.hasNext()) {
                return it.next();
            } else {
                return null;
            }
        }
    }

    public QueueDeliveryInfo getQueueDeliveryInfo(String queueName) throws AndesException {
        QueueDeliveryInfo queueDeliveryInfo = subscriptionCursar4QueueMap.get(queueName);
        if (queueDeliveryInfo == null) {
            queueDeliveryInfo = new QueueDeliveryInfo();
            queueDeliveryInfo.queueName = queueName;
            Collection<LocalSubscription> localSubscribersForQueue = subscriptionStore.getActiveLocalSubscribersForQueue(queueName);
            queueDeliveryInfo.iterator = localSubscribersForQueue.iterator();
            subscriptionCursar4QueueMap.put(queueName, queueDeliveryInfo);
        }
        return queueDeliveryInfo;
    }

    /**
     * does that queue has too many messages pending
     *
     * @param localSubscription local subscription
     * @return is subscription ready to accept messages
     */
    private boolean isThisSubscriptionHasRoom(LocalSubscription localSubscription) {
        //
        int notAckedMsgCount = localSubscription.getnotAckedMsgCount();

        //Here we ignore messages that has been scheduled but not executed, so it might send few messages than maxNumberOfUnAckedMessages
        if (notAckedMsgCount < maxNumberOfUnAckedMessages) {
            return true;
        } else {

            if (log.isDebugEnabled()) {
                //log.debug("Not selected, channel =" + localSubscription + " pending count =" + (notAckedMsgCount + workqueueSize));
            }
            return false;
        }
    }

    @Override
    public void run() {

        while (true) {
            for (String queue : queueList) {

                Collection<LocalSubscription> subscriptions4Queue;
                try {
                    subscriptions4Queue = subscriptionStore.getActiveLocalSubscribersForQueue(queue);
                    if (subscriptions4Queue != null && !subscriptions4Queue.isEmpty()) {

                        if (isClusteringEnabled) {
                            Slot currentSlot = slotManager.getASlotFromSlotManager(queue);
                            if (currentSlot == null) {
                                //no available free slots
                                //delete previsous entry from slot assignment map
                              //  slotManager.deleteEntryFromSlotAssignmentMap(queue);
                                try {
                                    //TODO is it ok to sleep since there are other queues
                                    Thread.sleep(2000);
                                } catch (InterruptedException e) {
                                    //silently ignore
                                }
                            } else {
                                slotManager.addEntryToSlotAssignmentMap(queue, currentSlot);
                                long firstMsgId = currentSlot.getStartMessageId();
                                long lastMsgId = currentSlot.getEndMessageId();
                                List<AndesMessageMetadata> messagesReadByLeadingThread = messageStore.getMetaDataList(queue, firstMsgId, lastMsgId);
                                if (messagesReadByLeadingThread!=null) {
                                   // sendMessages(messagesReadByLeadingThread, subscriptions4Queue, queue);
                                    QueueDeliveryWorker.getInstance().run(messagesReadByLeadingThread);
                                }
                            }
                        } else {
                            long startMessageId = 0;
                            if (localLastProcessedIdMap.get(queue) != null) {
                                startMessageId = localLastProcessedIdMap.get(queue);
                            }
                            List<AndesMessageMetadata> messagesReadByLeadingThread = messageStore.getNextNMessageMetadataFromQueue
                                    (queue, startMessageId, 100);
                            if (messagesReadByLeadingThread == null) {
                                try {
                                    //TODO is it ok to sleep since there are other queues
                                    Thread.sleep(2000);
                                } catch (InterruptedException ignored) {
                                    //silently ignore
                                }
                            } else {
                                QueueDeliveryWorker.getInstance().run(messagesReadByLeadingThread);
                            }
                        }
                    }
                } catch (AndesException e) {
                    log.error("Error running Cassandra Message Reader" + e.getMessage(), e);
                }
            }
        }

    }


    public void addQueueToThread(String queueName) {
        getQueueList().add(queueName);
      //  QueueDeliveryWorker queueDeliveryWorker = new QueueDeliveryWorker(1000,false);
       // queueToQueueDeliveryWorkerMap.put(queueName,queueDeliveryWorker);
    }

    public List<String> getQueueList() {
        return queueList;
    }

    private void sendMessages(List<AndesMessageMetadata> messageMetadataList, Collection<LocalSubscription> subscriptions4Queue, String queue) {
        try {
            for (AndesMessageMetadata message : messageMetadataList) {
                for (int j = 0; j < subscriptions4Queue.size(); j++) {
                    LocalSubscription localSubscription = findNextSubscriptionToSent(queue, subscriptions4Queue);
                    if (isThisSubscriptionHasRoom(localSubscription)) {
                        if (log.isDebugEnabled()) {
                            log.debug("TRACING>> scheduled to deliver - messageID: " + message.getMessageID() + " for queue: " + message.getDestination());
                        }
                        if (localSubscription.isActive()) {
                            localSubscription.sendMessageToSubscriber(message);
                            if (!isClusteringEnabled) {
                                localLastProcessedIdMap.put(queue, message.getMessageID());

                            }
                        }
                        break;
                    }
                }
            }
        } catch (AndesException e) {
            log.error("Error in sending messages " + e.getMessage(), e);

        }
    }

    private void startSlotDeletingThread() {
        new Thread() {

        }.start();
    }
}

