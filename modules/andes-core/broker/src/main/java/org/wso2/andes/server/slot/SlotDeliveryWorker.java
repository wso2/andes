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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.cassandra.QueueDeliveryWorker;
import org.wso2.andes.subscription.SubscriptionStore;


import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class SlotDeliveryWorker extends Thread {

    private List<String> queueList;
    private SlotManager slotManager;
    private MessageStore messageStore;
    private SubscriptionStore subscriptionStore;
    private Map<String, QueueDeliveryInfo> subscriptionCursar4QueueMap = new HashMap<String, QueueDeliveryInfo>();
    private int maxNumberOfUnAckedMessages = 20000;
    private HashMap<String, Long> localLastProcessedIdMap;
    private static HashMap<String, QueueDeliveryWorker> queueToQueueDeliveryWorkerMap;
    private static boolean isClusteringEnabled;
    private static Log log = LogFactory.getLog(SlotDeliveryWorker.class);
    private ConcurrentHashMap<String, List<Slot>> slotsOwnedByMe;
    private int messageCountToRead = 1000;


    private static final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private static final Lock readLock = readWriteLock.readLock();
    private static final Lock writeLock = readWriteLock.writeLock();


    public SlotDeliveryWorker() {
        log.info("SlotDeliveryWorker Initialized.");
        this.queueList = new ArrayList<String>();
        this.messageStore = MessagingEngine.getInstance().getDurableMessageStore();
        this.subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
        isClusteringEnabled = AndesContext.getInstance().isClusteringEnabled();
        localLastProcessedIdMap = new HashMap<String, Long>();
        queueToQueueDeliveryWorkerMap = new HashMap<String, QueueDeliveryWorker>();
        slotsOwnedByMe = new ConcurrentHashMap<String, List<Slot>>();
        if (isClusteringEnabled) {
            slotManager = SlotManager.getInstance();
            startSlotDeletingThread();
        } else {
        }
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
                List<Slot> slotsListForThisQueue = new ArrayList<Slot>();
                try {
                    subscriptions4Queue = subscriptionStore.getActiveLocalSubscribersForQueue(queue);
                    if (subscriptions4Queue != null && !subscriptions4Queue.isEmpty()) {

                        if (isClusteringEnabled) {
                            Slot currentSlotImp = slotManager.getSlot(queue);
                            if (currentSlotImp == null) {
                                //no available free slots
                                try {
                                    //TODO is it ok to sleep since there are other queues
                                    Thread.sleep(2000);
                                } catch (InterruptedException e) {
                                    //silently ignore
                                }
                            } else {
                                log.info("Received slot for queue " + queue + " is: " + currentSlotImp.getStartMessageId() + " - " + currentSlotImp.getEndMessageId());
                                slotManager.updateSlotAssignmentMap(queue, currentSlotImp);
                                writeLock.lock();
                                try {
                                    if (slotsOwnedByMe.get(queue) != null) {
                                        slotsOwnedByMe.get(queue).add(currentSlotImp);

                                    } else {
                                        slotsListForThisQueue.add(currentSlotImp);
                                        slotsOwnedByMe.put(queue, slotsListForThisQueue);

                                    }
                                } finally {
                                    writeLock.unlock();
                                }
                                long firstMsgId = currentSlotImp.getStartMessageId();
                                long lastMsgId = currentSlotImp.getEndMessageId();
                                List<AndesMessageMetadata> messagesReadByLeadingThread = messageStore.getMetaDataList(queue, firstMsgId, lastMsgId);
                                if (messagesReadByLeadingThread != null && !messagesReadByLeadingThread.isEmpty()) {
                                    // sendMessages(messagesReadByLeadingThread, subscriptions4Queue, queue);
                                    log.info("Number of messages read from slot " + currentSlotImp.getStartMessageId() + " - " +
                                            currentSlotImp.getEndMessageId()+" is " + messagesReadByLeadingThread.size());
                                    QueueDeliveryWorker.getInstance().run(messagesReadByLeadingThread);
                                }
                            }
                        } else {
                            long startMessageId = 0;
                            if (localLastProcessedIdMap.get(queue) != null) {
                                startMessageId = localLastProcessedIdMap.get(queue);
                            }
                            List<AndesMessageMetadata> messagesReadByLeadingThread = messageStore.getNextNMessageMetadataFromQueue
                                    (queue, startMessageId++, messageCountToRead);
                            if (messagesReadByLeadingThread == null || messagesReadByLeadingThread.isEmpty()) {
                                try {
                                    //log.info("There are no messages to read....");
                                    //TODO is it ok to sleep since there are other queues
                                    Thread.sleep(2000);
                                } catch (InterruptedException ignored) {
                                    //silently ignore
                                }
                            } else {
                                log.info(messagesReadByLeadingThread.size()+" number of messages read from slot");
                                localLastProcessedIdMap.put(queue,messagesReadByLeadingThread.get(messagesReadByLeadingThread.size() -1).getMessageID());
                                QueueDeliveryWorker.getInstance().run(messagesReadByLeadingThread);
                            }
                        }
                    }
                } catch (AndesException e) {
                    log.error("Error running Cassandra Message Reader " + e.getMessage(), e);
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
            public void run() {
                try {

                    log.info("SLOT DELETING THREAD STARTED");
                    Thread.sleep(10000);
                    while (true) {
                        writeLock.lock();
                        try {
                            Iterator<String> queueIterator = slotsOwnedByMe.keySet().iterator();
                            while (queueIterator.hasNext()) {
                                String queue = queueIterator.next();
                                Iterator<Slot> slotIterator = slotsOwnedByMe.get(queue).iterator();
                                while (slotIterator.hasNext()) {
                                    Slot slotImp = slotIterator.next();
                                    if (slotManager.isThisSlotEmpty(slotImp)) {
                                        slotManager.unAssignSlot(queue, slotImp.getStartMessageId());
                                         slotIterator.remove();
                                    }
                                }
                            }
                        } finally {
                            writeLock.unlock();
                        }

                        Thread.sleep(10000);
                    }
                } catch (InterruptedException e) {
                    log.error("Error in slot deleting thread, it will break the thread", e);
                }
            }

        }.start();
    }
}

