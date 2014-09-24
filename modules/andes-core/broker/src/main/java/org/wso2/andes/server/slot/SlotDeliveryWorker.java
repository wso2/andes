/*
*  Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.apache.thrift.TException;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.cassandra.QueueDeliveryWorker;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;
import org.wso2.andes.server.slot.thrift.MBThriftClient;
import org.wso2.andes.server.slot.thrift.MBThriftUtils;
import org.wso2.andes.subscription.SubscriptionStore;


import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * SlotDelivery worker is responsible of distributing messages to subscribers.
 * Messages will be taken from a slot.
 */
public class SlotDeliveryWorker extends Thread {

    private List<String> queueList;
    private MessageStore messageStore;
    private SubscriptionStore subscriptionStore;
    private HashMap<String, Long> localLastProcessedIdMap;
    private static boolean isClusteringEnabled;
    private static Log log = LogFactory.getLog(SlotDeliveryWorker.class);
    private ConcurrentHashMap<String, List<Slot>> slotsOwnedByMe;
    private static final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private static final Lock writeLock = readWriteLock.writeLock();
    private Timer slotDeletingTimer = new Timer();
    private static MBThriftClient mbThriftClient;
    private boolean running = true;
    private String nodeId;

    public SlotDeliveryWorker() {
        log.info("SlotDeliveryWorker Initialized.");
        this.queueList = new ArrayList<String>();
        this.messageStore = MessagingEngine.getInstance().getDurableMessageStore();
        this.subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
        isClusteringEnabled = AndesContext.getInstance().isClusteringEnabled();
        localLastProcessedIdMap = new HashMap<String, Long>();
        slotsOwnedByMe = new ConcurrentHashMap<String, List<Slot>>();
        //start slot deleting thread only if clustering is enabled. Otherwise slots assignment will not happen
        if (isClusteringEnabled) {
            nodeId = HazelcastAgent.getInstance().getNodeId();
            scheduleSlotDeletingTimer();
        } else {
        }

    }

    @Override
    public void run() {
        /**
         * This while loop is necessary since whenever there are messages this thread should deliver them
         */
        while (running) {
            //iterate through all the queues registered in this thread
            for (String queue : queueList) {
                int emptyQueueCounter = 0;
                Collection<LocalSubscription> subscriptions4Queue;
                List<Slot> slotsListForThisQueue = new ArrayList<Slot>();
                try {
                    subscriptions4Queue = subscriptionStore.getActiveLocalSubscribers(queue, false);
                    if (subscriptions4Queue != null && !subscriptions4Queue.isEmpty()) {

                        if (isClusteringEnabled) {
                            mbThriftClient = MBThriftUtils.getMBThriftClient();
                            Slot currentSlot = mbThriftClient.getSlot(queue, nodeId);
                            if (0 == currentSlot.getEndMessageId()) {
                                //no available free slots
                                emptyQueueCounter++;
                                try {
                                    if (emptyQueueCounter == queueList.size()) {
                                        Thread.sleep(2000);
                                    }
                                } catch (InterruptedException ignored) {
                                    //silently ignore
                                }
                            } else {
                                if (log.isDebugEnabled()) {
                                    log.debug("Received slot for queue " + queue + " is: " + currentSlot.getStartMessageId() +
                                            " - " + currentSlot.getEndMessageId());
                                }
                                //  slotManager.updateSlotAssignmentMap(queue, currentSlot);
                                writeLock.lock();
                                //update in-memory slot assignment map
                                try {
                                    if (slotsOwnedByMe.get(queue) != null) {
                                        slotsOwnedByMe.get(queue).add(currentSlot);

                                    } else {
                                        slotsListForThisQueue.add(currentSlot);
                                        slotsOwnedByMe.put(queue, slotsListForThisQueue);

                                    }
                                } finally {
                                    writeLock.unlock();
                                }
                                long firstMsgId = currentSlot.getStartMessageId();
                                long lastMsgId = currentSlot.getEndMessageId();
                                //read messages in the slot
                                List<AndesMessageMetadata> messagesReadByLeadingThread = messageStore.getMetaDataList(queue, firstMsgId, lastMsgId);
                                if (messagesReadByLeadingThread != null && !messagesReadByLeadingThread.isEmpty()) {
                                    if (log.isDebugEnabled()) {
                                        log.info("Number of messages read from slot " + currentSlot.getStartMessageId() + " - " +
                                                currentSlot.getEndMessageId() + " is " + messagesReadByLeadingThread.size());
                                    }
                                    QueueDeliveryWorker.getInstance().startSendingMessages(messagesReadByLeadingThread);
                                }
                            }
                        } else {
                            long startMessageId = 0;
                            if (localLastProcessedIdMap.get(queue) != null) {
                                startMessageId = localLastProcessedIdMap.get(queue) + 1;
                            }
                            List<AndesMessageMetadata> messagesReadByLeadingThread = messageStore.getNextNMessageMetadataFromQueue
                                    (queue, startMessageId, SlotCoordinationConstants.STANDALONE_SLOT_THRESHOLD);
                            if (messagesReadByLeadingThread == null || messagesReadByLeadingThread.isEmpty()) {
                                emptyQueueCounter++;
                                try {
                                    //there are no messages to read
                                    if (emptyQueueCounter == queueList.size()) {
                                        Thread.sleep(2000);
                                    }
                                } catch (InterruptedException ignored) {
                                    //silently ignore
                                }
                            } else {
                                if (log.isDebugEnabled()) {
                                    log.info(messagesReadByLeadingThread.size() + " number of messages read from slot");
                                }
                                localLastProcessedIdMap.put(queue, messagesReadByLeadingThread.
                                        get(messagesReadByLeadingThread.size() - 1).getMessageID());
                                QueueDeliveryWorker.getInstance().startSendingMessages(messagesReadByLeadingThread);
                            }
                        }
                    }
                } catch (AndesException e) {
                    log.error("Error running Cassandra Message Reader " + e.getMessage(), e);
                } catch (TException e) {
                    log.error("Error occurred while connecting to the thrift coordinator " + e.getMessage(), e);
                    //stop the current thread
                    setRunning(false);
                    MBThriftUtils.resetMBThriftClient();
                    SlotDeliveryWorkerManager slotDeliveryWorkerManager = SlotDeliveryWorkerManager.getInstance();
                    //if reconnecting is not happening now try to reconnect
                    if (!slotDeliveryWorkerManager.isReconnectingToServerStarted()) {
                        slotDeliveryWorkerManager.setReconnectingFlag(true);
                    }
                }
            }
        }

    }


    /**
     * Add a queue to queue list of this SlotDeliveryWorkerThread
     *
     * @param queueName
     */
    public void addQueueToThread(String queueName) {
        getQueueList().add(queueName);
    }

    /**
     * get queue list belongs to this thread
     *
     * @return queue list
     */
    public List<String> getQueueList() {
        return queueList;
    }


    /**
     * @return whether the worker thread is in running state or not
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * set state of the worker thread
     *
     * @param running
     */
    public void setRunning(boolean running) {
        this.running = running;
    }

    /**
     * This thread will remove empty slots from slotAssignmentMap
     */
    private void scheduleSlotDeletingTimer() {
        slotDeletingTimer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                writeLock.lock();
                try {
                    Iterator<String> queueIterator = slotsOwnedByMe.keySet().iterator();
                    while (queueIterator.hasNext()) {
                        String queue = queueIterator.next();
                        Iterator<Slot> slotIterator = slotsOwnedByMe.get(queue).iterator();
                        while (slotIterator.hasNext()) {
                            Slot slot = slotIterator.next();
                            if (SlotUtils.checkSlotEmptyFromMessageStore(slot)) {
                                mbThriftClient = MBThriftUtils.getMBThriftClient();
                                mbThriftClient.deleteSlot(queue, slot, nodeId);
                                slotIterator.remove();
                            }
                        }
                    }
                } catch (TException e) {
                    //we only reset the mbThrift client here since this thread will be run every 10 seconds
                    MBThriftUtils.resetMBThriftClient();
                    log.error("Error occurred while connecting to the thrift coordinator " + e.getMessage(), e);
                } finally {
                    writeLock.unlock();
                }
            }
        }, 11000, 10000);
    }
}

