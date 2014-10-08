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

package org.wso2.andes.server.slot;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.cassandra.OnflightMessageTracker;
import org.wso2.andes.server.cassandra.QueueDeliveryWorker;
import org.wso2.andes.server.cluster.coordination.hazelcast.HazelcastAgent;
import org.wso2.andes.server.slot.thrift.MBThriftClient;
import org.wso2.andes.subscription.SubscriptionStore;


import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * SlotDelivery worker is responsible of distributing messages to subscribers. Messages will be
 * taken from a slot.
 */
public class SlotDeliveryWorker extends Thread {

    private ConcurrentSkipListSet<String> queueList;
    private SubscriptionStore subscriptionStore;
    private HashMap<String, Long> localLastProcessedIdMap;
    private static boolean isClusteringEnabled;
    private static Log log = LogFactory.getLog(SlotDeliveryWorker.class);

    /**
     * This map contains slotId to slot hashmap against queue name
     */
    private volatile boolean running;
    private String nodeId;
    private QueueDeliveryWorker queueDeliveryWorker;

    public SlotDeliveryWorker() {
        log.info("SlotDeliveryWorker Initialized.");
        queueDeliveryWorker = QueueDeliveryWorker.getInstance();
        this.queueList = new ConcurrentSkipListSet<String>();
        this.subscriptionStore = AndesContext.getInstance().getSubscriptionStore();
        isClusteringEnabled = AndesContext.getInstance().isClusteringEnabled();
        localLastProcessedIdMap = new HashMap<String, Long>();
        /*
        Start slot deleting thread only if clustering is enabled. Otherwise slots assignment will
         not happen
         */
        if (isClusteringEnabled) {
            nodeId = HazelcastAgent.getInstance().getNodeId();
        }
    }

    @Override
    public void run() {
        /**
         * This while loop is necessary since whenever there are messages this thread should
         * deliver them
         */
        running = true;
        while (running) {

            //Iterate through all the queues registered in this thread
            int idleQueueCounter = 0;

            for (String queueName : queueList) {
                Collection<LocalSubscription> subscriptions4Queue;
                try {
                    subscriptions4Queue = subscriptionStore.getActiveLocalSubscribers(queueName,
                                                                                      false);
                    if (subscriptions4Queue != null && !subscriptions4Queue.isEmpty()) {
                        //Check in memory buffer in QueueDeliveryWorker has room
                        if (queueDeliveryWorker.getQueueDeliveryInfo(queueName)
                                               .isMessageBufferFull()) {
                            if (isClusteringEnabled) {
                                long startTime = System.currentTimeMillis();
                                Slot currentSlot = MBThriftClient.getSlot(queueName, nodeId);
                                long endTime = System.currentTimeMillis();

                                if (log.isDebugEnabled()) {
                                    log.debug(
                                            (endTime - startTime) + " milliSec took to get a slot" +
                                            " from slot manager");
                                }
                                /**
                                 * If the slot is empty
                                 */
                                if (0 == currentSlot.getEndMessageId()) {

                                    /*
                                    If the message buffer in QueueDeliveryWorker is not empty
                                     send those messages
                                     */
                                    if (log.isDebugEnabled()) {
                                        log.debug("Recieved an empty slot from slot manager in " +
                                                  "cluster mode");
                                    }
                                    boolean sentFromMessageBuffer = sendFromMessageBuffer(
                                            queueName);
                                    if (!sentFromMessageBuffer) {
                                        //No available free slots
                                        idleQueueCounter++;
                                        if (idleQueueCounter == queueList.size()) {
                                            try {
                                                if (log.isDebugEnabled()) {
                                                    log.debug("Sleeping Slot Delivery Worker");
                                                }
                                                Thread.sleep(100);
                                            } catch (InterruptedException ignored) {
                                                //Silently ignore
                                            }
                                        }
                                    }
                                } else {
                                    if (log.isDebugEnabled()) {
                                        log.debug("Received slot for queue " + queueName + " " +
                                                  "is: " + currentSlot.getStartMessageId() +
                                                  " - " + currentSlot.getEndMessageId() +
                                                  "Thread Id:" + Thread.currentThread().getId());
                                    }
                                    long firstMsgId = currentSlot.getStartMessageId();
                                    long lastMsgId = currentSlot.getEndMessageId();
                                    //Read messages in the slot
                                    List<AndesMessageMetadata> messagesReadByLeadingThread =
                                            MessagingEngine.getInstance().getMetaDataList(
                                                    queueName, firstMsgId, lastMsgId);
                                    if (messagesReadByLeadingThread != null &&
                                        !messagesReadByLeadingThread.isEmpty()) {
                                        if (log.isDebugEnabled()) {
                                            log.debug("Number of messages read from slot " +
                                                      currentSlot.getStartMessageId() + " - " +
                                                      currentSlot.getEndMessageId() + " is " +
                                                      messagesReadByLeadingThread.size());
                                        }
                                        QueueDeliveryWorker.getInstance().sendMessageToFlusher(
                                                messagesReadByLeadingThread, currentSlot);
                                    } else {
                                        MBThriftClient.deleteSlot(queueName, currentSlot, nodeId);
                                        /*If there are messages to be sent in the message
                                        buffer in QueueDeliveryWorker send them */
                                        sendFromMessageBuffer(queueName);
                                    }
                                }
                            } else {
                                long startMessageId = 0;
                                if (localLastProcessedIdMap.get(queueName) != null) {
                                    startMessageId = localLastProcessedIdMap.get(queueName) + 1;
                                }
                                int slotWindowSize = ClusterResourceHolder.getInstance()
                                                                          .getClusterConfiguration()
                                                                          .getSlotWindowSize();
                                List<AndesMessageMetadata> messagesReadByLeadingThread =
                                        MessagingEngine.getInstance()
                                                       .getNextNMessageMetadataFromQueue
                                                               (queueName, startMessageId,
                                                                slotWindowSize);
                                if (messagesReadByLeadingThread == null ||
                                    messagesReadByLeadingThread.isEmpty()) {
                                    log.debug("No messages are read from the leading thread...");
                                    boolean sentFromMessageBuffer = sendFromMessageBuffer
                                            (queueName);
                                    log.debug(
                                            "Sent messages from buffer = " + sentFromMessageBuffer);
                                    if (!sentFromMessageBuffer) {
                                        idleQueueCounter++;
                                        try {
                                            //There are no messages to read
                                            if (idleQueueCounter == queueList.size()) {
                                                if (log.isDebugEnabled()) {
                                                    log.debug("Sleeping Slot Delivery Worker");
                                                }
                                                Thread.sleep(2000);
                                            }
                                        } catch (InterruptedException ignored) {
                                            //Silently ignore
                                        }

                                    }
                                } else {
                                    if (log.isDebugEnabled()) {
                                        log.debug(messagesReadByLeadingThread.size() + " " +
                                                  "number of messages read from slot");
                                    }
                                    log.debug(
                                            "Messages from the leading thread count= " +
                                            messagesReadByLeadingThread
                                                    .size());
                                    long lastMessageId = messagesReadByLeadingThread.get(
                                            messagesReadByLeadingThread
                                                    .size() - 1).getMessageID();
                                    log.debug(
                                            "Last message id from the leading thread = " +
                                            lastMessageId);
                                    localLastProcessedIdMap.put(queueName, lastMessageId);
                                    Slot currentSlot = new Slot();
                                    currentSlot.setQueueName(queueName);
                                    currentSlot.setStartMessageId(startMessageId);
                                    currentSlot.setEndMessageId(lastMessageId);
                                    log.debug("sending read messages to flusher << " + currentSlot
                                            .toString() + " >>");
                                    queueDeliveryWorker.sendMessageToFlusher
                                            (messagesReadByLeadingThread, currentSlot);
                                }
                            }
                        } else {
                                /*If there are messages to be sent in the message
                                            buffer in QueueDeliveryWorker send them */
                            if (log.isDebugEnabled()) {
                                log.debug(
                                        "The queue" + queueName + " has no room. Thus sending " +
                                        "from buffer.");
                            }
                            sendFromMessageBuffer(queueName);
                        }
                    } else {
                        idleQueueCounter++;
                        if (idleQueueCounter == queueList.size()) {
                            try {
                                if (log.isDebugEnabled()) {
                                    log.debug("Sleeping Slot Delivery Worker");
                                }
                                Thread.sleep(100);
                            } catch (InterruptedException ignored) {
                                //Silently ignore
                            }
                        }
                    }
                } catch (AndesException e) {
                    log.error("Error running Message Store Reader " + e.getMessage(), e);
                } catch (ConnectionException e) {
                    log.error("Error occurred while connecting to the thrift coordinator " +
                              e.getMessage(), e);
                    setRunning(false);
                }
            }
        }

    }


    /**
     * Send messages from buffer in QueueDeliveryWorker if the buffer is not empty
     *
     * @param queueName
     * @return whether the messages are sent from message buffer or not
     * @throws AndesException
     */
    private boolean sendFromMessageBuffer(String queueName) throws AndesException {
        boolean sentFromMessageBuffer = false;
        if (!queueDeliveryWorker.isMessageBufferEmpty(queueName)) {
            queueDeliveryWorker.sendMessagesInBuffer(queueName);
            sentFromMessageBuffer = true;
        }
        return sentFromMessageBuffer;
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
     * Get queue list belongs to this thread
     *
     * @return queue list
     */
    public ConcurrentSkipListSet<String> getQueueList() {
        return queueList;
    }


    /**
     * @return Whether the worker thread is in running state or not
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Set state of the worker thread
     *
     * @param running
     */
    public void setRunning(boolean running) {
        this.running = running;
    }

    /**
     * Check whether the slot is empty and if not resend the remaining messages. If the slot is
     * empty delete the slot from slot manager
     *
     * @param slot
     *         to be checked for emptiness
     * @throws AndesException
     */
    public void checkForSlotCompletionAndResend(Slot slot) throws AndesException {
        if (SlotUtils.checkSlotEmptyFromMessageStore(slot)) {
            try {
                if (AndesContext.getInstance().isClusteringEnabled()) {
                    MBThriftClient.deleteSlot(slot.getQueueName(), slot, nodeId);
                }
            } catch (ConnectionException e) {
                throw new AndesException("Error deleting slot while checking for slot completion.",
                                         e);
            }

        } else {
            /*
            Acks for all sent messages fro this slot has been received,
            however slot is not empty. This happens when we write messages to message store out of
            order. Therefore we resend those messages.
             */
            List<AndesMessageMetadata> messagesReadByLeadingThread =
                    MessagingEngine.getInstance().getMetaDataList(
                            slot.getQueueName(), slot.getStartMessageId(), slot.getEndMessageId());
            if (messagesReadByLeadingThread != null &&
                !messagesReadByLeadingThread.isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug(
                            "Resending missing" + messagesReadByLeadingThread.size() + "messages " +
                            "for slot: " + slot.toString());
                }

                boolean allMessagesAlreadySent = true;
                for (AndesMessageMetadata messageMetadata : messagesReadByLeadingThread) {
                    boolean messageNotSent = OnflightMessageTracker.getInstance()
                                                                   .testMessage(messageMetadata
                                                                                        .getMessageID());

                    if (messageNotSent) {
                        allMessagesAlreadySent = false;
                        break;
                    }
                }

                // Return the slot if all messages remaining in slot are already sent. Otherwise
                // the slot will not be
                // removed.
                if (allMessagesAlreadySent) {
                    try {
                        if (AndesContext.getInstance().isClusteringEnabled()) {
                            MBThriftClient.deleteSlot(slot.getQueueName(), slot, nodeId);
                        }
                    } catch (ConnectionException e) {
                        throw new AndesException(
                                "Error deleting slot while checking for slot completion.", e);
                    }
                } else {
                    QueueDeliveryWorker.getInstance().sendMessageToFlusher(
                            messagesReadByLeadingThread, slot);
                }
            }
        }
    }
}

