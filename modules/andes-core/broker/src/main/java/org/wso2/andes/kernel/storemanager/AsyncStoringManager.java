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

package org.wso2.andes.kernel.storemanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesAckData;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.AndesRemovableMetadata;
import org.wso2.andes.kernel.MessageStore;
import org.wso2.andes.kernel.MessageStoreManager;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.util.AndesConstants;
import org.wso2.andes.store.MessageContentRemoverTask;
import org.wso2.andes.tools.utils.DisruptorBasedExecutor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This message store manager stores messages through disruptor with batching (async storing)
 */
public class AsyncStoringManager extends BasicStoringManager implements MessageStoreManager {

    private static Log log = LogFactory.getLog(AsyncStoringManager.class);
    /**
     * Disruptor which implements a ring buffer to store messages asynchronously to store
     */
    private DisruptorBasedExecutor disruptorBasedExecutor;

    /**
     * Message store to deal with messages
     */
    private MessageStore messageStore;

    /**
     * This task will asynchronously remove message content
     */
    private MessageContentRemoverTask messageContentRemoverTask;

    /**
     * Executor service thread pool to execute content remover task
     */
    private ScheduledExecutorService asyncStoreTasksScheduler;

    /**
     * content removal time difference in seconds
     */
    private int contentRemovalTimeDifference;

    /**
     * message count will be flushed to DB in these interval in seconds
     */
    private int messageCountFlushInterval;

    /**
     * message count will be flushed to DB when count difference reach this val
     */
    private int messageCountFlushNumberGap;

    /**
     * Map to keep message count difference not flushed to disk of each queue
     */
    private Map<String, AtomicInteger> messageCountDifferenceMap = new HashMap<String,
            AtomicInteger>();


    public AsyncStoringManager(MessageStore messageStore) throws AndesException {
        super(messageStore);
        this.messageStore = messageStore;
        initialise(messageStore);
    }

    /**
     * Initialise Disruptor with the durable message store as persistent storage
     *
     * @param messageStore MessageStore implementation to be used as the durable message
     * @throws AndesException
     */
    private void initialise(final MessageStore messageStore) throws AndesException {

        this.messageStore = messageStore;
        disruptorBasedExecutor = new DisruptorBasedExecutor
                (MessageStoreManagerFactory.createDirectMessageStoreManager(messageStore));

        int threadPoolCount = 2;
        contentRemovalTimeDifference = 30;
        asyncStoreTasksScheduler = Executors.newScheduledThreadPool(threadPoolCount);

        //this task will periodically remove message contents from store
        messageContentRemoverTask = new MessageContentRemoverTask(messageStore);
        int schedulerPeriod = ClusterResourceHolder.getInstance().getClusterConfiguration()
                .getContentRemovalTaskInterval();
        asyncStoreTasksScheduler.scheduleAtFixedRate(messageContentRemoverTask,
                schedulerPeriod,
                schedulerPeriod,
                TimeUnit.SECONDS);

        messageCountFlushInterval = 15;
        messageCountFlushNumberGap = 100;

        //this task will periodically flush message count value to the store
        Thread messageCountFlusher = new Thread(new Runnable() {
            @Override
            public void run() {
                for (Map.Entry<String, AtomicInteger> entry : messageCountDifferenceMap.entrySet()) {
                    try {
                        if (entry.getValue().get() > 0) {
                            AndesContext.getInstance().getAndesContextStore()
                                    .incrementMessageCountForQueue(entry.getKey(),
                                            entry.getValue().get());
                        } else if (entry.getValue().get() < 0) {
                            AndesContext.getInstance().getAndesContextStore()
                                    .incrementMessageCountForQueue(
                                            entry.getKey(),
                                            entry.getValue().get());
                        }
                        entry.getValue().set(0);
                    } catch (AndesException e) {
                        log.error("Error while updating message counts for queue " + entry.getKey());
                    }
                }
            }
        });

        asyncStoreTasksScheduler.scheduleAtFixedRate(messageCountFlusher,
                10,
                messageCountFlushInterval,
                TimeUnit.SECONDS);

    }

    /**
     * store metadata to persistent storage asynchronously through disruptor
     *
     * @param metadata AndesMessageMetadata
     * @throws AndesException
     */
    @Override
    public void storeMetadata(AndesMessageMetadata metadata) throws AndesException {
        disruptorBasedExecutor.messageCompleted(metadata);
    }

    @Override
    public void storeMetaData(List<AndesMessageMetadata> messageMetadata) throws AndesException {
        for (AndesMessageMetadata metadata: messageMetadata){
            disruptorBasedExecutor.messageCompleted(metadata);
        }
    }

    /**
     * store message content to persistent storage asynchronously through disruptor
     *
     * @param messagePart AndesMessagePart
     * @throws AndesException
     */
    @Override
    public void storeMessagePart(AndesMessagePart messagePart) throws AndesException {
        disruptorBasedExecutor.messagePartReceived(messagePart);
    }

    /**
     * Acknowledgement is parsed through to persistent storage through Disruptor
     *
     * @param ackData AndesAckData
     * @throws AndesException
     */
    @Override
    public void ackReceived(AndesAckData ackData) throws AndesException {
        disruptorBasedExecutor.ackReceived(ackData);
    }

    /**
     * schedule to remove message content chunks of messages
     *
     * @param messageIdList list of message ids whose content should be removed
     * @throws AndesException
     */
    public void deleteMessageParts(List<Long> messageIdList) throws AndesException {
        for (Long messageId : messageIdList) {
            addContentDeletionTask(System.nanoTime() + contentRemovalTimeDifference * 1000000000,
                    messageId);
        }
    }

    /**
     * Acknowledgement list is parsed through to persistent storage through Disruptor
     *
     * @param ackList ack message list to process
     * @throws AndesException
     */
    public void ackReceived(List<AndesAckData> ackList) throws AndesException {
        for (AndesAckData ack : ackList) {
            disruptorBasedExecutor.ackReceived(ack);
        }
    }

    /**
     * decrement queue count by 1. Flush if difference is in tab
     * @param queueName
     *         name of the queue to decrement count
     * @param decrementBy
     *         decrement count by this value
     * @throws AndesException
     */
    public void decrementQueueCount(String queueName, int decrementBy) throws AndesException {
        AtomicInteger msgCount = messageCountDifferenceMap.get(queueName);
        if (msgCount == null) {
            msgCount = new AtomicInteger(0);
            messageCountDifferenceMap.put(queueName, msgCount);
        }

        synchronized (this) {
            int currentVal = msgCount.get();
            int newVal = currentVal - decrementBy;
            msgCount.set(newVal);
        }

        //we flush this value to store in 100 message tabs
        if (msgCount.get() % messageCountFlushNumberGap == 0) {
            if (msgCount.get() > 0) {
                AndesContext.getInstance().getAndesContextStore()
                        .incrementMessageCountForQueue(queueName, msgCount.get());
            } else {
                AndesContext.getInstance().getAndesContextStore().decrementMessageCountForQueue(
                        queueName, msgCount.get());
            }
            msgCount.set(0);
        }

    }

    /**
     * increment message count of queue. Flush if difference is in tab
     * @param queueName name of the queue to increment count
     * @param incrementBy increment count by this value
     * @throws AndesException
     */
    public void incrementQueueCount(String queueName, int incrementBy) throws AndesException {
        AtomicInteger msgCount = messageCountDifferenceMap.get(queueName);
        if (msgCount == null) {
            msgCount = new AtomicInteger(0);
            messageCountDifferenceMap.put(queueName, msgCount);
        }

        synchronized (this) {
            int currentVal = msgCount.get();
            int newVal = currentVal + incrementBy;
            msgCount.set(newVal);
        }

        //we flush this value to store in 100 message tabs
        if (msgCount.get() % messageCountFlushNumberGap == 0) {
            if (msgCount.get() > 0) {
                AndesContext.getInstance().getAndesContextStore().incrementMessageCountForQueue(
                        queueName, msgCount.get());
            } else {
                AndesContext.getInstance().getAndesContextStore().decrementMessageCountForQueue(
                        queueName, msgCount.get());
            }
            msgCount.set(0);
        }
    }

    /**
     * Store message parts through Disruptor
     *
     * @param messageParts message parts to store
     * @throws AndesException
     */
    public void storeMessagePart(List<AndesMessagePart> messageParts) throws AndesException {
        for (AndesMessagePart messagePart : messageParts) {
            disruptorBasedExecutor.messagePartReceived(messagePart);
        }
    }

    /**
     * Delete messages in async way and optionally move to DLC
     *
     * @param messagesToRemove        messages to remove
     * @param moveToDeadLetterChannel whether to send to DLC
     * @throws AndesException
     */
    public void deleteMessages(List<AndesRemovableMetadata> messagesToRemove,
                               boolean moveToDeadLetterChannel) throws AndesException {
        List<Long> idsOfMessagesToRemove = new ArrayList<Long>();
        Map<String, List<AndesRemovableMetadata>> storageQueueSeparatedRemoveMessages = new HashMap<String, List<AndesRemovableMetadata>>();
        Map<String, Integer> destinationSeparatedMsgCounts = new HashMap<String, Integer>();

        for (AndesRemovableMetadata message : messagesToRemove) {
            idsOfMessagesToRemove.add(message.messageID);

            //update <storageQueue, metadata> map
            List<AndesRemovableMetadata> messages = storageQueueSeparatedRemoveMessages
                    .get(message.storageDestination);
            if (messages == null) {
                messages = new ArrayList
                        <AndesRemovableMetadata>();
            }
            messages.add(message);
            storageQueueSeparatedRemoveMessages.put(message.storageDestination, messages);

            //update <destination, Msgcount> map
            Integer count = destinationSeparatedMsgCounts.get(message.messageDestination);
            if(count == null) {
                count = 0;
            }
            count = count + 1;
            destinationSeparatedMsgCounts.put(message.messageDestination, count);

            //if to move, move to DLC. This is costy. Involves per message read and writes
            if (moveToDeadLetterChannel) {
                AndesMessageMetadata metadata = messageStore.getMetaData(message.messageID);
                messageStore
                        .addMetaDataToQueue(AndesConstants.DEAD_LETTER_QUEUE_NAME, metadata);
            }
        }

        //remove metadata
        for (String storageQueueName : storageQueueSeparatedRemoveMessages.keySet()) {
            messageStore.deleteMessageMetadataFromQueue(storageQueueName,
                                                        storageQueueSeparatedRemoveMessages
                                                                .get(storageQueueName));
        }
        //decrement message counts
        for(String destination: destinationSeparatedMsgCounts.keySet()) {
            decrementQueueCount(destination, destinationSeparatedMsgCounts.get(destination));
        }

        if (!moveToDeadLetterChannel) {
            //remove content
            //TODO: - hasitha if a topic message be careful as it is shared
            deleteMessageParts(idsOfMessagesToRemove);
        }

        if(moveToDeadLetterChannel) {
            //increment message count of DLC
            incrementQueueCount(AndesConstants.DEAD_LETTER_QUEUE_NAME, messagesToRemove.size());
        }

    }

    /**
     * schedule to delete messages
     *
     * @param nanoTimeToWait time gap to elapse from now until delete all is triggered
     * @param messageID      id of the message to be removed
     */
    private void addContentDeletionTask(long nanoTimeToWait, long messageID) {
        messageContentRemoverTask.put(nanoTimeToWait, messageID);
    }

    /**
     * Stop all on going threads and close message store
     */
    public void close() throws InterruptedException {
        try {
            asyncStoreTasksScheduler.shutdown();
            asyncStoreTasksScheduler.awaitTermination(5, TimeUnit.SECONDS);
            messageStore.close();
        } catch (InterruptedException e) {
            asyncStoreTasksScheduler.shutdownNow();
            log.warn("Content remover task forcefully shutdown.");
            throw e;
        }
    }
}
