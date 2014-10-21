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
import org.wso2.andes.server.cassandra.OnflightMessageTracker;
import org.wso2.andes.server.stats.PerformanceCounter;
import org.wso2.andes.server.util.AndesConstants;
import org.wso2.andes.store.MessageContentRemoverTask;
import org.wso2.andes.tools.utils.DisruptorBasedExecutor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This message store manager stores messages in durable storage through disruptor (async storing)
 */
public class DurableAsyncStoringManager implements MessageStoreManager {

    private static Log log = LogFactory.getLog(DurableAsyncStoringManager.class);
    /**
     * Disruptor which implements a ring buffer to store messages asynchronously to store
     */
    private DisruptorBasedExecutor disruptorBasedExecutor;

    /**
     * Message store to deal with messages
     */
    private MessageStore durableMessageStore;

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
     * Map to keep message count difference not flushed to disk of each queue
     */
    private Map<String, AtomicInteger> messageCountDifferenceMap = new HashMap<String,
            AtomicInteger>();

    /**
     * Initialise Disruptor with the durable message store as persistent storage
     *
     * @param durableMessageStore
     *         MessageStore implementation to be used as the durable message
     * @throws AndesException
     */
    @Override
    public void initialise(final MessageStore durableMessageStore) throws AndesException {

        this.durableMessageStore = durableMessageStore;
        disruptorBasedExecutor = new DisruptorBasedExecutor(this, null);

        int threadPoolCount = 2;
        contentRemovalTimeDifference = 30;
        asyncStoreTasksScheduler = Executors.newScheduledThreadPool(threadPoolCount);

        //this task will periodically remove message contents from store
        messageContentRemoverTask = new MessageContentRemoverTask(durableMessageStore);
        int schedulerPeriod = ClusterResourceHolder.getInstance().getClusterConfiguration()
                                                   .getContentRemovalTaskInterval();
        asyncStoreTasksScheduler.scheduleAtFixedRate(messageContentRemoverTask,
                                                     schedulerPeriod,
                                                     schedulerPeriod,
                                                     TimeUnit.SECONDS);

        //this task will periodically flush message count value to the store
        Thread messageCountFlusher = new Thread(new Runnable() {
            @Override
            public void run() {
                Iterator<Map.Entry<String, AtomicInteger>> iter = messageCountDifferenceMap
                        .entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<String, AtomicInteger> entry = iter.next();
                    try {
                        if (entry.getValue().get() > 0) {
                            AndesContext.getInstance().getAndesContextStore().incrementMessageCountForQueue(entry.getKey(),
                                                                                                            entry.getValue().get());
                        } else if (entry.getValue().get() < 0) {
                            AndesContext.getInstance().getAndesContextStore().incrementMessageCountForQueue(
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
                                                     15,
                                                     TimeUnit.SECONDS);

    }

    /**
     * store metadata to persistent storage asynchronously through disruptor
     *
     * @param metadata
     *         AndesMessageMetadata
     * @param channelID
     *         channel ID
     * @throws AndesException
     */
    @Override
    public void storeMetadata(AndesMessageMetadata metadata, long channelID) throws AndesException {
        disruptorBasedExecutor.messageCompleted(metadata, channelID);
    }

    /**
     * store message content to persistent storage asynchronously through disruptor
     *
     * @param messagePart
     *         AndesMessagePart
     * @throws AndesException
     */
    @Override
    public void storeMessageContent(AndesMessagePart messagePart) throws AndesException {
        disruptorBasedExecutor.messagePartReceived(messagePart);
    }

    /**
     * Acknowledgement is parsed through to persistent storage through Disruptor
     *
     * @param ackData
     *         AndesAckData
     * @throws AndesException
     */
    @Override
    public void ackReceived(AndesAckData ackData) throws AndesException {
        disruptorBasedExecutor.ackReceived(ackData);
    }

    /**
     * schedule to remove message content chunks of messages
     *
     * @param messageIdList
     *         list of message ids whose content should be removed
     * @throws AndesException
     */
    public void deleteMessageParts(List<Long> messageIdList) throws AndesException {
        for (Long messageId : messageIdList) {
            addContentDeletionTask(System.nanoTime() + contentRemovalTimeDifference * 1000000000,
                                   messageId);
        }
    }

    public void processAckReceived(List<AndesAckData> ackList) throws AndesException {
        List<AndesRemovableMetadata> removableMetadata = new ArrayList<AndesRemovableMetadata>();
        for (AndesAckData ack : ackList) {
            removableMetadata.add(new AndesRemovableMetadata(ack.messageID, ack.qName));
            //record ack received
            PerformanceCounter.recordMessageRemovedAfterAck();
        }
        //remove messages permanently from store
        deleteMessages(removableMetadata, false);
    }

    /**
     * decrement queue count by 1. Flush if difference is in tab
     *
     * @param queueName
     *         name of queue to decrement count
     * @throws AndesException
     */
    public void decrementQueueCount(String queueName) throws AndesException {
        AtomicInteger msgCount = messageCountDifferenceMap.get(queueName);
        if (msgCount == null) {
            msgCount = new AtomicInteger(0);
            messageCountDifferenceMap.put(queueName,msgCount);
        }

        msgCount.decrementAndGet();

        //we flush this value to store in 100 message tabs
        if (msgCount.get() % 100 == 0) {
            if (msgCount.get() > 0) {
                AndesContext.getInstance().getAndesContextStore().incrementMessageCountForQueue(queueName,msgCount.get());
            } else {
                AndesContext.getInstance().getAndesContextStore().decrementMessageCountForQueue(
                        queueName, msgCount.get());
            }
            msgCount.set(0);
        }

    }

    /**
     * increment message count of queue by 1.Flush if difference is in tab
     *
     * @param queueName
     *         name of queue to increment count
     * @throws AndesException
     */
    public void incrementQueueCount(String queueName) throws AndesException {
        AtomicInteger msgCount = messageCountDifferenceMap.get(queueName);
        if (msgCount == null) {
            msgCount = new AtomicInteger(0);
            messageCountDifferenceMap.put(queueName,msgCount);
        }

        msgCount.incrementAndGet();

        //we flush this value to store in 100 message tabs
        if (msgCount.get() % 100 == 0) {
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

    public void storeMessagePart(List<AndesMessagePart> messageParts) throws AndesException {
        durableMessageStore.storeMessagePart(messageParts);
    }

    public void storeMetaData(List<AndesMessageMetadata> messageMetadata) throws AndesException {

        durableMessageStore.addMetaData(messageMetadata);

        //increment message count for queue
        for (AndesMessageMetadata md : messageMetadata) {
            incrementQueueCount(md.getDestination());
        }
        //record the successfully written message count
        PerformanceCounter.recordIncomingMessageWrittenToStore();
    }

    public AndesMessagePart getContent(long messageId, int offsetValue) throws AndesException {
        return durableMessageStore.getContent(messageId, offsetValue);
    }

    public List<AndesRemovableMetadata> getExpiredMessages(int limit) throws AndesException {
        return durableMessageStore.getExpiredMessages(limit);
    }

    public void deleteMessages(List<AndesRemovableMetadata> messagesToRemove,
                               boolean moveToDeadLetterChannel) throws AndesException {

        List<Long> idsOfMessagesToRemove = new ArrayList<Long>();
        Map<String, List<AndesRemovableMetadata>> queueSeparatedRemoveMessages = new HashMap
                <String, List<AndesRemovableMetadata>>();

        for (AndesRemovableMetadata message : messagesToRemove) {
            idsOfMessagesToRemove.add(message.messageID);

            List<AndesRemovableMetadata> messages = queueSeparatedRemoveMessages
                    .get(message.destination);
            if (messages == null) {
                messages = new ArrayList
                        <AndesRemovableMetadata>();
            }
            messages.add(message);
            queueSeparatedRemoveMessages.put(message.destination, messages);

            //decrement message count of queue
            decrementQueueCount(message.destination);

            //update server side message trackings
            OnflightMessageTracker onflightMessageTracker = OnflightMessageTracker.getInstance();
            onflightMessageTracker.updateDeliveredButNotAckedMessages(message.messageID);


            //if to move, move to DLC
            if (moveToDeadLetterChannel) {
                AndesMessageMetadata metadata = durableMessageStore.getMetaData(message.messageID);
                durableMessageStore
                        .addMetaDataToQueue(AndesConstants.DEAD_LETTER_QUEUE_NAME, metadata);
                //increment message count of DLC
                incrementQueueCount(AndesConstants.DEAD_LETTER_QUEUE_NAME);
            }
        }

        //remove metadata
        for (String queueName : queueSeparatedRemoveMessages.keySet()) {
            durableMessageStore.deleteMessageMetadataFromQueue(queueName,
                                                               queueSeparatedRemoveMessages
                                                                       .get(queueName));
        }

        if (!moveToDeadLetterChannel) {
            //schedule to remove content
            //TODO: - hasitha if a topic message be careful as it is shared
            deleteMessageParts(idsOfMessagesToRemove);
        }

        //remove these message ids from expiration tracking
        durableMessageStore.deleteMessagesFromExpiryQueue(idsOfMessagesToRemove);
    }

    public List<AndesMessageMetadata> getMetaDataList(final String queueName, long firstMsgId, long lastMsgID) throws AndesException {
        return durableMessageStore.getMetaDataList(queueName,firstMsgId,lastMsgID);
    }

    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(final String queueName, long firstMsgId, int count) throws AndesException {
        return durableMessageStore.getNextNMessageMetadataFromQueue(queueName, firstMsgId, count);
    }

    public void close() {
        try {
            asyncStoreTasksScheduler.shutdown();
            asyncStoreTasksScheduler.awaitTermination(5, TimeUnit.SECONDS);
            durableMessageStore.close();
        } catch (InterruptedException e) {
            asyncStoreTasksScheduler.shutdownNow();
            log.warn("Content remover task forcefully shutdown.");
        }
    }

    private void addContentDeletionTask(long nanoTimeToWait, long messageID) {
        messageContentRemoverTask.put(nanoTimeToWait, messageID);
    }

    /**
     * Store a message in a different Queue without altering the meta data.
     *
     * @param messageId        The message Id to move
     * @param currentQueueName The current destination of the message
     * @param targetQueueName  The target destination Queue name
     * @throws AndesException
     */
    public void moveMetaDataToQueue(long messageId, String currentQueueName, String targetQueueName) throws
            AndesException {
        durableMessageStore.moveMetaDataToQueue(messageId, currentQueueName, targetQueueName);
    }

    /**
     * Update the meta data for the given message with the given information in the AndesMetaData. Update destination
     * and meta data bytes.
     *
     * @param currentQueueName The queue the Meta Data currently in
     * @param metadataList     The updated meta data list.
     * @throws AndesException
     */
    public void updateMetaDataInformation(String currentQueueName, List<AndesMessageMetadata> metadataList) throws
            AndesException {
        durableMessageStore.updateMetaDataInformation(currentQueueName, metadataList);
    }
}
