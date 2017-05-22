/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.andes.kernel;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.subscription.StorageQueue;
import org.wso2.andes.server.queue.DLCQueueUtils;
import org.wso2.andes.tools.utils.MessageTracer;

/**
 * This class is for message handling operations of a queue. Handling
 * Message caching, buffering, keep trace of messages etc
 */
public class MessageHandler {


    private static Log log = LogFactory.getLog(MessageHandler.class);

    /**
     * Reference to MessageStore. This is the persistent storage for messages
     */
    private MessageStore messageStore;

    /**
     * Manager for message delivery to subscriptions.
     */
    private MessageDeliveryManager messageDeliveryManager;

    /**
     * Name of the queue to handle
     */
    private String storageQueueName;

    /**
     * Maximum number to retries retrieve metadata list for a given storage
     * queue ( in the errors occur in message stores)
     */
    private static final int MAX_META_DATA_RETRIEVAL_COUNT = 5;

    /**
     * In-memory message list scheduled to be delivered. These messages will be flushed
     * to subscriber.Used Map instead of Set because of https://wso2.org/jira/browse/MB-1624
     */
    private ConcurrentMap<Long, DeliverableAndesMetadata> readButUndeliveredMessages;

    /***
     * In case of a purge, we must store the timestamp when the purge was called.
     * This way we can identify messages received before that timestamp that fail and ignore them.
     */
    private long lastPurgedTimestamp;

    /**
     * Max number of messages to keep in buffer
     */
    private final Integer maxNumberOfReadButUndeliveredMessages;

    private long lastBufferedMessageId;

    private final long messageFetchSize;

    public MessageHandler(String queueName) {
        this.storageQueueName = queueName;
        this.readButUndeliveredMessages = new ConcurrentSkipListMap<>();
        this.maxNumberOfReadButUndeliveredMessages = AndesConfigurationManager.
                readValue(AndesConfiguration.PERFORMANCE_TUNING_DELIVERY_MAX_READ_BUT_UNDELIVERED_MESSAGES);
        this.messageDeliveryManager = MessageDeliveryManager.getInstance();
        this.lastPurgedTimestamp = 0L;
        this.messageStore = AndesContext.getInstance().getMessageStore();
        lastBufferedMessageId = 0;
        messageFetchSize = 1000;
    }

    /**
     * Start delivering messages for queue
     *
     * @param queue queue to deliver messages
     * @throws AndesException
     */
    public void startMessageDelivery(StorageQueue queue) throws AndesException {
        messageDeliveryManager.startMessageDeliveryForQueue(queue);
    }

    /**
     * Stop delivering messages for queue
     *
     * @param queue queue to stop message delivery
     * @throws AndesException
     */
    public void stopMessageDelivery(StorageQueue queue) throws AndesException {
        messageDeliveryManager.stopDeliveryForQueue(queue);
    }

    /***
     * @return Last purged timestamp of queue.
     */
    public Long getLastPurgedTimestamp() {
        return lastPurgedTimestamp;
    }

    /**
     * Read messages from persistent store and buffer.
     * as well.
     *
     * @return number of messages loaded to memory
     */
    public int bufferMessages() throws AndesException {

        List<DeliverableAndesMetadata> messagesReadFromStore = readMessagesFromMessageStore();

        for (DeliverableAndesMetadata message : messagesReadFromStore) {
            bufferMessage(message);
        }

        return messagesReadFromStore.size();
    }

    /**
     * Read messages from persistent store
     *
     * @return list of messages
     * @throws AndesException
     */
    private List<DeliverableAndesMetadata> readMessagesFromMessageStore() throws AndesException {
        List<DeliverableAndesMetadata> messagesRead;
        int numberOfRetries = 0;
        long startMessageId = lastBufferedMessageId + 1;
        try {

            //Read messages in the slot
            messagesRead = messageStore.getMetadataList(storageQueueName, startMessageId, messageFetchSize);

            if (log.isDebugEnabled()) {
                StringBuilder messageIDString = new StringBuilder();
                for (DeliverableAndesMetadata metadata : messagesRead) {
                    messageIDString.append(metadata.getMessageID()).append(" , ");
                }
                log.debug("Messages Read: " + messageIDString);
            }

        } catch (AndesException aex) {

            numberOfRetries = numberOfRetries + 1;

            if (numberOfRetries <= MAX_META_DATA_RETRIEVAL_COUNT) {

                String errorMsg = String.format("error occurred retrieving metadata" +
                        " list. retry count = %d", numberOfRetries);

                log.error(errorMsg, aex);
                messagesRead = messageStore.getMetadataList(storageQueueName, startMessageId, messageFetchSize);
            } else {
                String errorMsg = String.format("error occurred retrieving metadata list, in final attempt = %d. "
                        + "this slot will not be delivered " + "and become stale in message store", numberOfRetries);

                throw new AndesException(errorMsg, aex);
            }

        }

        if (!messagesRead.isEmpty()) {
            DeliverableAndesMetadata metadata = messagesRead.get(messagesRead.size()-1);
            lastBufferedMessageId = metadata.getMessageID();
        }

        if (log.isDebugEnabled()) {
            log.debug("Number of messages read from slot " + startMessageId
                    + " - " + lastBufferedMessageId + " is " + messagesRead.size()
                    + " storage queue= " + storageQueueName);
        }

        return messagesRead;
    }

    /**
     * Get buffered messages
     *
     * @return Collection with DeliverableAndesMetadata
     */
    public Collection<DeliverableAndesMetadata> getReadButUndeliveredMessages() {
        return readButUndeliveredMessages.values();
    }

    /**
     * Buffer messages to be delivered
     *
     * @param message message metadata to buffer
     */
    public void bufferMessage(DeliverableAndesMetadata message) {
        readButUndeliveredMessages.putIfAbsent(message.getMessageID(), message);
        message.markAsBuffered();
        MessageTracer.trace(message, MessageTracer.METADATA_BUFFERED_FOR_DELIVERY);
    }

    /**
     * Remove buffered message given the id.
     *
     * @param messageId id of the message to be removed.
     */
    public void removeBufferedMessage(long messageId) {
        readButUndeliveredMessages.remove(messageId);
        if (log.isDebugEnabled()) {
            log.debug("Removing scheduled to send message from buffer with id: " + messageId);
        }
    }

    /**
     * Returns boolean variable saying whether the internal buffer is full or not
     *
     * @return true if buffer is full and wise versa
     */
    public boolean isBufferFull() {
        return readButUndeliveredMessages.size() >= maxNumberOfReadButUndeliveredMessages;
    }

    /***
     * Clear the read-but-undelivered collection of messages of the given queue from memory
     *
     * @return Number of messages that was in the read-but-undelivered buffer
     */
    public int clearReadButUndeliveredMessages() {
        lastPurgedTimestamp = System.currentTimeMillis();
        int messageCount = readButUndeliveredMessages.size();
        readButUndeliveredMessages.clear();
        return messageCount;
    }

    /**
     * Removes all the messages from read buffer, deletes the slots and deletes all messages from persistent storage.
     *
     * @return the number of messages that were deleted from the store
     */
    public int purgeMessagesOfQueue() throws AndesException {

        try {
            //clear all in-memory messages
            purgeInMemoryMessagesOfQueue();

            // Delete messages from store
            int deletedMessageCount;
            if (!(DLCQueueUtils.isDeadLetterQueue(storageQueueName))) {
                // delete all messages for the queue
                deletedMessageCount = messageStore.deleteAllMessageMetadata(storageQueueName);
            } else {
                //delete all the messages in dlc
                deletedMessageCount = messageStore.clearDLCQueue(storageQueueName);
            }
            return deletedMessageCount;

        } catch (AndesException e) {
            // This will be a store-specific error.
            throw new AndesException("Error occurred when purging queue from store : " + storageQueueName, e);
        }

    }

    /**
     * Delete all in memory messages that are ready to be delivered.
     *
     * @return the number read but undelivered messages were removed from queue
     */
    public int purgeInMemoryMessagesOfQueue() {

        //clear all messages read to memory and return the slots
        return clearReadButUndeliveredMessages();

    }

    /**
     * Get message count for queue. This will query the persistent store.
     *
     * @return number of messages remaining in persistent store addressed to queue
     * @throws AndesException
     */
    public long getMessageCountForQueue() throws AndesException {
        long messageCount;
        if (!DLCQueueUtils.isDeadLetterQueue(storageQueueName)) {
            messageCount = messageStore.getMessageCountForQueue(storageQueueName);
        } else {
            messageCount = messageStore.getMessageCountForDLCQueue(storageQueueName);
        }
        return messageCount;
    }
}
