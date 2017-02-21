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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.slot.Slot;
import org.wso2.andes.kernel.slot.SlotDeletionExecutor;
import org.wso2.andes.kernel.slot.SlotDeliveryWorkerManager;
import org.wso2.andes.kernel.slot.SlotReAssignTask;
import org.wso2.andes.kernel.subscription.StorageQueue;
import org.wso2.andes.server.queue.DLCQueueUtils;
import org.wso2.andes.tools.utils.MessageTracer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

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
    private SlotDeliveryWorkerManager messageDeliveryManager;

    /**
     * Name of the queue to handle
     */
    private String queueName;

    /**
     * Maximum number to retries retrieve metadata list for a given storage
     * queue ( in the errors occur in message stores)
     */
    private static final int MAX_META_DATA_RETRIEVAL_COUNT = 5;

    /**
     * In-memory message list scheduled to be delivered. These messages will be flushed
     * to subscriber.Used Map instead of Set because of https://wso2.org/jira/browse/MB-1624
     */
    private ConcurrentMap<Long, DeliverableAndesMetadata> readButUndeliveredMessages = new
            ConcurrentSkipListMap<>();

    /**
     * Map of slots read so far
     */
    private Map<String, Slot> slotsRead;

    /***
     * In case of a purge, we must store the timestamp when the purge was called.
     * This way we can identify messages received before that timestamp that fail and ignore them.
     */
    private long lastPurgedTimestamp;

    /**
     * Max number of messages to keep in buffer
     */
    private Integer maxNumberOfReadButUndeliveredMessages;

    /**
     * Used for asynchronously execute slot reassign task
     */
    private final ExecutorService executor;


    public MessageHandler(String queueName) {
        this.queueName = queueName;
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().
                setNameFormat("AndesReleaseSlotTaskExecutor").build();
        this.executor = Executors.newSingleThreadExecutor(namedThreadFactory);
        this.maxNumberOfReadButUndeliveredMessages = AndesConfigurationManager.
                readValue(AndesConfiguration.PERFORMANCE_TUNING_DELIVERY_MAX_READ_BUT_UNDELIVERED_MESSAGES);
        this.messageDeliveryManager = SlotDeliveryWorkerManager.getInstance();
        this.lastPurgedTimestamp = 0L;
        this.messageStore = AndesContext.getInstance().getMessageStore();
        this.slotsRead = new ConcurrentHashMap<>();
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
     * Read messages from persistent store and buffer indicated
     * by the slot. This will filter messages for overlapped slots
     * as well.
     *
     * @param currentSlot slot of which messages to load
     * @return number of messages loaded to memory
     */
    public int bufferMessages(Slot currentSlot) throws AndesException {

        List<DeliverableAndesMetadata> messagesReadFromStore = readMessagesFromMessageStore(currentSlot);

        //if no messages are in the slot range, delete the slot from coordinator. No use of it
        if (messagesReadFromStore.isEmpty()) {
            SlotDeletionExecutor.getInstance().scheduleToDelete(currentSlot);
        }

        Slot trackedSlot = slotsRead.get(currentSlot.getId());
        if (trackedSlot == null) {
            slotsRead.put(currentSlot.getId(), currentSlot);
            trackedSlot = currentSlot;
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Overlapped slot received. Slot ID " + trackedSlot.getId());
            }
        }

        //filter and removed already buffered messages
        filterOverlappedMessages(trackedSlot, messagesReadFromStore);

        trackedSlot.incrementPendingMessageCount(messagesReadFromStore.size());

        for (DeliverableAndesMetadata message : messagesReadFromStore) {
            bufferMessage(message);
        }

        return messagesReadFromStore.size();
    }

    /**
     * Read messages from persistent store indicated by the message slot
     *
     * @param slot message slot to load messages
     * @return list of messages
     * @throws AndesException
     */
    private List<DeliverableAndesMetadata> readMessagesFromMessageStore(Slot slot) throws AndesException {
        List<DeliverableAndesMetadata> messagesRead;
        int numberOfRetries = 0;
        try {

            //Read messages in the slot
            messagesRead = messageStore.getMetadataList(slot,
                    slot.getStorageQueueName(),
                    slot.getStartMessageId(),
                    slot.getEndMessageId());

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
                        " list for slot :" + " %s, retry count = %d", slot.toString(), numberOfRetries);

                log.error(errorMsg, aex);
                messagesRead = messageStore.getMetadataList(slot,
                        slot.getStorageQueueName(),
                        slot.getStartMessageId(),
                        slot.getEndMessageId());
            } else {
                String errorMsg = String.format("error occurred retrieving metadata list for slot "
                        + ": %s, in final attempt = %d. " + "this slot will not be delivered " +
                        "and become stale in message store", slot.toString(), numberOfRetries);

                throw new AndesException(errorMsg, aex);
            }

        }

        if (log.isDebugEnabled()) {
            log.debug("Number of messages read from slot " + slot.getStartMessageId()
                    + " - " + slot.getEndMessageId() + " is " + messagesRead.size()
                    + " storage queue= " + slot.getStorageQueueName());
        }

        return messagesRead;
    }

    /**
     * This will remove already buffered messages from the messagesRead list.
     * This is to avoid resending a message.
     *
     * @param slot     Slot which contains the given messages
     * @param messages Messages of the given slots
     */
    private void filterOverlappedMessages(Slot slot, List<DeliverableAndesMetadata> messages) {
        Iterator<DeliverableAndesMetadata> readMessageIterator = messages.iterator();
        while (readMessageIterator.hasNext()) {
            DeliverableAndesMetadata currentMessage = readMessageIterator.next();
            if (slot.checkIfMessageIsAlreadyAdded(currentMessage.getMessageID())) {
                if (log.isDebugEnabled()) {
                    log.debug("Tracker rejected message id= " + currentMessage.getMessageID()
                            + " from buffering "
                            + "to deliver. This is an already buffered message");
                }
                readMessageIterator.remove();
            } else {
                currentMessage.changeSlot(slot);
                slot.addMessageToSlotIfAbsent(currentMessage);
            }
        }
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
     * Returns boolean variable saying whether this destination has room or not
     *
     * @return whether this destination has room or not
     */
    public boolean messageBufferHasRoom() {
        boolean hasRoom = true;
        if (readButUndeliveredMessages.size() >= maxNumberOfReadButUndeliveredMessages) {
            hasRoom = false;
        }
        return hasRoom;
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
        for (Slot slot : slotsRead.values()) {
            if (log.isDebugEnabled()) {
                log.debug("clear tracking of messages for slot = " + slot);
            }
            slot.markMessagesOfSlotAsReturned();
        }
        slotsRead.clear();
        return messageCount;
    }

    /**
     * Delete all in memory messages that are ready to be delivered.
     *
     * @return how many read but undelivered messages were removed from queue
     */
    public int purgeMessagesOfQueue() {

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
        if (!DLCQueueUtils.isDeadLetterQueue(queueName)) {
            messageCount = messageStore.getMessageCountForQueue(queueName);
        } else {
            messageCount = messageStore.getMessageCountForDLCQueue(queueName);
        }
        return messageCount;
    }

    /**
     * Delete message slot read
     *
     * @param slotToDelete slot to delete
     */
    public void deleteSlot(Slot slotToDelete) {
        if (log.isDebugEnabled()) {
            log.debug("Releasing tracking of messages for slot " + slotToDelete.toString());
        }
        slotToDelete.deleteAllMessagesInSlot();
        slotsRead.remove(slotToDelete.getId());
    }

    /**
     * Schedule to release all non empty slots read back to the coordinator
     */
    public void releaseAllSlots() {
        executor.submit(new SlotReAssignTask(queueName));
    }


    /**
     * Dump all message status of the slots read to file given
     *
     * @param fileToWrite      file to write
     * @param storageQueueName name of storage queue
     * @throws AndesException
     */
    public void dumpAllSlotInformationToFile(File fileToWrite, String storageQueueName) throws AndesException {

        FileWriter information = null;
        try {
            information = new FileWriter(fileToWrite, true);
            for (Slot slot : slotsRead.values()) {
                List<DeliverableAndesMetadata> messagesOfSlot = slot.getAllMessagesOfSlot();
                if (!messagesOfSlot.isEmpty()) {

                    int writerFlushCounter = 0;
                    for (DeliverableAndesMetadata message : messagesOfSlot) {
                        information.append(storageQueueName).append(",").append(slot.getId()).append(",")
                                .append(message.dumpMessageStatus()).append("\n");
                        writerFlushCounter = writerFlushCounter + 1;
                        if (writerFlushCounter % 10 == 0) {
                            information.flush();
                        }
                    }

                    information.flush();
                }
            }
            information.flush();

        } catch (FileNotFoundException e) {
            log.error("File to write is not found", e);
            throw new AndesException("File to write is not found", e);
        } catch (IOException e) {
            log.error("Error while dumping message status to file", e);
            throw new AndesException("Error while dumping message status to file", e);
        } finally {
            try {
                if (information != null) {
                    information.close();
                }
            } catch (IOException e) {
                log.error("Error while closing file when dumping message status to file", e);
            }
        }
    }

}
