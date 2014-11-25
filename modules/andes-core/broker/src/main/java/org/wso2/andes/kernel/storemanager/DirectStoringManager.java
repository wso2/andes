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
import org.wso2.andes.server.cassandra.OnflightMessageTracker;
import org.wso2.andes.server.slot.SlotMessageCounter;
import org.wso2.andes.server.stats.PerformanceCounter;
import org.wso2.andes.server.util.AndesConstants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This DurableDirectStoringManager stores messages directly to the message store
 */
public class DirectStoringManager extends BasicStoringManager implements MessageStoreManager{

    private static Log log = LogFactory.getLog(DirectStoringManager.class);

    /**
     * message store which is used to persist messages
     */
    private MessageStore messageStore;

    public DirectStoringManager(MessageStore messageStore) {
        super(messageStore);
        this.messageStore = messageStore;
    }

    /**
     * Metadata stored in message store directly
     * @param metadata AndesMessageMetadata
     * @throws AndesException
     */
    @Override
    public void storeMetadata(AndesMessageMetadata metadata) throws AndesException{
        long start = System.currentTimeMillis();
        messageStore.addMetaData(metadata);
        List<AndesMessageMetadata> medatadataList = new ArrayList<AndesMessageMetadata>();
        medatadataList.add(metadata);
        /*
        update last message ID in slot message counter. When the slot is filled the last message
        ID of the slot will be submitted to the slot manager by SlotMessageCounter
         */
        if (AndesContext.getInstance().isClusteringEnabled()) {
            SlotMessageCounter.getInstance().recordMetaDataCountInSlot(medatadataList);
        }
        PerformanceCounter.warnIfTookMoreTime("Store Metadata" , start, 10);

        incrementQueueCount(metadata.getDestination(), 1);
        //record the successfully written message count
        PerformanceCounter.recordIncomingMessageWrittenToStore();
    }

    /**
     * store message metadata batch directly to store
     * @param messageMetadata
     *         metadata list to store
     * @throws AndesException
     */
    @Override
    public void storeMetaData(List<AndesMessageMetadata> messageMetadata) throws AndesException {
        long start = System.currentTimeMillis();
        messageStore.addMetaData(messageMetadata);
         /*
        update last message ID in slot message counter. When the slot is filled the last message
        ID of the slot will be submitted to the slot manager by SlotMessageCounter
         */
        if (AndesContext.getInstance().isClusteringEnabled()) {
            SlotMessageCounter.getInstance().recordMetaDataCountInSlot(messageMetadata);
        }
        PerformanceCounter.warnIfTookMoreTime("Store Metadata", start, 200);
        Map<String, Integer> destinationSeparatedMetadataCount = new HashMap<String,
                Integer>();
        for (AndesMessageMetadata message : messageMetadata) {
            //separate metadata queue-wise
            Integer msgCount = destinationSeparatedMetadataCount
                    .get(message.getDestination());
            if (msgCount == null) {
                msgCount =0;
            }
            msgCount = msgCount + 1;
            destinationSeparatedMetadataCount.put(message.getDestination(), msgCount);

            //record the successfully written message count
            PerformanceCounter.recordIncomingMessageWrittenToStore();
        }
        //increment message count for queues
        for(String queue : destinationSeparatedMetadataCount.keySet()) {
            incrementQueueCount(queue, destinationSeparatedMetadataCount.get(queue));
        }
    }

    /**
     * message content stored in message store directly
     * @param messagePart AndesMessagePart
     * @throws AndesException
     */
    @Override
    public void storeMessagePart(AndesMessagePart messagePart) throws AndesException{
        List<AndesMessagePart> partList = new ArrayList<AndesMessagePart>(1);
        partList.add(messagePart);
        long start = System.currentTimeMillis();
        messageStore.storeMessagePart(partList);
        PerformanceCounter.warnIfTookMoreTime("Store Message Content Chunks", start, 200);
    }

    /**
     * handle a sing ack directly with message store
     * @param ackData AndesAckData
     * @throws AndesException
     */
    @Override
    public void ackReceived(AndesAckData ackData) throws AndesException{
        List<AndesAckData> ackDataList = new ArrayList<AndesAckData>();
        ackDataList.add(ackData);
        ackReceived(ackDataList);

    }

    /**
     * Delete message parts directly from store
     * @param messageIdList
     *         list of message ids whose content should be removed
     * @throws AndesException
     */
    @Override
    public void deleteMessageParts(List<Long> messageIdList) throws AndesException {
         long start = System.currentTimeMillis();
         messageStore.deleteMessageParts(messageIdList);
        PerformanceCounter.warnIfTookMoreTime("Delete Message Content Chunks", start, 200);
    }

    /**
     * Handle a list of acks directly with DB. This is a blocking call
     * @param ackList
     *         ack message list to process
     * @throws AndesException
     */
    @Override
    public void ackReceived(List<AndesAckData> ackList) throws AndesException {
        List<AndesRemovableMetadata> removableMetadata = new ArrayList<AndesRemovableMetadata>();
        for (AndesAckData ack : ackList) {
            if(log.isDebugEnabled()) {
                log.debug("ack - direct store manager");
            }
            //for topics message is shared. If all acks are received only we should remove message
            boolean isOkToDeleteMessage = OnflightMessageTracker.getInstance().handleAckReceived(ack.channelID, ack.messageID);
            if(isOkToDeleteMessage) {
                if(log.isDebugEnabled()) {
                    log.debug("Ok to delete message id= " + ack.messageID);
                }
                removableMetadata.add(new AndesRemovableMetadata(ack.messageID, ack.destination, ack.msgStorageDestination));
            }

            OnflightMessageTracker.getInstance().decrementNonAckedMessageCount(ack.channelID);
            //record ack received
            PerformanceCounter.recordMessageRemovedAfterAck();
        }

        //remove messages permanently from store
        this.deleteMessages(removableMetadata, false);
    }

    /**
     * Directly increment message count for a queue in store
     * @param queueName
     *         name of the queue to decrement count
     * @param decrementBy
     *         decrement count by this value
     * @throws AndesException
     */
    @Override
    public void decrementQueueCount(String queueName , int decrementBy) throws AndesException {
        AndesContext.getInstance().getAndesContextStore().decrementMessageCountForQueue(queueName,
                                                                                        decrementBy);
    }

    /**
     * Directly decrement message count for a queue in store
     * @param queueName name of the queue to increment count
     * @param incrementBy increment count by this value
     * @throws AndesException
     */
    @Override
    public void incrementQueueCount(String queueName, int incrementBy) throws AndesException {
        AndesContext.getInstance().getAndesContextStore().incrementMessageCountForQueue(queueName,
                                                                                        incrementBy);
    }

    /**
     * Store a list of message parts directly to message store.
     * @param messageParts
     *         message parts to store
     * @throws AndesException
     */
    @Override
    public void storeMessagePart(List<AndesMessagePart> messageParts) throws AndesException {
        long start = System.currentTimeMillis();
         messageStore.storeMessagePart(messageParts);
        PerformanceCounter.warnIfTookMoreTime("Store Message Content Chunks", start, 200);
    }

    /**
     * Directly delete messages from store and move to DLC optionally
     * @param messagesToRemove
     *         messages to remove
     * @param moveToDeadLetterChannel
     *         whether to send to DLC
     * @throws AndesException
     */
    @Override
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
}
