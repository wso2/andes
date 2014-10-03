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
import org.wso2.andes.server.cassandra.OnflightMessageTracker;
import org.wso2.andes.server.stats.PerformanceCounter;
import org.wso2.andes.server.util.AndesConstants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This DurableDirectStoringManager stores directly to the durable message store
 */
public class DurableDirectStoringManager implements MessageStoreManager{

    private static Log log = LogFactory.getLog(DurableDirectStoringManager.class);

    /**
     * Durable message store which is used to persist messages
     */
    private MessageStore durableMessageStore;

    /**
     * Durable message store is used directly to store.
     * @param durableMessageStore MessageStore implementation to be used as the durable message
     * @throws AndesException
     */
    @Override
    public void initialise(MessageStore durableMessageStore) throws AndesException{
        this.durableMessageStore = durableMessageStore;
    }

    /**
     * Metadata stored in durable message store directly
     * @param metadata AndesMessageMetadata
     * @param channelID channel ID
     * @throws AndesException
     */
    @Override
    public void storeMetadata(AndesMessageMetadata metadata, long channelID) throws AndesException{
        durableMessageStore.addMetaData(metadata);
    }

    /**
     * message content stored in durable message store directly
     * @param messagePart AndesMessagePart
     * @throws AndesException
     */
    @Override
    public void storeMessageContent(AndesMessagePart messagePart) throws AndesException{
        // NOTE: Should there be a method in message store to store a single AndesMessagePart?
        List<AndesMessagePart> partList = new ArrayList<AndesMessagePart>(1); // only 1 entry
        partList.add(messagePart);
        durableMessageStore.storeMessagePart(partList);
    }

    /**
     *
     * @param ackData AndesAckData
     * @throws AndesException
     */
    @Override
    public void ackReceived(AndesAckData ackData) throws AndesException{
        List<AndesAckData> ackDataList = new ArrayList<AndesAckData>();
        ackDataList.add(ackData);
        processAckReceived(ackDataList);

    }

    @Override
    public void deleteMessageParts(List<Long> messageIdList) throws AndesException {
         durableMessageStore.deleteMessageParts(messageIdList);
    }

    @Override
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

    @Override
    public void decrementQueueCount(String queueName) throws AndesException {
        AndesContext.getInstance().getAndesContextStore().decrementMessageCountForQueue(queueName, 1);
    }

    @Override
    public void incrementQueueCount(String queueName) throws AndesException {
        AndesContext.getInstance().getAndesContextStore().incrementMessageCountForQueue(queueName, 1);
    }

    @Override
    public void storeMessagePart(List<AndesMessagePart> messageParts) throws AndesException {
         durableMessageStore.storeMessagePart(messageParts);
    }

    @Override
    public void storeMetaData(List<AndesMessageMetadata> messageMetadata) throws AndesException {
         durableMessageStore.addMetaData(messageMetadata);
    }

    @Override
    public AndesMessagePart getContent(long messageId, int offsetValue) throws AndesException {
        return durableMessageStore.getContent(messageId, offsetValue);
    }

    @Override
    public void deleteMessages(List<AndesRemovableMetadata> messagesToRemove,
                               boolean moveToDeadLetterChannel) throws AndesException {
        List<Long> idsOfMessagesToRemove = new ArrayList<Long>();
        Map<String, List<AndesRemovableMetadata>> queueSeparatedRemoveMessages = new HashMap<String, List<AndesRemovableMetadata>>();

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
            //remove content
            //TODO: - hasitha if a topic message be careful as it is shared
            deleteMessageParts(idsOfMessagesToRemove);
        }

        //remove these message ids from expiration tracking
        durableMessageStore.deleteMessagesFromExpiryQueue(idsOfMessagesToRemove);

    }

    @Override
    public List<AndesRemovableMetadata> getExpiredMessages(int limit) throws AndesException {
        return durableMessageStore.getExpiredMessages(limit);
    }

    @Override
    public List<AndesMessageMetadata> getMetaDataList(String queueName, long firstMsgId,
                                                      long lastMsgID) throws AndesException {
        return durableMessageStore.getMetaDataList(queueName, firstMsgId, lastMsgID);
    }

    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(String queueName,
                                                                       long firstMsgId, int count)
            throws AndesException {
        return durableMessageStore.getNextNMessageMetadataFromQueue(queueName, firstMsgId, count);
    }

    @Override
    public void close() {
        durableMessageStore.close();
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
