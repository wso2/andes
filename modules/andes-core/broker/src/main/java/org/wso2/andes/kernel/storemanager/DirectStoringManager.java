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
 * This DurableDirectStoringManager stores messages directly to the message store
 */
public class DirectStoringManager extends BasicStoringManager implements MessageStoreManager{

    private static Log log = LogFactory.getLog(DirectStoringManager.class);

    /**
     * message store which is used to persist messages
     */
    private MessageStore messageStore;

    /**
     * Message store is used directly to store.
     * @param messageStore MessageStore implementation to be used as the durable message
     * @throws AndesException
     */
    @Override
    public void initialise(MessageStore messageStore) throws AndesException{
        this.messageStore = messageStore;
    }

    /**
     * Metadata stored in message store directly
     * @param metadata AndesMessageMetadata
     * @throws AndesException
     */
    @Override
    public void storeMetadata(AndesMessageMetadata metadata) throws AndesException{
        messageStore.addMetaData(metadata);
        incrementQueueCount(metadata.getDestination());
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
        messageStore.addMetaData(messageMetadata);
        //increment message count for queue
        for (AndesMessageMetadata md : messageMetadata) {
            incrementQueueCount(md.getDestination());
            //record the successfully written message count
            PerformanceCounter.recordIncomingMessageWrittenToStore();
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
        messageStore.storeMessagePart(partList);
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
         messageStore.deleteMessageParts(messageIdList);
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
            removableMetadata.add(new AndesRemovableMetadata(ack.messageID, ack.qName));
            //record ack received
            PerformanceCounter.recordMessageRemovedAfterAck();
        }
        //remove messages permanently from store
        deleteMessages(removableMetadata, false);
    }

    /**
     * Directly increment message count for a queue in store
     * @param queueName
     *         name of queue
     * @throws AndesException
     */
    @Override
    public void decrementQueueCount(String queueName) throws AndesException {
        AndesContext.getInstance().getAndesContextStore().decrementMessageCountForQueue(queueName, 1);
    }

    /**
     * Directly decrement message count for a queue in store
     * @param queueName
     *         name of queue
     * @throws AndesException
     */
    @Override
    public void incrementQueueCount(String queueName) throws AndesException {
        AndesContext.getInstance().getAndesContextStore().incrementMessageCountForQueue(queueName, 1);
    }

    /**
     * Store a list of message parts directly to message store.
     * @param messageParts
     *         message parts to store
     * @throws AndesException
     */
    @Override
    public void storeMessagePart(List<AndesMessagePart> messageParts) throws AndesException {
         messageStore.storeMessagePart(messageParts);
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
                AndesMessageMetadata metadata = messageStore.getMetaData(message.messageID);
                messageStore
                        .addMetaDataToQueue(AndesConstants.DEAD_LETTER_QUEUE_NAME, metadata);
                //increment message count of DLC
                incrementQueueCount(AndesConstants.DEAD_LETTER_QUEUE_NAME);
            }
        }

        //remove metadata
        for (String queueName : queueSeparatedRemoveMessages.keySet()) {
            messageStore.deleteMessageMetadataFromQueue(queueName,
                                                               queueSeparatedRemoveMessages
                                                                       .get(queueName));
        }

        if (!moveToDeadLetterChannel) {
            //remove content
            //TODO: - hasitha if a topic message be careful as it is shared
            deleteMessageParts(idsOfMessagesToRemove);
        }

        //remove these message ids from expiration tracking
        messageStore.deleteMessagesFromExpiryQueue(idsOfMessagesToRemove);

    }
}
