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

import org.wso2.andes.kernel.*;
import org.wso2.andes.server.queue.DLCQueueUtils;
import org.wso2.andes.server.stats.PerformanceCounter;
import org.wso2.andes.server.util.AndesConstants;

import java.util.List;

/**
 * This is the base class for Message Storing Manager. This class implements common methods
 * to all types of storing managers
 */
public abstract class BasicStoringManager implements MessageStoreManager {

    private MessageStore messageStore;

    public BasicStoringManager(MessageStore messageStore) {
        this.messageStore = messageStore;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesMessagePart getMessagePart(long messageId, int offsetValue) throws AndesException {
        long start = System.currentTimeMillis();
        AndesMessagePart messagePart =  messageStore.getContent(messageId, offsetValue);
        PerformanceCounter.warnIfTookMoreTime("Read Single Message Chunk", start, 30);
        return messagePart;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesRemovableMetadata> getExpiredMessages(int limit) throws AndesException {
        long start = System.currentTimeMillis();
        List<AndesRemovableMetadata> expiredMessages =  messageStore.getExpiredMessages(limit);
        PerformanceCounter.warnIfTookMoreTime("Read Expired Messages", start, 100);
        return expiredMessages;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getMetaDataList(String queueName, long firstMsgId,
                                                      long lastMsgID) throws AndesException {
        long start = System.currentTimeMillis();
        List<AndesMessageMetadata> metadataList =  messageStore.getMetaDataList(queueName, firstMsgId, lastMsgID);
        PerformanceCounter.warnIfTookMoreTime("Read Metadata With Limits", start, 300);
        return metadataList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(String queueName,
                                                                       long firstMsgId, int count)
            throws AndesException {
        long start = System.currentTimeMillis();
        List<AndesMessageMetadata> metadataList = messageStore.getNextNMessageMetadataFromQueue(queueName, firstMsgId, count);
        PerformanceCounter.warnIfTookMoreTime("Read Metadata ", start, 300);
        return metadataList;
    }

    /**
     * {@inheritDoc}
     */
    public void moveMetaDataToQueue(long messageId, String currentQueueName, String targetQueueName) throws
            AndesException {
        long start = System.currentTimeMillis();
        messageStore.moveMetaDataToQueue(messageId, currentQueueName, targetQueueName);
        PerformanceCounter.warnIfTookMoreTime("Move Metadata ", start, 10);
    }


    /**
     * {@inheritDoc}
     */
    public void updateMetaDataInformation(String currentQueueName, List<AndesMessageMetadata> metadataList) throws
            AndesException {
        long start = System.currentTimeMillis();
        messageStore.updateMetaDataInformation(currentQueueName, metadataList);
        PerformanceCounter.warnIfTookMoreTime("Update Metadata ", start, 100);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int purgeQueueFromStore(String storageQueueName) throws AndesException {

        try {
            // Retrieve message IDs addressed to the queue and keep track of message count for
            // the queue in the store
            List<Long> messageIDsAddressedToQueue = messageStore.getMessageIDsAddressedToQueue
                    (storageQueueName);

            Integer messageCountInStore = messageIDsAddressedToQueue.size();

            //  Clear message metadata from queues
            messageStore.deleteAllMessageMetadata(storageQueueName);

            // Reset message count for the specific queue
            messageStore.resetMessageCounterForQueue(storageQueueName);

            // There is only 1 DLC queue per tenant. So we have to read and parse the message
            // metadata and filter messages specific to a given queue.
            // This is pretty exhaustive. If there are 1000 DLC messages and only 10 are relevant
            // to the given queue, We still have to get all 1000
            // into memory. Options are to delete dlc messages leisurely with another thread,
            // or to break from original DLC pattern and maintain multiple DLC queues per each queue.
            Integer messageCountFromDLC = messageStore.deleteAllMessageMetadataFromDLC
                    (DLCQueueUtils.identifyTenantInformationAndGenerateDLCString
                            (storageQueueName, AndesConstants.DEAD_LETTER_QUEUE_NAME), storageQueueName);

            // Clear message content leisurely / asynchronously using retrieved message IDs
            messageStore.deleteMessageParts(messageIDsAddressedToQueue);

            // We can also delete messages from the messagesForExpiry store,
            // but since we have a thread running to clear them up as they expire,
            // we do not necessarily have to rush it here.
            // messageStore.deleteMessagesFromExpiryQueue(messageIDsAddressedToQueue);

            // If any other places in the store keep track of messages in future,
            // they should also be cleared here.

            return messageCountInStore + messageCountFromDLC;

        } catch (AndesException e) {
            // This will be a store-specific error. We could make all 5 operations into one atomic transaction so
            // that in case of an error data won't be obsolete, but we must do it in a proper generic manner (to
            // allow any collection of store methods to be executed in a single transaction.). To be done as a
            // separate task.
            throw new AndesException("Error occurred when purging queue from store : " + storageQueueName, e);
        }
    }

    /**
     * {@inheritDoc}
     * @param messageId ID of message
     * @param expirationTime The timestamp at which the message is set to expire
     * @param isMessageForTopic True if the message is addressed to a durable topic
     * @param destination final destination of the message.
     * @throws AndesException
     */
    @Override
    public void storeMessageInExpiryQueue(Long messageId, Long expirationTime,
                                          boolean isMessageForTopic, String destination) throws AndesException {
        messageStore.addMessageToExpiryQueue(messageId,expirationTime,isMessageForTopic,destination);
    }

    /**
     * {@inheritDoc}
     * @param messageId ID of message
     * @return
     * @throws AndesException
     */
    @Override
    public AndesMessageMetadata getMetadataOfMessage(Long messageId) throws AndesException {
        return messageStore.getMetaData(messageId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws InterruptedException {
        messageStore.close();
    }
}
