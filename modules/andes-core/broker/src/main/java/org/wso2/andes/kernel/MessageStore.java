/*
 * Copyright (c) 2005-2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.kernel;

import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.store.HealthAwareStore;

import java.util.List;
import java.util.Map;

/**
 * Message meta data and content storing related data base types specific logic is abstracted out
 * using this interface.
 */
public interface MessageStore extends HealthAwareStore{

    /**
     * Initialise the MessageStore and returns the DurableStoreConnection used by store
     * @param connectionProperties ConfigurationProperties to be used to create the connection
     * @return DurableStoreConnection object created to make the connection to store
     * @throws AndesException
     */
    public DurableStoreConnection initializeMessageStore(AndesContextStore contextStore,
                                                         ConfigurationProperties
                                                                 connectionProperties)
            throws AndesException;

    /**
     * store a message content chunk set
     *
     * @param partList message content chunk list
     * @throws AndesException
     */
    public void storeMessagePart(List<AndesMessagePart> partList) throws AndesException;

    /**
     * read content chunk from store
     *
     * @param messageId id of the message chunk belongs
     * @param offsetValue chunk offset
     * @return message content part
     * @throws AndesException
     */
    public AndesMessagePart getContent(long messageId, int offsetValue) throws AndesException;

    /**
     * Read content for given message metadata list
     *  
     * @param messageIDList message id list for the content to be retrieved
     * @return <code>Map<Long, List<AndesMessagePart>></code> Message id and its corresponding message part list
     * @throws AndesException
     */
    public Map<Long, List<AndesMessagePart> > getContent(List<Long> messageIDList) throws AndesException;

    /**
     * store mata data of messages
     *
     * @param metadataList metadata list to store
     * @throws AndesException
     */
    public void addMetadata(List<AndesMessageMetadata> metadataList) throws AndesException;

    /**
     * Store level method to update a list of messages
     * The previousMessageID property of a message will be used to locate the existing message
     * The message id and destination will be replaced from the new ones
     * If was in the dead letter channel (DLC), it will be moved out of the DLC
     *
     * @param messageList list of message to be updated
     */
    public void updateMessage(List<AndesMessage> messageList) throws AndesException;

    /**
     * store metadata of a single message
     *
     * @param metadata metadata to store
     * @throws AndesException
     */
    public void addMetadata(AndesMessageMetadata metadata) throws AndesException;

    /**
     * Store messages into database.
     * @param messageList messages to be stored
     */
    public void storeMessages(List<AndesMessage> messageList) throws AndesException;

    /**
     * store metadata specifically under a queue
     *
     * @param queueName name of the queue to store metadata
     * @param metadata metadata to store
     * @throws AndesException
     */
    public void addMetadataToQueue(final String queueName, AndesMessageMetadata metadata)
            throws AndesException;

    /**
     * store metadata list specifically under a queue
     *
     * @param queueName name of the queue to store metadata
     * @param metadata metadata list to store
     * @throws AndesException
     */
    public void addMetadataToQueue(final String queueName, List<AndesMessageMetadata> metadata)
            throws AndesException;

    /**
     * Store a message in a different Queue without altering the meta data.
     *
     * @param messageId        The message Id to move
     * @param currentQueueName The current destination of the message
     * @param targetQueueName  The target destination Queue name
     * @throws AndesException
     */
    public void moveMetadataToQueue(long messageId, String currentQueueName, String targetQueueName) throws
            AndesException;

    /**
     * Add a message for a specific storage queue to dead letter channel (DLC)
     *
     * @param messageId            message ID of the message
     * @param storageQueueName     storage queue of the message
     * @param destinationQueueName destination queue of the message, used to create the DLC name
     * @throws AndesException
     */
    public void moveMetaDataToDLC(long messageId, String storageQueueName, String destinationQueueName) throws
            AndesException;


    /**
     * Update the meta data for the given message with the given information in the AndesMetaData. Update destination
     * and meta data bytes.
     *
     * @param currentQueueName The queue the Meta Data currently in
     * @param metadataList     The updated meta data list.
     * @throws AndesException
     */
    public void updateMetadataInformation(String currentQueueName, List<AndesMessageMetadata> metadataList) throws
            AndesException;

    /**
     * read metadata from store
     *
     * @param messageId id of the message
     * @return metadata of the message
     * @throws AndesException
     */
    public AndesMessageMetadata getMetadata(long messageId) throws AndesException;

    /**
     * Retrieve a list of metadata for within a range of message IDs and set the passed storage queue name for the
     * retrieved message
     *
     * @param storageQueueName storage queue name to be set
     * @param firstMsgId       starting value of the range of message IDs
     * @param lastMsgID        ending value of the range of message IDs
     * @param fromDLC          indication whether the message is retrived from the dead letter channel (DLC) or from
     *                         the storage
     * @return
     * @throws AndesException
     */
    public List<AndesMessageMetadata> getMetadataList(final String storageQueueName, long firstMsgId,
                                                      long lastMsgID, boolean fromDLC) throws AndesException;

    /**
     * read  a metadata list from store specifying a starting message id and a count
     *
     * @param storageQueueName name of the queue
     * @param firstMsgId       first id
     * @param count            how many messages to read
     * @param fromStorage      whether from storage or dead letter channel
     * @return list of metadata
     * @throws AndesException
     */
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(final String storageQueueName,
                                                                       long firstMsgId, int count, boolean fromStorage)
            throws AndesException;

    /**
     * delete message metadata of messages for a queue
     *
     * @param storageQueueName name of the queue
     * @param messagesToRemove messages to remove
     * @throws AndesException
     */
    public void deleteMessageMetadataFromQueue(final String storageQueueName, List<Long> messagesToRemove)
            throws AndesException;

    /**
     * Method to delete a set of messages from the database
     * If deleteAllMetaData is set to true, the storageQueue in the metadata table could be cleared at once
     * and the messages destined to that queue, given by the messagesToRemove list could be deleted from the content
     * table
     * Else, the messages in the list 'messagesToRemove' will be deleted
     *
     * @param storageQueueName  name of the queue
     * @param messagesToRemove  the list of messages to remove
     * @param deleteAllMetaData whether to delete all metadata destined to the storageQueue
     * @throws AndesException
     */
    public void deleteMessages(final String storageQueueName, List<Long> messagesToRemove, boolean deleteAllMetaData)
            throws AndesException;

    /**
     * get expired messages from store
     *
     * @param limit max num of messages to read
     * @return AndesRemovableMetadata
     * @throws AndesException
     */
    public List<AndesRemovableMetadata> getExpiredMessages(int limit) throws AndesException;

    /**
     * delete messages from expiry queue
     *
     * @param messagesToRemove message IDs to remove
     * @throws AndesException
     */
    public void deleteMessagesFromExpiryQueue(List<Long> messagesToRemove) throws AndesException;

    /**
     * add messages to expiry queue
     *
     * @param messageId id of the message to add
     * @param expirationTime expiration time
     * @param isMessageForTopic is message addressed to topic
     * @param destination destination message is addressed to
     * @throws AndesException
     */
    public void addMessageToExpiryQueue(Long messageId, Long expirationTime,
                                        boolean isMessageForTopic, String destination)
            throws AndesException;

    /***
     * Store level method to remove all metadata references to all messages addressed to a specific queue.
     * @param storageQueueName name of the queue being purged
     * @throws AndesException
     */
    public void deleteAllMessageMetadata(String storageQueueName) throws AndesException;

    /**
     * Method to delete a list of messages from the DLC
     *
     * @param messagesToRemove the list of message IDs to be removed
     */
    public void deleteMessageMetadataFromDLC(List<AndesRemovableMetadata> messagesToRemove) throws AndesException;

    /**
     * Get a list of message IDs addressed to a specific queue.
     *
     * @param storageQueueName name of the queue being purged.
     * @param startMessageID   the start message id to start searching from
     * @param storageType      the storage type from which to retrieved the message 0=normal storage, 1 = dead letter
     *                         channel, 2=both 1&2
     * @throws AndesException
     */
    public List<Long> getMessageIDsAddressedToQueue(String storageQueueName, Long startMessageID, int storageType)
            throws AndesException;

    /**
     * Add message counting entry for queue. queue count is initialised to zero. The counter for
     * created queue can then be incremented and decremented.
     * @see this.removeMessageCounterForQueue this.incrementMessageCountForQueue,
     * this.decrementMessageCountForQueue
     *
     * @param storageQueueName name of queue
     */
    public void addQueue(String storageQueueName) throws AndesException;

    /**
     * Get message count of queue
     *
     * @param fromStorage
     * @param storageQueueName name of queue
     * @param fromStorage      whether to read the message from storage or the dead letter channel
     * @return message count
     */
    public long getMessageCountForQueue(String storageQueueName, boolean fromStorage) throws AndesException;

    /**
     * Store level method to reset the message counter of a given queue to 0.
     * @param storageQueueName name of the queue being purged
     * @throws AndesException
     */
    public void resetMessageCounterForQueue(String storageQueueName) throws AndesException;

    /**
     * Remove Message counting entry
     *
     * @param storageQueueName name of the queue actually stored in DB
     */
    public void removeQueue(String storageQueueName) throws AndesException;


    /**
     * Increment message counter for a queue by a given incrementBy value
     * @param storageQueueName      name of the queue actually stored in DB
     * @param incrementBy           increment counter by
     * @throws AndesException
     */
    public void incrementMessageCountForQueue(String storageQueueName, long incrementBy) throws AndesException;


    /**
     * Decrement message counter for a queue
     *
     * @param storageQueueName      name of the queue actually stored in DB
     * @param decrementBy           decrement counter by
     * @throws AndesException
     */
    public void decrementMessageCountForQueue(String storageQueueName, long decrementBy) throws AndesException;

    /**
     * Store retained message list in the message store.
     * @see org.wso2.andes.kernel.AndesMessageMetadata#retain
     *
     * @param retainMap Retained messages map
     */
    public void storeRetainedMessages(Map<String,AndesMessage> retainMap) throws AndesException;

    /**
     * Return all topic names with retained messages in the database
     * @see org.wso2.andes.kernel.AndesMessageMetadata#retain
     *
     * @return Topic list with retained messages
     * @throws AndesException
     */
    public List<String> getAllRetainedTopics() throws AndesException;

    /**
     * Get all content parts for the given message ID. The message ID should belong to a
     * existing retained message.
     * @see org.wso2.andes.kernel.AndesMessageMetadata#retain
     *
     * @param messageID Message ID of the message
     * @return List of content parts
     * @throws AndesException
     */
    public Map<Integer, AndesMessagePart> getRetainedContentParts(long messageID) throws AndesException;

    /**
     * Return retained message metadata for the given destination. Null is returned if
     * no retained message is available for a destination.
     * @see org.wso2.andes.kernel.AndesMessageMetadata#retain
     *
     * @param destination Destination/Topic name
     * @return AndesMessageMetadata
     * @throws AndesException
     */
    public AndesMessageMetadata getRetainedMetadata(String destination) throws AndesException;

    /**
     * close the message store
     */
    public void close();

}
