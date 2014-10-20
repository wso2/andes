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

package org.wso2.andes.kernel;

import org.wso2.andes.configuration.ConfigurationProperties;

import java.util.List;

/**
 * Message meta data and content storing related data base types specific logic is abstracted out
 * using this interface.
 */
public interface MessageStore {

    /**
     * Initialise the MessageStore and returns the DurableStoreConnection used by store
     * @param connectionProperties ConfigurationProperties to be used to create the connection
     * @return DurableStoreConnection object created to make the connection to store
     * @throws AndesException
     */
    public DurableStoreConnection initializeMessageStore(ConfigurationProperties
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
     * delete message contents of messages from store
     *
     * @param messageIdList ids of messages
     * @throws AndesException
     */
    public void deleteMessageParts(List<Long> messageIdList) throws AndesException;

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
     * store mata data of messages
     *
     * @param metadataList metadata list to store
     * @throws AndesException
     */
    public void addMetaData(List<AndesMessageMetadata> metadataList) throws AndesException;

    /**
     * store metadata of a single message
     *
     * @param metadata metadata to store
     * @throws AndesException
     */
    public void addMetaData(AndesMessageMetadata metadata) throws AndesException;

    /**
     * store metadata specifically under a queue
     *
     * @param queueName name of the queue to store metadata
     * @param metadata metadata to store
     * @throws AndesException
     */
    public void addMetaDataToQueue(final String queueName, AndesMessageMetadata metadata)
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
    public void moveMetaDataToQueue(long messageId, String currentQueueName, String targetQueueName) throws
            AndesException;

    /**
     * Update the meta data for the given message with the given information in the AndesMetaData. Update destination
     * and meta data bytes.
     *
     * @param currentQueueName The queue the Meta Data currently in
     * @param metadataList     The updated meta data list.
     * @throws AndesException
     */
    public void updateMetaDataInformation(String currentQueueName, List<AndesMessageMetadata> metadataList) throws
            AndesException;

    /**
     * read metadata from store
     *
     * @param messageId id of the message
     * @return metadata of the message
     * @throws AndesException
     */
    public AndesMessageMetadata getMetaData(long messageId) throws AndesException;

    /**
     * read a metadata list from store specifying a message id range
     *
     * @param queueName name of the queue
     * @param firstMsgId first id of the range
     * @param lastMsgID last id of the range
     * @return list of metadata
     * @throws AndesException
     */
    public List<AndesMessageMetadata> getMetaDataList(final String queueName, long firstMsgId,
                                                      long lastMsgID) throws AndesException;

    /**
     * read  a metadata list from store specifying a starting message id and a count
     *
     * @param queueName name of the queue
     * @param firstMsgId first id
     * @param count how many messages to read
     * @return list of metadata
     * @throws AndesException
     */
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(final String queueName,
                                                                       long firstMsgId, int count)
            throws AndesException;

    /**
     * delete message metadata of messages for a queue
     *
     * @param queueName name of the queue
     * @param messagesToRemove messages to remove
     * @throws AndesException
     */
    public void deleteMessageMetadataFromQueue(final String queueName,
                                               List<AndesRemovableMetadata> messagesToRemove)
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
     * @param messagesToRemove messages to remove
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

    /**
     * close the message store
     */
    public void close();

}
