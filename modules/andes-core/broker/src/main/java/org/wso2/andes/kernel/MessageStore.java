/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.kernel;

import org.wso2.andes.configuration.ConfigurationProperties;
import org.wso2.andes.server.store.util.CassandraDataAccessException;

import java.util.List;

public interface MessageStore {

    public DurableStoreConnection initializeMessageStore(ConfigurationProperties
                                                        connectionProperties) throws AndesException;

    public void storeMessagePart(List<AndesMessagePart> partList) throws AndesException;

    public void deleteMessageParts(List<Long> messageIdList) throws AndesException;

    public AndesMessagePart getContent(long messageId, int offsetValue) throws AndesException;

    public void ackReceived(List<AndesAckData> ackList) throws AndesException;

    public void addMetaData(List<AndesMessageMetadata> metadataList) throws AndesException;

    public void addMetaData(AndesMessageMetadata metadata) throws AndesException;

    public void addMetaDataToQueue(final String queueName, AndesMessageMetadata metadata) throws AndesException;

    public void addMetadataToQueue(final String queueName, List<AndesMessageMetadata> metadata) throws AndesException;

    public long getMessageCountForQueue(final String destinationQueueName) throws AndesException;

    public AndesMessageMetadata getMetaData(long messageId) throws AndesException;

    public List<AndesMessageMetadata> getMetaDataList(final String queueName, long firstMsgId, long lastMsgID) throws AndesException;

    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(final String queueName, long firstMsgId, int count) throws AndesException;

    /**
     * This method can be used to delete messages from a special queue such as dead letter channel where messages
     * are stored in a different queue rather than it's actual destination.
     *
     * @param queueName        queue from which metadata to be removed ignoring the destination of metadata
     * @param messagesToRemove AndesMessageMetadata list to be reomoved from given queue
     * @throws AndesException
     */
    public void deleteMessageMetadataFromQueue(final String queueName, List<AndesRemovableMetadata> messagesToRemove) throws AndesException;

    List<AndesRemovableMetadata> getExpiredMessages(int limit) throws AndesException;

    /**
     * Generic interface to delete all references to a message - to be used when deleting messages not in ackrecieved state
     *
     * @param messagesToRemove
     * @param moveToDeadLetterChannel
     * @throws AndesException
     */
    void deleteMessages(List<AndesRemovableMetadata> messagesToRemove, boolean moveToDeadLetterChannel) throws AndesException;

    /**
     * close the database connection
     */
    public void close();

    /**
     * interface to delete messages from expired messages collection
     * @param messagesToRemove
     * @throws AndesException
     */
    void deleteMessagesFromExpiryQueue(List<Long> messagesToRemove) throws AndesException;

    @Deprecated
    public void deleteMessageMetadataFromQueue(QueueAddress queueAddress, List<AndesRemovableMetadata> messagesToRemove) throws AndesException;

    @Deprecated
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(QueueAddress queueAddress, long startMsgID, int count) throws AndesException;

    @Deprecated
    public void addMessageMetaData(QueueAddress queueAddress, List<AndesMessageMetadata> messageList) throws AndesException;

    @Deprecated
    public void moveMessageMetaData(QueueAddress sourceAddress, QueueAddress targetAddress, List<AndesMessageMetadata> messageList) throws AndesException;

    @Deprecated
    public long moveAllMessageMetaDataOfQueue(QueueAddress sourceAddress, QueueAddress targetAddress, String destinationQueue) throws AndesException;

    @Deprecated
    public int countMessagesOfQueue(QueueAddress queueAddress, String destinationQueueNameToMatch) throws AndesException;

    /**
     * Adds the received JMS Message ID along with its expiration time to MESSAGES_FOR_EXPIRY_COLUMN_FAMILY queue
     * @param messageId
     * @param expirationTime
     * @param isMessageForTopic,
     * @param destination
     * @throws org.wso2.andes.server.store.util.CassandraDataAccessException
     */
    @Deprecated
    void addMessageToExpiryQueue(Long messageId, Long expirationTime, boolean isMessageForTopic, String destination) throws CassandraDataAccessException;

    /**
     * @param messageList
     */
    @Deprecated
    void moveToDeadLetterChannel(List<AndesRemovableMetadata> messageList);

    /**
     * Get Top @param limit messages having expiration times < current timestamp
     * if limit <= 0, fetches all entries matching criteria.
     * @param limit
     * @param columnFamilyName
     * @param keyspace
     * @return
     */
    @Deprecated
    List<AndesRemovableMetadata> getExpiredMessages(Long limit, String columnFamilyName, String keyspace);
}
