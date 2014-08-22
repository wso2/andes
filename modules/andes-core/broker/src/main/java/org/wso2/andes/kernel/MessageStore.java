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

import org.wso2.andes.server.stats.MessageCounterKey;
import org.wso2.andes.server.store.util.CassandraDataAccessException;

import java.util.List;
import java.util.Map;

public interface MessageStore {

    public void initializeMessageStore (DurableStoreConnection cassandraconnection) throws AndesException;

    public void storeMessagePart(List<AndesMessagePart> part)throws AndesException;

    public void deleteMessageParts(List<Long> messageIdList)throws AndesException;

    public AndesMessagePart getContent(String messageId, int offsetValue)throws AndesException;

    public void deleteMessageMetadataFromQueue(QueueAddress queueAddress,List<AndesRemovableMetadata> messagesToRemove) throws AndesException;

    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(QueueAddress queueAddress, long startMsgID, int count)throws AndesException;

    public void ackReceived(List<AndesAckData> ackList)throws AndesException;

    public void addMessageMetaData(QueueAddress queueAddress, List<AndesMessageMetadata> messageList)throws AndesException;

    public void moveMessageMetaData(QueueAddress sourceAddress, QueueAddress targetAddress, List<AndesMessageMetadata> messageList) throws AndesException;

    public long moveAllMessageMetaDataOfQueue(QueueAddress sourceAddress, QueueAddress targetAddress, String destinationQueue) throws AndesException;

    public int countMessagesOfQueue(QueueAddress queueAddress, String destinationQueueNameToMatch) throws AndesException;

    public long getMessageCountForQueue(String destinationQueueName) throws AndesException;

    public AndesMessageMetadata getMetaData(long messageId) throws AndesException;

    public void close();

    /**
     * interface to delete messages from expired messages collection
     * @param messagesToRemove
     * @throws AndesException
     */
    void deleteMessagesFromExpiryQueue(List<Long> messagesToRemove) throws AndesException;

    /**
     * Adds the received JMS Message ID along with its expiration time to MESSAGES_FOR_EXPIRY_COLUMN_FAMILY queue
     * @param messageId
     * @param expirationTime
     * @param isMessageForTopic,
     * @param destination
     * @throws org.wso2.andes.server.store.util.CassandraDataAccessException
     */
    void addMessageToExpiryQueue(Long messageId, Long expirationTime, boolean isMessageForTopic, String destination) throws CassandraDataAccessException;

    /**
     * Generic interface to delete all references to a message - to be used when deleting messages not in ackrecieved state
     * @param messagesToRemove
     * @param moveToDeadLetterChannel
     * @throws AndesException
     */
    void deleteMessages(List<AndesRemovableMetadata> messagesToRemove, boolean moveToDeadLetterChannel) throws AndesException;

    /**
     *
     * @param messageList
     */
    void moveToDeadLetterChannel(List<AndesRemovableMetadata> messageList);

    /**
     * Get Top @param limit messages having expiration times < current timestamp
     * if limit <= 0, fetches all entries matching criteria.
     * @param limit
     * @param columnFamilyName
     * @param keyspace
     * @return
     */
    List<AndesRemovableMetadata> getExpiredMessages(Long limit,String columnFamilyName, String keyspace);

    /**
     * Generic interface report the message store about the message status changes.
     * @param messageId The message Id
     * @param timeMillis The timestamp which the change happened
     * @param messageCounterKey The combined key which contains the message queue name and the state it changed to
     * @throws AndesException
     */
    public void addMessageStatusChange(long messageId, long timeMillis, MessageCounterKey messageCounterKey) throws AndesException;

    /**
     * Generic interface to get data to draw the message rate graph.
     * @param queueName The queue name to get data, if null all.
     * @return The message rate data. Map<MessageCounterType, Map<TimeInMillis, Number of messages>>
     * @throws AndesException
     */
    public Map<MessageCounterKey.MessageCounterType, java.util.Map<Long, Integer>> getMessageRates(String queueName, Long minDate, Long maxDate) throws AndesException;

    /**
     * Get the status of each of messages.
     * @param queueName The queue name to get data, if null all.
     * @return Message Status data. Map<Message Id, Map<Property, Value>>
     */
    public Map<Long, Map<String, String>> getMessageStatuses(String queueName, Long minDate, Long maxDate) throws AndesException;
}
