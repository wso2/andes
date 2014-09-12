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
import org.wso2.andes.server.stats.MessageStatus;
import org.wso2.andes.server.store.util.CassandraDataAccessException;

import java.util.List;
import java.util.Map;
import java.util.Set;

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
     * Generic interface to report the message store about the message status changes.
     * @param messageId The message Id
     * @param timeMillis The timestamp which the change happened
     * @param messageCounterKey The combined key which contains the message queue name and the state it changed to
     * @throws AndesException
     */
    public void addMessageStatusChange(long messageId, long timeMillis, MessageCounterKey messageCounterKey) throws AndesException;

    /**
     * Get stats of each message published, delivered and acknowledged times.
     *
     * @param queueName The queue name the message is in.
     * @param minDate The min value for the time range to retrieve in timemillis.
     * @param maxDate The max value for the time range to retrieve in timemillis.
     * @param minMessageId The min messageId to retrieve (use for paginated data retrieval. Else null).
     * @param limit Limit of the number of records to retrieve. The messages will be retrieved in ascending messageId order. If null MAX value of long will be set.
     * @param compareAllStatuses Compare all the statuses that are changed within the given period, else only the published time will be compared.
     * @return The message statuses orderd in message Id ascending order
     */
    public Set<MessageStatus> getMessageStatuses(String queueName, Long minDate, Long maxDate, Long minMessageId, Long limit, Boolean compareAllStatuses) throws AndesException;

    /**
     *
     * @param queueName The queue name the message is in.
     * @param minDate The min value for the time range to retrieve in timemillis.
     * @param maxDate The max value for the time range to retrieve in timemillis.
     * @return The row count.
     * @throws AndesException
     */
    public Long getMessageStatusCount(String queueName, Long minDate, Long maxDate) throws AndesException;

    /**
     * Get the times of messages within a given range arrived at a given status.
     *
     * @param queueName The queue name the message is in.
     * @param minDate The min value for the time range to retrieve in timemillis.
     * @param maxDate The max value for the time range to retrieve in timemillis.
     * @param minMessageId The min messageId to retrieve (use for paginated data retrieval. Else null).
     * @param limit Limit of the number of records to retrieve. The messages will be retrieved in ascending messageId order. If null MAX value of long will be set.
     * @param rangeColumn The message status change type to compare and return.
     * @return Map<MessageId MessageStatusChangeTime>
     * @throws AndesException
     */
    public Map<Long, Long> getMessageStatusChangeTimes(String queueName, Long minDate, Long maxDate, Long minMessageId, Long limit, MessageCounterKey.MessageCounterType rangeColumn) throws AndesException;

    /**
     * Start message status update statements executor schedule to batch execute collected message status change
     * updates or inserts after a given period of time.
     */
    public void startMessageStatusUpdateExecutor();

    /**
     * Stop message status update statements executor schedule either to shutdown the message broker, turn off stats
     * gathering or simply execute message status update statements only after the statement buffer is full.
     */
    public void stopMessageStatusUpdateExecutor();
}
