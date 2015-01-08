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

package org.wso2.andes.store.cassandra.cql;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.stats.PerformanceCounter;
import org.wso2.andes.server.store.util.CQLDataAccessHelper;
import org.wso2.andes.server.store.util.CassandraDataAccessException;
import org.wso2.andes.server.util.AlreadyProcessedMessageTracker;
import org.wso2.andes.server.util.AndesConstants;
import org.wso2.andes.store.cassandra.CassandraConstants;
import org.wso2.andes.store.cassandra.cql.dao.CQLQueryBuilder;
import org.wso2.andes.store.cassandra.cql.dao.CassandraHelper;
import org.wso2.andes.store.cassandra.cql.dao.GenericCQLDAO;

import java.util.*;

/**
 * This is the implementation of MessageStore that deals with Cassandra no SQL DB.
 * It uses CQL for making queries
 */
public class CQLBasedMessageStoreImpl implements org.wso2.andes.kernel.MessageStore {
    private static Log log = LogFactory.getLog(CQLBasedMessageStoreImpl.class);

    private AlreadyProcessedMessageTracker alreadyMovedMessageTracker;

    /**
     * Cassandra cluster object.
     */
    private Cluster cluster;
    /**
     * CQLConnection object which tracks the Cassandra connection
     */
    private CQLConnection cqlConnection;

    public CQLBasedMessageStoreImpl() {
    }

    //todo remove this method after testing
    public void setCQLConnection(CQLConnection cqlConnection) {
        this.cqlConnection = cqlConnection;
    }

    public DurableStoreConnection initializeMessageStore(ConfigurationProperties
                                                                 connectionProperties)
            throws AndesException {

        // create connection object
        //todo remove this if after testing
        if (cqlConnection == null) {
            cqlConnection = new CQLConnection();
        }
        cqlConnection.initialize(connectionProperties);

        // get cassandra cluster and create column families
        initializeCassandraMessageStore(cqlConnection);

        alreadyMovedMessageTracker = new AlreadyProcessedMessageTracker("Message-move-tracker",
                15000000000L, 10);

        return cqlConnection;
    }

    private void initializeCassandraMessageStore(CQLConnection cqlConnection) throws
            AndesException {
        try {
            cluster = cqlConnection.getCluster();
            createColumnFamilies(cqlConnection);
        } catch (CassandraDataAccessException e) {
            log.error("Error while initializing cassandra message store", e);
            throw new AndesException("Error while initializing cassandra message store",e);
        }
    }

    /**
     * Create a cassandra column families for andes usage
     *
     * @throws CassandraDataAccessException
     */
    private void createColumnFamilies(CQLConnection connection) throws CassandraDataAccessException {
        int gcGraceSeconds = connection.getGcGraceSeconds();

        CQLDataAccessHelper.createColumnFamily(CassandraConstants.MESSAGE_CONTENT_COLUMN_FAMILY,
                CassandraConstants.KEYSPACE, this.cluster, CassandraConstants.LONG_TYPE, DataType.blob(), gcGraceSeconds);
        CQLDataAccessHelper.createColumnFamily(CassandraConstants.META_DATA_COLUMN_FAMILY,
                CassandraConstants.KEYSPACE, this.cluster, CassandraConstants.LONG_TYPE, DataType.blob(), gcGraceSeconds);
        CQLDataAccessHelper.createCounterColumnFamily(CassandraConstants.MESSAGE_COUNTERS_COLUMN_FAMILY,
                CassandraConstants.KEYSPACE, this.cluster, gcGraceSeconds);
        CQLDataAccessHelper.createMessageExpiryColumnFamily(CassandraConstants.MESSAGES_FOR_EXPIRY_COLUMN_FAMILY,
                CassandraConstants.KEYSPACE, gcGraceSeconds);
    }

    public void storeMessagePart(List<AndesMessagePart> partList) throws AndesException {
        try {
            List<Insert> inserts = new ArrayList<Insert>();
            for (AndesMessagePart part : partList) {
                final String rowKey = AndesConstants.MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX
                        + part.getMessageID();
                Insert insert = CQLDataAccessHelper.addMessageToQueue(CassandraConstants.KEYSPACE,
                        CassandraConstants
                                .MESSAGE_CONTENT_COLUMN_FAMILY,
                        rowKey, part.getOffSet(),
                        part.getData(), false);
                inserts.add(insert);
            }
            GenericCQLDAO.batchExecuteWrite(CassandraConstants.KEYSPACE,
                    inserts.toArray(new Insert[inserts.size()]));

        } catch (CassandraDataAccessException e) {
            log.error("Error in adding the message part to the store",e);
            throw new AndesException("Error in adding the message part to the store", e);
        }
    }

    //TODO: hasitha - should move this logic to storing manager
    public void duplicateMessageContent(long messageId, long messageIdOfClone)
            throws AndesException {
        String originalRowKey = AndesConstants.MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX +
                messageId;
        String cloneMessageKey = AndesConstants.MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX +
                messageIdOfClone;
        try {

            long tryCount = 0;
            //read from store
            List<AndesMessageMetadata> messages = CQLDataAccessHelper
                    .getMessagesFromQueue(originalRowKey.trim(),
                            CassandraConstants.MESSAGE_CONTENT_COLUMN_FAMILY,
                            CassandraConstants.KEYSPACE, 0, Long.MAX_VALUE,
                            Long.MAX_VALUE, true, false);

            //if there are values duplicate them
            if (!messages.isEmpty()) {
                for (AndesMessageMetadata msg : messages) {
                    long offset = msg.getMessageID();
                    final byte[] chunkData = msg.getMetadata();
                    CQLDataAccessHelper.addMessageToQueue(CassandraConstants.KEYSPACE,
                            CassandraConstants
                                    .MESSAGE_CONTENT_COLUMN_FAMILY,
                            cloneMessageKey,
                            offset, chunkData, false);

                    if (log.isDebugEnabled()) {
                        log.debug("DUPLICATE>> new id " + messageIdOfClone + " cloned from id " + messageId + " offset" + offset);
                    }
                }
            } else {
                tryCount += 1;
                if (tryCount == 3) {
                    throw new AndesException(
                            "Original Content is not written. Cannot duplicate content. Tried 3 " +
                                    "times");
                }
                try {
                    Thread.sleep(20 * tryCount);
                } catch (InterruptedException e) {
                    //silently ignore
                }

                this.duplicateMessageContent(messageId, messageIdOfClone);
            }

        } catch (CassandraDataAccessException e) {
            log.error("Error occurred while duplicating content of message ID : " + messageId,e);
            throw new AndesException("Error occurred while duplicating content of message ID : " + messageId,e);
        }
    }

    /**
     * Get message content part of a message.
     *
     * @param messageId   The message Id
     * @param offsetValue Message content offset value
     * @return Message content part
     * @throws AndesException
     */
    public AndesMessagePart getContent(long messageId, int offsetValue) throws AndesException {
        AndesMessagePart messagePart;
        try {
            String rowKey = AndesConstants.MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX + messageId;
            messagePart = CQLDataAccessHelper.getMessageContent(rowKey.trim(),
                    CassandraConstants
                            .MESSAGE_CONTENT_COLUMN_FAMILY,
                    CassandraConstants.KEYSPACE,
                    messageId, offsetValue);
        } catch (CassandraDataAccessException e) {
            log.error("Error in reading content messageID= " + messageId + " offset=" + offsetValue,e);
            throw new AndesException("Error in reading content messageID=" + messageId + " offset=" + offsetValue,e);
        }
        return messagePart;
    }

    /**
     * Store metadata list to cassandra
     *
     * @param metadataList metadata list to store
     * @throws AndesException
     */
    @Override
    public void addMetaData(List<AndesMessageMetadata> metadataList) throws AndesException {

        try {
            List<Insert> inserts = new ArrayList<Insert>();
            for (AndesMessageMetadata md : metadataList) {
                Insert insert = CQLDataAccessHelper.addMessageToQueue(CassandraConstants.KEYSPACE,
                        CassandraConstants
                                .META_DATA_COLUMN_FAMILY,
                        md.getStorageQueueName(),
                        md.getMessageID(),
                        md.getMetadata(), false);

                inserts.add(insert);
            }
            long start = System.currentTimeMillis();

            GenericCQLDAO.batchExecuteWrite(CassandraConstants.KEYSPACE, inserts.toArray(new Insert[inserts.size()]));
            int latency = (int) (System.currentTimeMillis() - start);
            if (latency > 1000) {

                log.warn("Cassandra writing took " + latency + " millisecoonds for batch of " +
                        metadataList.size());
            }
            if(log.isDebugEnabled()) {
                PerformanceCounter.recordIncomingMessageWrittenToCassandraLatency(latency);
            }

         } catch (CassandraDataAccessException e) {
            log.error("Error writing incoming messages to Cassandra", e);
            throw new AndesException("Error writing incoming messages to Cassandra", e);
        }
    }

    /**
     * Add a metadata object to Cassandra
     *
     * @param metadata metadata to store
     * @throws AndesException
     */
    @Override
    public void addMetaData(AndesMessageMetadata metadata) throws AndesException {
        String storageQueueName = metadata.getStorageQueueName();
        try {
            Insert insert = CQLDataAccessHelper.addMessageToQueue(CassandraConstants.KEYSPACE,
                    CassandraConstants
                            .META_DATA_COLUMN_FAMILY,
                    storageQueueName,
                    metadata.getMessageID(),
                    metadata.getMetadata(), false);

            GenericCQLDAO.executeWrite(CassandraConstants.KEYSPACE, insert.getQueryString());
        } catch (CassandraDataAccessException e) {
            String errorString = "Error writing incoming message to cassandra.";
            log.error(errorString, e);
            throw new AndesException(errorString, e);
        }
    }

    /**
     * Add the given andes meta data to the given queue row in META_DATA_COLUMN_FAMILY. This can be
     * used when inserting meta data to a different queue row than it's destination eg:- Dead Letter
     * Queue.
     *
     * @param storageQueueName The queue name to add meta data to
     * @param metadata  The andes meta data to add
     * @throws AndesException
     */
    @Override
    public void addMetaDataToQueue(String storageQueueName, AndesMessageMetadata metadata)
            throws AndesException {
        String destination;

        if (storageQueueName == null) {
            destination = metadata.getStorageQueueName();
        } else {
            destination = storageQueueName;
        }

        try {
            Insert insert = CQLDataAccessHelper.addMessageToQueue(CassandraConstants.KEYSPACE,
                    CassandraConstants
                            .META_DATA_COLUMN_FAMILY,
                    destination,
                    metadata.getMessageID(),
                    metadata.getMetadata(), false);

            GenericCQLDAO.executeWrite(CassandraConstants.KEYSPACE, insert.getQueryString());
        } catch (CassandraDataAccessException e) {
            String errorString = "Error writing incoming message to cassandra.";
            log.error(errorString, e);
            throw new AndesException(errorString, e);
        }

    }

    /**
     * Add metadata to a specific queue
     *
     * @param queueName    name of the queue to store metadata
     * @param metadataList metadata list to insert
     * @throws AndesException
     */
    @Override
    public void addMetadataToQueue(String queueName, List<AndesMessageMetadata> metadataList)
            throws AndesException {
        try {
            Insert insert = null;
            for (AndesMessageMetadata metadata : metadataList) {
                insert = CQLDataAccessHelper.addMessageToQueue(CassandraConstants.KEYSPACE,
                        CassandraConstants
                                .META_DATA_COLUMN_FAMILY,
                        queueName,
                        metadata.getMessageID(),
                        metadata.getMetadata(), false);
            }
            if (insert != null) {
                GenericCQLDAO.executeWrite(CassandraConstants.KEYSPACE, insert.getQueryString());
            }
        } catch (CassandraDataAccessException e) {
            String errorString = "Error writing incoming message to cassandra.";
            log.error(errorString, e);
            throw new AndesException(errorString, e);
        }
    }

    /**
     * Remove message meta from the current column family and move it to a different column family
     * without altering the meta data.
     *
     * @param messageId        The message Id to move
     * @param currentQueueName The current destination of the message
     * @param targetQueueName  The target destination Queue name
     * @throws AndesException
     */
    @Override
    public void moveMetaDataToQueue(long messageId, String currentQueueName, String targetQueueName)
            throws
            AndesException {
        List<AndesMessageMetadata> messageMetadataList = getMetaDataList(currentQueueName,
                messageId, messageId);

        if (messageMetadataList.size() == 0) {
            throw new AndesException(
                    "Message MetaData not found to move the message to " + targetQueueName);
        }
        ArrayList<AndesRemovableMetadata> removableMetaDataList = new
                ArrayList<AndesRemovableMetadata>();
        removableMetaDataList.add(new AndesRemovableMetadata(messageId, currentQueueName,currentQueueName));

        addMetaDataToQueue(targetQueueName, messageMetadataList.get(0));
        deleteMessageMetadataFromQueue(currentQueueName, removableMetaDataList);
    }

    /**
     * Remove the meta data from the current column family and insert the new meta in it's place in
     * the new destination column family.
     *
     * @param currentQueueName The queue the Meta Data currently in
     * @param metadataList     The updated meta data list.
     * @throws AndesException
     */
    @Override
    public void updateMetaDataInformation(String currentQueueName,
                                          List<AndesMessageMetadata> metadataList) throws
            AndesException {
        try {
            // Step 1 - Insert the new meta data
            List<Insert> inserts = new ArrayList<Insert>();
            for (AndesMessageMetadata metadata : metadataList) {
                Insert insert = CQLDataAccessHelper.addMessageToQueue(CassandraConstants.KEYSPACE,
                        CassandraConstants
                                .META_DATA_COLUMN_FAMILY,
                        metadata.getStorageQueueName(),
                        metadata.getMessageID(),
                        metadata.getMetadata(),
                        false);

                inserts.add(insert);
            }


            long start = System.currentTimeMillis();
            GenericCQLDAO.batchExecuteWrite(CassandraConstants.KEYSPACE,
                    inserts.toArray(new Insert[inserts.size()]));

            if(log.isDebugEnabled()) {
                PerformanceCounter.recordIncomingMessageWrittenToCassandraLatency(
                        (int) (System.currentTimeMillis() -
                                start));
            }

            // Step 2 - Delete the old meta data when inserting new meta is complete to avoid losing messages
            List<Statement> statements = new ArrayList<Statement>();
            for (AndesMessageMetadata metadata : metadataList) {

                Delete delete = CQLDataAccessHelper
                        .deleteLongColumnFromRaw(CassandraConstants.KEYSPACE,
                                CassandraConstants.META_DATA_COLUMN_FAMILY,
                                currentQueueName, metadata.getMessageID(), false);
                statements.add(delete);
            }

            GenericCQLDAO.batchExecuteWrite(CassandraConstants.KEYSPACE,
                    statements.toArray(new Statement[statements.size()]));

        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error updating message meta data", e);
        }
    }

    /**
     * retrieve metadata from store
     *
     * @param messageId id of the message
     * @return AndesMessageMetadata of given message id
     * @throws AndesException
     */
    @Override
    public AndesMessageMetadata getMetaData(long messageId) throws AndesException {
        AndesMessageMetadata metadata;
        try {

            Map<String,byte[]> messageInfo = CQLDataAccessHelper
                    .getMessageMetaDataFromQueue(CassandraConstants.META_DATA_COLUMN_FAMILY,
                            CassandraConstants.KEYSPACE, messageId);
            Map.Entry<String, byte[]> msgEntry = messageInfo.entrySet().iterator().next();
            metadata = new AndesMessageMetadata(messageId, msgEntry.getValue(), true);
            metadata.setStorageQueueName(msgEntry.getKey());

        } catch (CassandraDataAccessException e) {
            log.error("Error in getting meta data of provided message id", e);
            throw new AndesException("Error in getting meta data for messageID " + messageId, e);
        }
        return metadata;
    }

    /**
     * {@inheritDoc}
     * CQL range query may return more records than limit size of STANDARD_PAGE_SIZE because slot hasn't a hard limit.
     * In such case we need to get all metadata between firstMsgId and lastMsgID
     */
    @Override
    public List<AndesMessageMetadata> getMetaDataList(String queueName, long firstMsgId,
                                                      long lastMsgID) throws AndesException {
        try {
            //Contains all metadata between firstMsgId and lastMsgID
            List<AndesMessageMetadata> allMetadataList = new ArrayList<AndesMessageMetadata>();
            //Get first set of metadata list between firstMsgId and lastMsgID
            List<AndesMessageMetadata> metadataList = CQLDataAccessHelper
                    .getMessagesFromQueue(queueName,
                            CassandraConstants.META_DATA_COLUMN_FAMILY,
                            CassandraConstants.KEYSPACE, firstMsgId, lastMsgID,
                            CQLDataAccessHelper.STANDARD_PAGE_SIZE,
                            true, true);
            allMetadataList.addAll(metadataList);
            int metadataCount = metadataList.size();
            //Check metadata list size equal to greater than to STANDARD_PAGE_SIZE to retry again
            while (metadataCount >= CQLDataAccessHelper.STANDARD_PAGE_SIZE) {
                //Get nextFirstMsgId
                long nextFirstMsgId = metadataList.get(metadataCount - 1).getMessageID();
                //Break retrying if all messages received
                if(nextFirstMsgId == lastMsgID) {
                    break;
                }
                metadataList = CQLDataAccessHelper
                        .getMessagesFromQueue(queueName,
                                CassandraConstants.META_DATA_COLUMN_FAMILY,
                                CassandraConstants.KEYSPACE, nextFirstMsgId, lastMsgID,
                                CQLDataAccessHelper.STANDARD_PAGE_SIZE,
                                true, true);
                allMetadataList.addAll(metadataList);
                metadataCount = metadataList.size();
            }
            return allMetadataList;
        } catch (CassandraDataAccessException e) {
            String error = "Error while getting meta data for queue : " + queueName + " from msgId : " + firstMsgId + " to msgID : " + lastMsgID;
            log.error(error,e);
            throw new AndesException(error,e);
        }


    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(String storageQueueName,
                                                                       long firstMsgId, int count)
            throws AndesException {
        try {
            return CQLDataAccessHelper
                    .getMessagesFromQueue(storageQueueName,
                            CassandraConstants.META_DATA_COLUMN_FAMILY,
                            CassandraConstants.KEYSPACE, firstMsgId,
                            Long.MAX_VALUE, count, true, true);
        } catch (CassandraDataAccessException e) {
            log.error("Error while retrieving "+ count + " messages for queue : " +
                    storageQueueName + " from message ID : " + firstMsgId,e);
            throw new AndesException("Error while retrieving "+ count + " messages for queue : " +
                    storageQueueName + " from message ID : " + firstMsgId,e);
        }

    }

    /**
     * Delete a given message meta data list from the the given storageQueueName row in
     * META_DATA_COLUMN_FAMILY.
     *
     * @param storageQueueName        queue from which metadata to be removed ignoring the destination of metadata
     * @param messagesToRemove AndesMessageMetadata list to be removed from given queue
     * @throws AndesException
     */
    @Override
    public void deleteMessageMetadataFromQueue(String storageQueueName,
                                               List<AndesRemovableMetadata> messagesToRemove)
            throws AndesException {
        try {
            List<Statement> statements = new ArrayList<Statement>();
            for (AndesRemovableMetadata message : messagesToRemove) {
                Delete delete = CQLDataAccessHelper
                        .deleteLongColumnFromRaw(CassandraConstants.KEYSPACE,
                                CassandraConstants.META_DATA_COLUMN_FAMILY,
                                storageQueueName, message.getMessageID(), false);
                statements.add(delete);
            }
            GenericCQLDAO.batchExecuteWrite(CassandraConstants.KEYSPACE,
                    statements.toArray(new Statement[statements.size()]));

        } catch (CassandraDataAccessException e) {
            log.error("Error while deleting messages", e);
            throw new AndesException("Error while deleting messages",e);
        }
    }

    /**
     * {@inheritDoc}
     * @param messageIdList
     */
    @Override
    public void deleteMessageParts(Collection<Long> messageIdList) throws AndesException {
        try {
            List<String> rows2Remove = new ArrayList<String>();
            for (long messageId : messageIdList) {
                rows2Remove.add(AndesConstants.MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX + messageId);
            }
            //remove content
            if (!rows2Remove.isEmpty()) {
                CQLDataAccessHelper.deleteIntegerRowListFromColumnFamily(
                        CassandraConstants.MESSAGE_CONTENT_COLUMN_FAMILY, rows2Remove,
                        CassandraConstants.KEYSPACE);
            }
        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while deleting message parts for message list" ,e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void close() {
        alreadyMovedMessageTracker.shutDownMessageTracker();
        cqlConnection.close();
    }

    @Override
    public List<AndesRemovableMetadata> getExpiredMessages(int limit) throws AndesException{
        // todo implement
        return new ArrayList<AndesRemovableMetadata>();
    }

    /**
     * Adds the received JMS Message ID along with its expiration time to
     * "MESSAGES_FOR_EXPIRY_COLUMN_FAMILY" queue
     *
     * @param messageId      id of the message
     * @param expirationTime expiration time
     * @throws AndesException
     */
    @Override
    public void addMessageToExpiryQueue(Long messageId, Long expirationTime,
                                        boolean isMessageForTopic, String destination)
            throws AndesException {

        final String columnFamily = CassandraConstants.MESSAGES_FOR_EXPIRY_COLUMN_FAMILY;

        if (messageId == 0) {
            throw new AndesException("Can't add data with queueType = " + columnFamily +
                    " and messageId  = " + messageId + " expirationTime = " + expirationTime);
        }

        try {
            Map<String, Object> keyValueMap = new HashMap<String, Object>();
            keyValueMap.put(CQLDataAccessHelper.MESSAGE_ID, messageId);
            keyValueMap.put(CQLDataAccessHelper.MESSAGE_EXPIRATION_TIME, expirationTime);
            keyValueMap.put(CQLDataAccessHelper.MESSAGE_DESTINATION, destination);
            keyValueMap.put(CQLDataAccessHelper.MESSAGE_IS_FOR_TOPIC, isMessageForTopic);

            Insert insert = CQLQueryBuilder
                    .buildSingleInsert(CassandraConstants.KEYSPACE, columnFamily, keyValueMap);

            GenericCQLDAO.executeWrite(CassandraConstants.KEYSPACE, insert.getQueryString());

        } catch (CassandraDataAccessException e) {
            log.error("Error while adding message to expiry queue", e);
            throw new AndesException("Error while adding message to expiry queue",e);
        }

    }

    /**
     * {@inheritDoc}
     *
     * @param storageQueueName name of the queue being purged
     * @throws AndesException
     */
    @Override
    public void deleteAllMessageMetadata(String storageQueueName) throws AndesException {
        try {
            CQLDataAccessHelper.deleteRowFromColumnFamily(CassandraConstants
                    .META_DATA_COLUMN_FAMILY, storageQueueName, CassandraConstants.KEYSPACE);

        } catch (CassandraDataAccessException e) {
            log.error("Error while deleting messages from queue : " + storageQueueName,e);
            throw new AndesException("Error while deleting messages from queue : " + storageQueueName, e);
        }
    }

    /**
     * {@inheritDoc}
     * @param storageQueueName name of the queue being purged
     * @throws AndesException
     */
    @Override
    public void resetMessageCounterForQueue(String storageQueueName) throws AndesException {
        try {

            // Resetting counters is strictly prohibited by Cassandra by design, and even re-inserting a record
            // won't reset the value but set the counter to null.
            // Therefore we have to first get the counter value and decrement it in another query.
            // This could be problematic in a race condition between another counter modification.
            Long messageCountOfQueue = CQLDataAccessHelper.getCountValue(CassandraConstants.KEYSPACE,
                    CassandraConstants.MESSAGE_COUNTERS_COLUMN_FAMILY, storageQueueName,
                    CassandraConstants.MESSAGE_COUNTERS_RAW_NAME);
            CQLDataAccessHelper.decrementCounter(storageQueueName, CassandraConstants.MESSAGE_COUNTERS_COLUMN_FAMILY,
                    CassandraConstants.MESSAGE_COUNTERS_RAW_NAME, CassandraConstants.KEYSPACE, messageCountOfQueue);

        } catch (CassandraDataAccessException e) {
            log.error("Error while resetting message counter for queue : " + storageQueueName,e);
            throw new AndesException("Error while resetting message counter for queue : " + storageQueueName, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Long> getMessageIDsAddressedToQueue(String storageQueueName) throws AndesException {

        List<Long> messageIDs = new ArrayList<Long>();

        Long lastProcessedID = 0l;
        // In case paginated data fetching is slow, this can be set to Integer.MAX.
        // This is set to paginate so that a big data read wont cause continuous timeouts.
        Integer pageSize = CQLDataAccessHelper.STANDARD_PAGE_SIZE;

        Boolean allRecordsRetrieved = false;

        while (!allRecordsRetrieved) {
            try {
                List<Long> currentPage = CQLDataAccessHelper.getColumnDataFromColumnFamily
                        (storageQueueName, CassandraConstants.META_DATA_COLUMN_FAMILY,
                                CQLDataAccessHelper.MSG_KEY, CassandraConstants.KEYSPACE,
                                lastProcessedID, pageSize);

                if (currentPage.size() == 0) {
                    // this means that there are no more messages to be retrieved for this queue
                    allRecordsRetrieved = true;
                } else {
                    messageIDs.addAll(currentPage);
                    lastProcessedID = currentPage.get(currentPage.size() - 1);

                    if (currentPage.size() < pageSize) {
                        // again means there are no more message IDs to be retrieved
                        allRecordsRetrieved = true;
                    }
                }

            } catch (CassandraDataAccessException e) {
                // we also need to escape loop in case of an exception and communicate the error
                log.error("Error while getting message IDs for queue : " + storageQueueName,e);
                throw new AndesException("Error while getting message IDs for queue : " +
                        storageQueueName, e);
            }
        }

        return messageIDs;
    }

    /**
     * delete message from expiration column family
     *
     * @param messagesToRemove messages to remove
     * @throws AndesException
     */
    @Override
    public void deleteMessagesFromExpiryQueue(List<Long> messagesToRemove) throws AndesException {
        try {
            List<Statement> statements = new ArrayList<Statement>();
            for (Long messageId : messagesToRemove) {

                CQLQueryBuilder.CqlDelete cqlDelete = new CQLQueryBuilder.CqlDelete(
                        CassandraConstants.KEYSPACE,
                        CassandraConstants.MESSAGES_FOR_EXPIRY_COLUMN_FAMILY);

                cqlDelete.addCondition(CQLDataAccessHelper.MESSAGE_ID, messageId,
                        CassandraHelper.WHERE_OPERATORS.EQ);

                Delete delete = CQLQueryBuilder.buildSingleDelete(cqlDelete);

                statements.add(delete);
            }

            // There is another way. Can delete using a single where clause on the timestamp.
            // (delete all messages with expiration time < current timestamp)
            // But that could result in inconsistent data overall.
            // If a message has been added to the queue between expiry checker invocation and this method, it could be lost.
            // Therefore, the above approach to delete.
            GenericCQLDAO.batchExecuteWrite(CassandraConstants.KEYSPACE,
                    statements.toArray(new Statement[statements.size()]));
        } catch (CassandraDataAccessException e) {
            String error = "Error while deleting messages";
            log.error(error, e);
            throw new AndesException(error,e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int deleteAllMessageMetadataFromDLC(String storageQueueName, String DLCQueueName) throws
            AndesException {

        List<Statement> statements = new ArrayList<Statement>();

        try {

            Long lastProcessedID = 0l;
            // In case paginated data fetching is slow for some reason,
            // this can be set to Integer.MAX..
            // This is set to paginate so that a big data read wont cause continuous timeouts.
            Integer pageSize = CQLDataAccessHelper.STANDARD_PAGE_SIZE;

            Boolean allRecordsRetrieved = false;

            while (!allRecordsRetrieved) {

                List<AndesMessageMetadata> metadataList = CQLDataAccessHelper
                        .getMessagesFromQueue(DLCQueueName,
                        CassandraConstants.META_DATA_COLUMN_FAMILY,
                        CassandraConstants.KEYSPACE, lastProcessedID,
                        Long.MAX_VALUE, pageSize, true, true);

                if (metadataList.size() == 0) {
                    allRecordsRetrieved = true; // this means that there are no more messages
                    // to be retrieved for this queue
                } else {
                    for (AndesMessageMetadata amm : metadataList) {
                        if (amm.getDestination().equals(storageQueueName)) {

                            Delete delete = CQLDataAccessHelper
                                    .deleteLongColumnFromRaw(CassandraConstants.KEYSPACE,
                                            CassandraConstants.META_DATA_COLUMN_FAMILY,
                                            DLCQueueName, amm.getMessageID(), false);
                            statements.add(delete);
                        }
                    }

                    lastProcessedID = metadataList.get(metadataList.size() - 1).getMessageID();

                    if (metadataList.size() < pageSize) {
                        // again means there are no more metadata to be retrieved
                        allRecordsRetrieved = true;
                    }
                }

            }

            // Execute Batch Delete
            GenericCQLDAO.batchExecuteWrite(CassandraConstants.KEYSPACE,
                    statements.toArray(new Statement[statements.size()]));

        } catch (CassandraDataAccessException e) {
            log.error("Error while getting messages in DLC for queue : " + storageQueueName,e);
            throw new AndesException("Error while getting messages in DLC for queue : " + storageQueueName, e);
        }

        return statements.size();
    }


}
