/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.store.cassandra;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.AndesRemovableMetadata;
import org.wso2.andes.kernel.DurableStoreConnection;
import org.wso2.andes.kernel.MessageStore;
import org.wso2.andes.matrics.DataAccessMatrixManager;
import org.wso2.andes.matrics.MatrixConstants;
import org.wso2.andes.server.stats.PerformanceCounter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.wso2.carbon.metrics.manager.Timer.*;

/**
 * This is the implementation of MessageStore that deals with Cassandra no SQL DB.
 * It uses Hector for making queries.
 */
public class HectorBasedMessageStoreImpl implements MessageStore {

    private static Log log = LogFactory.getLog(HectorBasedMessageStoreImpl.class);

    /**
     * Message id prefix. We are using a String for message content related message ids in Hector
     */
    public static final String MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX = "mid";
    /**
     * Keyspace object
     */
    private Keyspace keyspace;

    /**
     * HectorConnection object which tracks the Cassandra connection
     */
    private HectorConnection hectorConnection;

    /**
     * Context store reference to get context store level functionality
     * Mainly we use this to have the message counts 
     */
    private AndesContextStore contextStore;

    /**
     * {@inheritDoc}
     */
    @Override
    public DurableStoreConnection initializeMessageStore(AndesContextStore contextStore,
            ConfigurationProperties connectionProperties) throws AndesException {
        // create connection object
        //todo remove this if after testing
        if (hectorConnection == null) {
            hectorConnection = new HectorConnection();
        }
        hectorConnection.initialize(connectionProperties);

        this.contextStore = contextStore;
        // get cassandra cluster and create column families
        initializeCassandraMessageStore(hectorConnection);
        return hectorConnection;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeMessagePart(List<AndesMessagePart> partList) throws AndesException {
        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.ADD_MESSAGE_PART, this).start();
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace,
                    CassandraConstants.stringSerializer);

            for (AndesMessagePart part : partList) {
                final String rowKey = MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX
                        + part.getMessageID();

                HectorDataAccessHelper.addMessageToQueue(CassandraConstants
                                .MESSAGE_CONTENT_COLUMN_FAMILY,
                        rowKey, part.getOffSet(),
                        part.getData(), mutator, false);
            }

            //batch execute
            mutator.execute();

        } catch (CassandraDataAccessException e) {
            //TODO handle Cassandra failures
            //When a error happened, we should remember that and stop accepting messages
            throw new AndesException("Error while adding the message part to the store", e);
        } finally {
            context.stop();
        }
    }

    /**
     * {@inheritDoc}
     * @param messageIdList
     */
    @Override
    public void deleteMessageParts(Collection<Long> messageIdList) throws AndesException {
        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.DELETE_MESSAGE_PART, this).start();
        try {

            Mutator<String> mutator = HFactory.createMutator(keyspace,
                    CassandraConstants.stringSerializer);

            List<String> rows2Remove = new ArrayList<String>();
            for (long messageId : messageIdList) {
                rows2Remove.add(MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX +
                        messageId);
            }

            //remove content
            if (!rows2Remove.isEmpty()) {
                HectorDataAccessHelper.deleteIntegerRowListFromColumnFamily(
                        CassandraConstants.MESSAGE_CONTENT_COLUMN_FAMILY, rows2Remove,
                        mutator, false);
            }

            //batch execute
            mutator.execute();
        } catch (CassandraDataAccessException e) {
            throw new AndesException(e);
        } finally {
            context.stop();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesMessagePart getContent(long messageId, int offsetValue) throws AndesException {
        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.GET_CONTENT, this).start();
        try {
            String rowKey = MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX + messageId;
            return HectorDataAccessHelper.getMessageContent(rowKey,
                    CassandraConstants.MESSAGE_CONTENT_COLUMN_FAMILY, keyspace, messageId,
                    offsetValue);

        } catch (Exception e) {
            throw new AndesException(
                    "Error while reading content messageID=" + messageId + " offset=" +
                            offsetValue, e);
        } finally {
            context.stop();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<Long, List<AndesMessagePart>> getContent(List<Long> messageIdList) throws AndesException {
        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.GET_CONTENT, this).start();
        try {

            return HectorDataAccessHelper.getMessageContentBatch(
                    CassandraConstants.MESSAGE_CONTENT_COLUMN_FAMILY, keyspace, messageIdList);

        } catch (Exception e) {
            throw new AndesException(
                    "Error while reading content messageIDs", e);
        } finally {
            context.stop();
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetaData(List<AndesMessageMetadata> metadataList) throws AndesException {
        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.ADD_META_DATA_LIST, this).start();
        try {

            Mutator<String> mutator = HFactory.createMutator(keyspace,
                    CassandraConstants.stringSerializer);

            for (AndesMessageMetadata metadata : metadataList) {
                HectorDataAccessHelper.addMessageToQueue(
                        CassandraConstants.META_DATA_COLUMN_FAMILY,
                        metadata.getStorageQueueName(),
                        metadata.getMessageID(),
                        metadata.getMetadata(), mutator, false);
            }
            long start = System.currentTimeMillis();

            //batch execute
            mutator.execute();

            int latency = (int) (System.currentTimeMillis() - start);

            if (latency > 1000) {
                log.warn("Cassandra writing took " + latency + " millisecoonds for batch of " +
                        metadataList.size());
            }

            if(log.isDebugEnabled()) {
                PerformanceCounter.recordIncomingMessageWrittenToCassandraLatency(latency);
            }

        } catch (Exception e) {
            //TODO handle Cassandra failures
            //TODO may be we can write those message to a disk, or do something. Currently we
            // will just loose them
            throw new AndesException("Error while writing incoming messages to Cassandra", e);
        } finally {
            context.stop();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetaData(AndesMessageMetadata metadata) throws AndesException {
        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.ADD_META_DATA, this).start();
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace,
                    CassandraConstants.stringSerializer);

            HectorDataAccessHelper.addMessageToQueue(
                    CassandraConstants
                            .META_DATA_COLUMN_FAMILY,
                    metadata.getStorageQueueName(),
                    metadata.getMessageID(),
                    metadata.getMetadata(), mutator, true);

        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while writing incoming message to cassandra.", e);
        } finally {
            context.stop();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetaDataToQueue(String queueName, AndesMessageMetadata metadata)
            throws AndesException {
        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.ADD_META_DATA_TO_QUEUE, this).start();
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace,
                    CassandraConstants.stringSerializer);

            HectorDataAccessHelper.addMessageToQueue(CassandraConstants
                            .META_DATA_COLUMN_FAMILY,
                    queueName,
                    metadata.getMessageID(),
                    metadata.getMetadata(), mutator, true);

        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while writing incoming message to cassandra.", e);
        } finally {
            context.stop();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetadataToQueue(String queueName, List<AndesMessageMetadata> metadataList)
            throws AndesException {

        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.ADD_META_DATA_TO_QUEUE_LIST, this).start();
        try {

            Mutator<String> mutator = HFactory.createMutator(keyspace,
                    CassandraConstants.stringSerializer);

            for (AndesMessageMetadata metadata : metadataList) {
                HectorDataAccessHelper.addMessageToQueue(
                        CassandraConstants
                                .META_DATA_COLUMN_FAMILY,
                        queueName,
                        metadata.getMessageID(),
                        metadata.getMetadata(), mutator, false);
            }

            //batch execute
            mutator.execute();
        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while writing incoming message to cassandra.", e);
        } finally {
            context.stop();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void moveMetaDataToQueue(long messageId, String currentQueueName,
                                    String targetQueueName) throws AndesException {
        List<AndesMessageMetadata> messageMetadataList = getMetaDataList(currentQueueName,
                messageId, messageId);

        if (messageMetadataList == null || messageMetadataList.size() == 0) {
            throw new AndesException(
                    "Message MetaData not found to move the message to Dead Letter Channel");
        }
        ArrayList<AndesRemovableMetadata> removableMetaDataList = new
                ArrayList<AndesRemovableMetadata>();
        removableMetaDataList.add(new AndesRemovableMetadata(messageId, currentQueueName, currentQueueName));

        addMetaDataToQueue(targetQueueName, messageMetadataList.get(0));
        deleteMessageMetadataFromQueue(currentQueueName, removableMetaDataList);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateMetaDataInformation(String currentQueueName, List<AndesMessageMetadata>
            metadataList) throws AndesException {

        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.UPDATE_META_DATA_INFORMATION, this).start();
        try {
            Mutator<String> insertMutator = HFactory.createMutator(keyspace,
                    CassandraConstants.stringSerializer);
            Mutator<String> deleteMutator = HFactory.createMutator(keyspace,
                    CassandraConstants.stringSerializer);

            // Step 1 - Insert the new meta data
            for (AndesMessageMetadata metadata : metadataList) {
                HectorDataAccessHelper.addMessageToQueue(CassandraConstants
                        .META_DATA_COLUMN_FAMILY,
                        metadata.getStorageQueueName(),
                        metadata.getMessageID(),
                        metadata.getMetadata(),
                        insertMutator, false);
            }

            long start = System.currentTimeMillis();

            //batch execute
            insertMutator.execute();

            if(log.isDebugEnabled()) {
                PerformanceCounter.recordIncomingMessageWrittenToCassandraLatency(
                        (int) (System.currentTimeMillis() -
                                start));
            }

            // Step 2 - Delete the old meta data when inserting new meta is complete to avoid
            // losing messages
            for (AndesMessageMetadata metadata : metadataList) {
                HectorDataAccessHelper
                        .deleteLongColumnFromRaw(CassandraConstants.META_DATA_COLUMN_FAMILY,
                                currentQueueName, metadata.getMessageID(), deleteMutator, false);
            }

            //batch execute
            deleteMutator.execute();

        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while updating message meta data", e);
        } finally {
            context.stop();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesMessageMetadata getMetaData(long messageId) throws AndesException {
        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.GET_META_DATA, this).start();
        try {

            byte[] value = HectorDataAccessHelper
                    .getMessageMetaDataOfMessage(CassandraConstants.META_DATA_COLUMN_FAMILY,
                            keyspace, messageId);
            return new AndesMessageMetadata(messageId, value, true);

        } catch (Exception e) {
            throw new AndesException("Error while getting meta data for messageID " + messageId,
                    e);
        } finally {
            context.stop();
        }
    }

    /**
     * {@inheritDoc}
     * Hector range query may return more records than limit size of STANDARD_PAGE_SIZE because slot hasn't a hard limit.
     * In such case we need to get all metadata between firstMsgId and lastMsgID
     */
    @Override
    public List<AndesMessageMetadata> getMetaDataList(String queueName, long firstMsgId,
                                                      long lastMsgID) throws AndesException {

        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.GET_META_DATA_LIST, this).start();
        try {
            //Contains all metadata between firstMsgId and lastMsgID
            List<AndesMessageMetadata> allMetadataList = new ArrayList<AndesMessageMetadata>();
            //Get first set of metadata list between firstMsgId and lastMsgID
            List<AndesMessageMetadata> metadataList = HectorDataAccessHelper.getMessagesFromQueue
                    (queueName, CassandraConstants.META_DATA_COLUMN_FAMILY, keyspace, firstMsgId,
                            lastMsgID, HectorDataAccessHelper.STANDARD_PAGE_SIZE, true);
            allMetadataList.addAll(metadataList);
            int metadataCount = metadataList.size();
            //Check metadata list size equal to greater than to STANDARD_PAGE_SIZE to retry again
            while (metadataCount >= HectorDataAccessHelper.STANDARD_PAGE_SIZE) {
                //Get nextFirstMsgId
                long nextFirstMsgId = metadataList.get(metadataCount - 1).getMessageID();
                //Break retrying if all messages received
                if(nextFirstMsgId == lastMsgID) {
                    break;
                }
                metadataList = HectorDataAccessHelper.getMessagesFromQueue
                        (queueName, CassandraConstants.META_DATA_COLUMN_FAMILY, keyspace, nextFirstMsgId,
                                lastMsgID, HectorDataAccessHelper.STANDARD_PAGE_SIZE, true);
                allMetadataList.addAll(metadataList);
                metadataCount = metadataList.size();
            }
            return allMetadataList;
        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while reading meta data list for message IDs " +
                    "from " + firstMsgId + " to " + lastMsgID, e);
        } finally {
            context.stop();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(String queueName,
                                                                       long firstMsgId, int count)
            throws AndesException {

        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.GET_NEXT_MESSAGE_METADATA_FROM_QUEUE, this).start();
        try {
            return HectorDataAccessHelper
                    .getMessagesFromQueue(queueName,
                            CassandraConstants.META_DATA_COLUMN_FAMILY,
                            keyspace, firstMsgId, Long.MAX_VALUE,
                            Integer.MAX_VALUE, true);

        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while reading meta data list for message IDs " +
                    "from " + firstMsgId + " to " + firstMsgId, e);
        } finally {
            context.stop();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessageMetadataFromQueue(String queueName, List<AndesRemovableMetadata>
            messagesToRemove) throws AndesException {

        Context context = DataAccessMatrixManager.addAndGetTimer(MatrixConstants.DELETE_MESSAGE_META_DATA_FROM_QUEUE, this).start();
        try {
            if (log.isTraceEnabled()) {
                StringBuilder messageIDsString = new StringBuilder();
                for (AndesRemovableMetadata metadata : messagesToRemove) {
                    messageIDsString.append(metadata.getMessageID()).append(" , ");
                }
                log.trace(messagesToRemove.size() + " messages removed : " + messageIDsString);
            }
            Mutator<String> mutator = HFactory.createMutator(keyspace,
                    CassandraConstants.stringSerializer);

            for (AndesRemovableMetadata message : messagesToRemove) {
                HectorDataAccessHelper
                        .deleteLongColumnFromRaw(
                                CassandraConstants.META_DATA_COLUMN_FAMILY,
                                queueName, message.getMessageID(), mutator, false);
            }

            //batch execute
            mutator.execute();

        } catch (Exception e) {
            throw new AndesException("Error while deleting messages", e);
        } finally {
            context.stop();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesRemovableMetadata> getExpiredMessages(int limit) throws AndesException {
        //todo: implement
        return new ArrayList<AndesRemovableMetadata>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessagesFromExpiryQueue(List<Long> messagesToRemove) throws AndesException {
        //todo:implement
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMessageToExpiryQueue(Long messageId, Long expirationTime,
                                        boolean isMessageForTopic, String destination) throws AndesException {

        //TODO implement
    }

    @Override
    public void deleteAllMessageMetadata(String storageQueueName) throws AndesException {

        Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());

        mutator.addDeletion(storageQueueName,CassandraConstants.META_DATA_COLUMN_FAMILY);

        mutator.execute();
    }

    @Override
    /**
     * {@inheritDoc}
     */
    public int deleteAllMessageMetadataFromDLC(String storageQueueName, String DLCQueueName) throws AndesException {

        int messageCountInDLC;

        try {

            Long lastProcessedID = 0l;
            // In case paginated data fetching is slow for some reason,
            // this can be set to Integer.MAX..
            // This is set to paginate so that a big data read wont cause continuous timeouts.
            Integer pageSize = HectorDataAccessHelper.STANDARD_PAGE_SIZE;

            Boolean allRecordsRetrieved = false;

            Mutator<String> mutator = HFactory.createMutator(keyspace,StringSerializer.get());

            while (!allRecordsRetrieved) {

                List<AndesMessageMetadata> metadataList = HectorDataAccessHelper
                        .getMessagesFromQueue(DLCQueueName,
                                CassandraConstants.META_DATA_COLUMN_FAMILY,
                                keyspace, lastProcessedID,
                                Long.MAX_VALUE, pageSize, true);

                if (metadataList.size() == 0) {
                    allRecordsRetrieved = true; // this means that there are no more messages
                    // to be retrieved for this queue
                } else {
                    for (AndesMessageMetadata amm : metadataList) {
                        if (amm.getDestination().equals(storageQueueName)) {
                            mutator.addDeletion(DLCQueueName, CassandraConstants.META_DATA_COLUMN_FAMILY, amm.getMessageID());
                        }
                    }

                    lastProcessedID = metadataList.get(metadataList.size() - 1).getMessageID();

                    if (metadataList.size() < pageSize) {
                        // again means there are no more metadata to be retrieved
                        allRecordsRetrieved = true;
                    }
                }

            }

            messageCountInDLC = mutator.getPendingMutationCount();

            // Execute Batch Delete
            mutator.execute();

        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while getting messages in DLC for queue : " + storageQueueName, e);
        }

        return messageCountInDLC;
    }

    /**
     * {@inheritDoc}
     * @param storageQueueName name of the storage queue.
     * @return List<Long> message ID list that is contained within given storage queues.
     * @throws AndesException
     */
    @Override
    public List<Long> getMessageIDsAddressedToQueue(String storageQueueName,Long startMessageID) throws AndesException {

        List<Long> messageIDs = new ArrayList<Long>();

        Long lastProcessedID = startMessageID;
        // In case paginated data fetching is slow, this can be set to Integer.MAX.
        // This is set to paginate so that a big data read wont cause continuous timeouts.
        Integer pageSize = HectorDataAccessHelper.STANDARD_PAGE_SIZE;

        Boolean allRecordsRetrieved = false;

        while (!allRecordsRetrieved) {
            try {
                List<Long> currentPage = HectorDataAccessHelper.getNumericColumnKeysOfRow
                        (keyspace, CassandraConstants.META_DATA_COLUMN_FAMILY, storageQueueName, pageSize,
                                lastProcessedID);

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
                throw new AndesException("Error while getting message IDs for queue : " + storageQueueName, e);
            }
        }

        return messageIDs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addQueue(String storageQueueName) throws AndesException {
        contextStore.addMessageCounterForQueue(storageQueueName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getMessageCountForQueue(String storageQueueName) throws AndesException {
        return contextStore.getMessageCountForQueue(storageQueueName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void resetMessageCounterForQueue(String storageQueueName) throws AndesException {
        contextStore.resetMessageCounterForQueue(storageQueueName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeQueue(String storageQueueName) throws AndesException {
        contextStore.removeMessageCounterForQueue(storageQueueName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void incrementMessageCountForQueue(String storageQueueName, long incrementBy) throws AndesException {
        contextStore.incrementMessageCountForQueue(storageQueueName, incrementBy);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void decrementMessageCountForQueue(String storageQueueName, long decrementBy) throws AndesException {
        contextStore.decrementMessageCountForQueue(storageQueueName, decrementBy);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        hectorConnection.close();
    }

    /**
     * Initialize HectorBasedMessageStoreImpl
     *
     * @param hectorConnection hector based connection to Cassandra
     * @throws AndesException
     */
    private void initializeCassandraMessageStore(HectorConnection hectorConnection)
            throws AndesException {
        try {
            keyspace = hectorConnection.getKeySpace();
            createColumnFamilies(hectorConnection, hectorConnection.getCluster(), keyspace.getKeyspaceName());
        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while initializing cassandra message store", e);
        }
    }

    /**
     * Create a cassandra column families for andes usage
     *
     * @throws CassandraDataAccessException
     */
    private void createColumnFamilies(HectorConnection connection,
                                      Cluster cluster,
                                      String keyspace) throws CassandraDataAccessException {

        int gcGraceSeconds = connection.getGcGraceSeconds();
        HectorDataAccessHelper.createColumnFamily(CassandraConstants.MESSAGE_CONTENT_COLUMN_FAMILY,
                keyspace, cluster,
                CassandraConstants.INTEGER_TYPE,
                gcGraceSeconds);
        HectorDataAccessHelper.createColumnFamily(CassandraConstants.META_DATA_COLUMN_FAMILY,
                keyspace, cluster,
                CassandraConstants.LONG_TYPE,
                gcGraceSeconds);
        HectorDataAccessHelper
                .createCounterColumnFamily(CassandraConstants.MESSAGE_COUNTERS_COLUMN_FAMILY,
                        keyspace, cluster,
                        gcGraceSeconds);
        HectorDataAccessHelper.createMessageExpiryColumnFamily(
                CassandraConstants.MESSAGES_FOR_EXPIRY_COLUMN_FAMILY, keyspace,
                cluster, CassandraConstants.UTF8_TYPE, gcGraceSeconds);
    }


    /**
     * {@inheritDoc}
     */
    public void storeRetainedMessages(List<AndesMessage> retainList) throws AndesException {
        // TODO: implement this method
        throw new org.apache.commons.lang.NotImplementedException("Hector base message store methods for" +
                                                                  " retain feature will be implemented " +
                                                                  "in next iteration");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getAllRetainedTopics() throws AndesException {
        // TODO: implement this method
        throw new org.apache.commons.lang.NotImplementedException("Hector base message store methods for" +
                                                                  " retain feature will be implemented " +
                                                                  "in next iteration");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesMessageMetadata getRetainedMetaData(String destination) throws AndesException {
        // TODO: implement this method
        throw new org.apache.commons.lang.NotImplementedException("Hector base message store methods for" +
                                                                  " retain feature will be implemented " +
                                                                  "in next iteration");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<Integer, AndesMessagePart> getRetainedContentParts(long messageID) throws AndesException {
        // TODO: implement this method
        throw new org.apache.commons.lang.NotImplementedException("Hector base message store methods for" +
                                                                  " retain feature will be implemented " +
                                                                  "in next iteration");
    }


}
