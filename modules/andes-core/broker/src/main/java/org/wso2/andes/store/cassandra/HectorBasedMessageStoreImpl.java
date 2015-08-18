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
import org.wso2.andes.metrics.MetricsConstants;
import org.wso2.andes.store.AndesStoreUnavailableException;
import org.wso2.carbon.metrics.manager.Level;
import org.wso2.carbon.metrics.manager.MetricManager;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.wso2.carbon.metrics.manager.Timer.Context;

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
     * Encapsulates functionality required to test connectivity to cluster
     */
    private HectorUtils hectorUtils;
    
    /**
     * The COMMA is used to append message IDs to one another when performing column operations
     */
    private static final String COMMA = " , ";

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
        
        this.hectorUtils = new HectorUtils();
        return hectorConnection;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeMessagePart(List<AndesMessagePart> partList) throws AndesException {
        Context context = MetricManager.timer(Level.DEBUG, MetricsConstants.ADD_MESSAGE_PART).start();
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace,
                    HectorConstants.stringSerializer);

            for (AndesMessagePart part : partList) {
                addMessagePartToBatch(part, mutator);
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
     * Add message part to the batch. By calling {@link me.prettyprint.hector.api.mutation.Mutator#execute()}
     * added messages can be persisted to DB as a batch.
     * @param part {@link org.wso2.andes.kernel.AndesMessagePart}
     * @param mutator {@link me.prettyprint.hector.api.mutation.Mutator}
     * @throws CassandraDataAccessException
     */
    private void addMessagePartToBatch(AndesMessagePart part, Mutator<String> mutator)
            throws CassandraDataAccessException, AndesStoreUnavailableException {

        final String rowKey = MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX
                + part.getMessageID();

        HectorDataAccessHelper.addMessageToQueue(HectorConstants
                        .MESSAGE_CONTENT_COLUMN_FAMILY,
                rowKey, part.getOffSet(),
                part.getData(), mutator, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesMessagePart getContent(long messageId, int offsetValue) throws AndesException {
        Context context = MetricManager.timer(Level.DEBUG, MetricsConstants.GET_CONTENT).start();
        try {
            String rowKey = MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX + messageId;
            return HectorDataAccessHelper.getMessageContent(rowKey,
                    HectorConstants.MESSAGE_CONTENT_COLUMN_FAMILY, keyspace, messageId,
                    offsetValue);

        } catch (CassandraDataAccessException e) {
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
        Context context = MetricManager.timer(Level.DEBUG, MetricsConstants.GET_CONTENT).start();
        try {

            return HectorDataAccessHelper.getMessageContentBatch(
                    HectorConstants.MESSAGE_CONTENT_COLUMN_FAMILY, keyspace, messageIdList);

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
    public void addMetadata(List<AndesMessageMetadata> metadataList) throws AndesException {
        Context context = MetricManager.timer(Level.DEBUG, MetricsConstants.ADD_META_DATA_LIST).start();
        try {

            Mutator<String> mutator = HFactory.createMutator(keyspace,
                    HectorConstants.stringSerializer);

            for (AndesMessageMetadata metadata : metadataList) {
                addMetadataToBatch(metadata, metadata.getStorageQueueName(), mutator);
            }
            long start = System.currentTimeMillis();

            //batch execute
            mutator.execute();

            int latency = (int) (System.currentTimeMillis() - start);

            if (latency > 1000) {
                log.warn("Cassandra writing took " + latency + " millisecoonds for batch of " + metadataList.size());
            }

        } catch (CassandraDataAccessException e) {
        
            throw new AndesException("Error while writing incoming messages to Cassandra", e);
        } finally {
            context.stop();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetadata(AndesMessageMetadata metadata) throws AndesException {
        Context context = MetricManager.timer(Level.DEBUG, MetricsConstants.ADD_META_DATA).start();
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace,
                    HectorConstants.stringSerializer);

            addMetadataToBatch(metadata, metadata.getStorageQueueName(), mutator);
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
    public void storeMessages(List<AndesMessage> messageList) throws AndesException {
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());

            for (AndesMessage message : messageList) {
                addMetadataToBatch(message.getMetadata(), message.getMetadata().getStorageQueueName(), mutator);
                for (AndesMessagePart messagePart : message.getContentChunkList()) {
                    addMessagePartToBatch(messagePart, mutator);
                }
            }
            mutator.execute();
        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error occurred while storing messages", e);
        }
    }

    /**
     * Add message metadata to a batch. By calling {@link me.prettyprint.hector.api.mutation.Mutator#execute()}
     * added messages can be persisted to DB as a batch.
     * @param metadata {@link org.wso2.andes.kernel.AndesMessageMetadata}
     * @param queueName name of the queue
     * @param mutator {@link me.prettyprint.hector.api.mutation.Mutator}
     * @throws CassandraDataAccessException
     */
    private void addMetadataToBatch(AndesMessageMetadata metadata, String queueName, Mutator<String> mutator)
            throws CassandraDataAccessException, AndesStoreUnavailableException {
        HectorDataAccessHelper.addMessageToQueue(
                HectorConstants
                        .META_DATA_COLUMN_FAMILY,
                metadata.getStorageQueueName(),
                metadata.getMessageID(),
                metadata.getMetadata(), mutator, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetadataToQueue(String queueName, AndesMessageMetadata metadata)
            throws AndesException {
        Context context = MetricManager.timer(Level.DEBUG, MetricsConstants.ADD_META_DATA_TO_QUEUE).start();
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace,
                    HectorConstants.stringSerializer);

            addMetadataToBatch(metadata, queueName, mutator);
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
    public void addMetadataToQueue(String queueName, List<AndesMessageMetadata> metadataList)
            throws AndesException {

        Context context = MetricManager.timer(Level.DEBUG, MetricsConstants.ADD_META_DATA_TO_QUEUE_LIST).start();
        try {

            Mutator<String> mutator = HFactory.createMutator(keyspace,
                    HectorConstants.stringSerializer);

            for (AndesMessageMetadata metadata : metadataList) {
                addMetadataToBatch(metadata, queueName, mutator);
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
    public void moveMetadataToQueue(long messageId, String currentQueueName,
                                    String targetQueueName) throws AndesException {
        List<AndesMessageMetadata> messageMetadataList = getMetadataList(currentQueueName,
                messageId, messageId);

        if (messageMetadataList == null || messageMetadataList.size() == 0) {
            throw new AndesException(
                    "Message MetaData not found to move the message to Dead Letter Channel");
        }
        ArrayList<Long> removableMetaDataList = new ArrayList<>();
        removableMetaDataList.add(messageId);

        addMetadataToQueue(targetQueueName, messageMetadataList.get(0));
        deleteMessageMetadataFromQueue(currentQueueName, removableMetaDataList);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void moveMetadataToDLC(long messageId, String dlcQueueName) throws AndesException {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void moveMetadataToDLC(List<Long> messageIds, String dlcQueueName) throws AndesException {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateMetadataInformation(String currentQueueName, List<AndesMessageMetadata>
            metadataList) throws AndesException {

        Context context = MetricManager.timer(Level.DEBUG, MetricsConstants.UPDATE_META_DATA_INFORMATION).start();
        try {
            Mutator<String> insertMutator = HFactory.createMutator(keyspace,
                    HectorConstants.stringSerializer);
            Mutator<String> deleteMutator = HFactory.createMutator(keyspace,
                    HectorConstants.stringSerializer);

            // Step 1 - Insert the new meta data
            for (AndesMessageMetadata metadata : metadataList) {
                addMetadataToBatch(metadata, metadata.getStorageQueueName(), insertMutator);
            }

            long start = System.currentTimeMillis();

            //batch execute
            insertMutator.execute();

            // Step 2 - Delete the old meta data when inserting new meta is complete to avoid
            // losing messages
            for (AndesMessageMetadata metadata : metadataList) {
                HectorDataAccessHelper
                        .deleteLongColumnFromRaw(HectorConstants.META_DATA_COLUMN_FAMILY,
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
    public AndesMessageMetadata getMetadata(long messageId) throws AndesException {
        Context context = MetricManager.timer(Level.DEBUG, MetricsConstants.GET_META_DATA).start();
        try {

            byte[] value = HectorDataAccessHelper
                    .getMessageMetaDataOfMessage(HectorConstants.META_DATA_COLUMN_FAMILY,
                            keyspace, messageId);
            return new AndesMessageMetadata(messageId, value, true);

        } catch (CassandraDataAccessException e) {
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
    public List<AndesMessageMetadata> getMetadataList(String queueName, long firstMsgId,
                                                      long lastMsgID) throws AndesException {

        Context context = MetricManager.timer(Level.DEBUG, MetricsConstants.GET_META_DATA_LIST).start();
        try {
            //Contains all metadata between firstMsgId and lastMsgID
            List<AndesMessageMetadata> allMetadataList = new ArrayList<AndesMessageMetadata>();
            //Get first set of metadata list between firstMsgId and lastMsgID
            List<AndesMessageMetadata> metadataList = HectorDataAccessHelper.getMessagesFromQueue
                    (queueName, HectorConstants.META_DATA_COLUMN_FAMILY, keyspace, firstMsgId,
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
                        (queueName, HectorConstants.META_DATA_COLUMN_FAMILY, keyspace, nextFirstMsgId,
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

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        List<AndesMessageMetadata> messageMetadataList = new ArrayList<AndesMessageMetadata>();
        long lastMsgId;
        int listSize;

        Context context = MetricManager.timer(Level.DEBUG, MetricsConstants.GET_NEXT_MESSAGE_METADATA_FROM_QUEUE).start();

        if (firstMsgId == 0) {
            firstMsgId = ServerStartupRecoveryUtils.getStartMessageIdForWarmStartup();
        }
        long messageIdDifference = ServerStartupRecoveryUtils.getMessageDifferenceForWarmStartup();
        lastMsgId = firstMsgId + messageIdDifference;
        long lastRecoveryMessageId = ServerStartupRecoveryUtils.getMessageIdToCompleteRecovery();
        try {
            List<AndesMessageMetadata> messagesFromQueue = HectorDataAccessHelper
                    .getMessagesFromQueue(queueName,
                            HectorConstants.META_DATA_COLUMN_FAMILY,
                            keyspace, firstMsgId, lastMsgId,
                            count, true);
            listSize = messagesFromQueue.size();
            messageMetadataList.addAll(messagesFromQueue);
            if (listSize < count) {
                readingCassandraInfoLog(scheduledExecutorService);
                while (lastMsgId <= lastRecoveryMessageId) {
                    long nextMsgId = lastMsgId + 1;
                    lastMsgId = nextMsgId + messageIdDifference;
                    messagesFromQueue = HectorDataAccessHelper
                            .getMessagesFromQueue(queueName,
                                    HectorConstants.META_DATA_COLUMN_FAMILY,
                                    keyspace, nextMsgId, lastMsgId,
                                    count, true);
                    listSize = listSize + messagesFromQueue.size();
                    messageMetadataList.addAll(messagesFromQueue);
                    if (listSize >= count) {
                        messageMetadataList = messageMetadataList.subList(0, count);
                        break;
                    }
                }
            }

        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while reading meta data list for message IDs " +
                    "from " + firstMsgId + " to " + firstMsgId, e);
        } finally {
            context.stop();
            scheduledExecutorService.shutdownNow();
        }
        return messageMetadataList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataForQueueFromDLC(String storageQueueName,
                                                                             String dlcQueueName, long firstMsgId,
                                                                             int count) throws AndesException {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataFromDLC(String dlcQueueName, long firstMsgId, int count)
            throws AndesException {
        throw new NotImplementedException();
    }

    /**
     * INFO log print to inform user while reading tombstone
     *
     * @param scheduledExecutorService ScheduledExecutorService to schedule printing logs
     */
    private void readingCassandraInfoLog(ScheduledExecutorService scheduledExecutorService) {
        long printDelay = 30L;
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                log.info("Reading data from cassandra.");
            }
        }, 0, printDelay, TimeUnit.SECONDS);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessageMetadataFromQueue(String queueName, List<Long>
            messagesToRemove) throws AndesException {

        Context context = MetricManager.timer(Level.DEBUG, MetricsConstants.DELETE_MESSAGE_META_DATA_FROM_QUEUE).start();
        try {
            if (log.isTraceEnabled()) {
                StringBuilder messageIDsString = new StringBuilder();
                for (Long metadata : messagesToRemove) {
                    messageIDsString.append(metadata.longValue()).append(COMMA);
                }
                log.trace(messagesToRemove.size() + " messages removed : " + messageIDsString);
            }
            Mutator<String> mutator = HFactory.createMutator(keyspace,
                    HectorConstants.stringSerializer);

            for (Long message : messagesToRemove) {
                HectorDataAccessHelper
                        .deleteLongColumnFromRaw(
                                HectorConstants.META_DATA_COLUMN_FAMILY,
                                queueName, message.longValue(), mutator, false);
            }

            //batch execute
            mutator.execute();

        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while deleting messages from queue " + queueName, e);
        } finally {
            context.stop();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessages(final String storageQueueName,
                               List<Long> messagesToRemove, boolean deleteAllMetaData)
            throws AndesException {
        Context context = MetricManager.timer(Level.INFO, MetricsConstants.DELETE_MESSAGE_META_DATA_AND_CONTENT)
                .start();
        try {
            if (log.isTraceEnabled()) {
                StringBuilder messageIDsString = new StringBuilder();
                for (Long messageID : messagesToRemove) {
                    messageIDsString.append(messageID.longValue()).append(COMMA);
                }
                log.trace(messagesToRemove.size() + " messages removed : " + messageIDsString);
            }
            Mutator<String> mutator = HFactory.createMutator(keyspace,
                    HectorConstants.stringSerializer);

            List<String> rows2Remove = new ArrayList<String>();
            //if all metadata is not be removed, add metadata and content of each message to delete
            //else, add content of each message and all metadata for the queue to delete
            if (!deleteAllMetaData) {
                for (Long messageID : messagesToRemove) {
                    HectorDataAccessHelper
                            .deleteLongColumnFromRaw(
                                    HectorConstants.META_DATA_COLUMN_FAMILY,
                                    storageQueueName, messageID.longValue(), mutator, false);
                    rows2Remove.add(MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX +
                            messageID.longValue());
                }
            } else {
                mutator.addDeletion(storageQueueName, HectorConstants.META_DATA_COLUMN_FAMILY);
                for (Long messageID : messagesToRemove) {
                    rows2Remove.add(MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX +
                            messageID.longValue());
                }
            }
            if (!rows2Remove.isEmpty()) {
                HectorDataAccessHelper.deleteIntegerRowListFromColumnFamily(
                        HectorConstants.MESSAGE_CONTENT_COLUMN_FAMILY, rows2Remove,
                        mutator, false);
            }

            //batch execute
            mutator.execute();

        } catch (CassandraDataAccessException e) {
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

    /**
     * {@inheritDoc}
     */
    @Override
    public int deleteAllMessageMetadata(String storageQueueName) throws AndesException {

        Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());

        mutator.addDeletion(storageQueueName,HectorConstants.META_DATA_COLUMN_FAMILY);

        mutator.execute();
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int clearDlcQueue(String dlcQueueName) throws AndesException {
        throw new NotImplementedException();
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

        long lastProcessedID = startMessageID;
        // In case paginated data fetching is slow, this can be set to Integer.MAX.
        // This is set to paginate so that a big data read wont cause continuous timeouts.
        int pageSize = HectorDataAccessHelper.STANDARD_PAGE_SIZE;

        boolean allRecordsRetrieved = false;

        while (!allRecordsRetrieved) {
            try {
                List<Long> currentPage = HectorDataAccessHelper.getNumericColumnKeysOfRow
                        (keyspace, HectorConstants.META_DATA_COLUMN_FAMILY, storageQueueName, pageSize,
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
    public long getMessageCountForQueueInDLC(String storageQueueName, String dlcQueueName) throws AndesException {
        throw new NotImplementedException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getMessageCountForDLCQueue(String dlcQueueName) throws AndesException {
        throw new NotImplementedException();
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
    private void initializeCassandraMessageStore(HectorConnection hectorConnection) throws AndesException {
            keyspace = hectorConnection.getKeySpace();
            createColumnFamilies(hectorConnection, hectorConnection.getCluster(), keyspace.getKeyspaceName());
    }

    /**
     * Create a cassandra column families for andes usage
     *
     * @throws CassandraDataAccessException
     */
    private void createColumnFamilies(HectorConnection connection,
                                      Cluster cluster,
                                      String keyspace) throws AndesException {

        try{
        int gcGraceSeconds = connection.getGcGraceSeconds();
        HectorDataAccessHelper.createColumnFamily(HectorConstants.MESSAGE_CONTENT_COLUMN_FAMILY,
                keyspace, cluster,
                HectorConstants.INTEGER_TYPE,
                gcGraceSeconds);
        HectorDataAccessHelper.createColumnFamily(HectorConstants.META_DATA_COLUMN_FAMILY,
                keyspace, cluster,
                HectorConstants.LONG_TYPE,
                gcGraceSeconds);
        HectorDataAccessHelper
                .createCounterColumnFamily(HectorConstants.MESSAGE_COUNTERS_COLUMN_FAMILY,
                        keyspace, cluster,
                        gcGraceSeconds);
        HectorDataAccessHelper.createMessageExpiryColumnFamily(
                HectorConstants.MESSAGES_FOR_EXPIRY_COLUMN_FAMILY, keyspace,
                cluster, HectorConstants.UTF8_TYPE, gcGraceSeconds);

    
        HectorDataAccessHelper.createColumnFamily(HectorConstants.MESSAGE_STORE_STATUS_COLUMN_FAMILY,
                                                  keyspace, cluster,
                                                  HectorConstants.UTF8_TYPE,
                                                  gcGraceSeconds);
        } catch (CassandraDataAccessException ex){
            throw new AndesException("Error while initializing cassandra message store", ex);
        }
    
    }
    
   /**
    * {@inheritDoc}
    */
    public boolean isOperational(String testString, long testTime){
        return hectorConnection.isReachable() &&
               hectorUtils.testInsert(hectorConnection, testString, testTime) &&
               hectorUtils.testRead(hectorConnection, testString, testTime) &&
               hectorUtils.testDelete(hectorConnection, testString, testTime);
    }


    /**
     * {@inheritDoc}
     */
    public void storeRetainedMessages(Map<String,AndesMessage> retainMap) throws AndesException {

        // TODO: implement this method
        log.warn("Hector base message store methods for retain feature will be implemented " +
                 "in next iteration");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getAllRetainedTopics() throws AndesException {

        // TODO: implement this method
        return Collections.emptyList();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesMessageMetadata getRetainedMetadata(String destination) throws AndesException {

        // TODO: implement this method
        AndesMessageMetadata messageMetadata = null;
        log.warn("Hector base message store methods for retain feature will be implemented " +
                 "in next iteration");
        return messageMetadata;

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<Integer, AndesMessagePart> getRetainedContentParts(long messageID) throws AndesException {

        // TODO: implement this method
        Map<Integer, AndesMessagePart> retainContentPartMap = Collections.emptyMap();
        log.warn("Hector base message store methods for retain feature will be implemented " +
                 "in next iteration");
        return retainContentPartMap;
    }

}
