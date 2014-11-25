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

package org.wso2.andes.store.cassandra.hector;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.kernel.*;
import org.wso2.andes.server.stats.PerformanceCounter;
import org.wso2.andes.server.store.util.CassandraDataAccessException;
import org.wso2.andes.server.store.util.HectorDataAccessHelper;
import org.wso2.andes.server.util.AlreadyProcessedMessageTracker;
import org.wso2.andes.server.util.AndesConstants;
import org.wso2.andes.store.cassandra.CassandraConstants;

import java.util.ArrayList;
import java.util.List;

/**
 * This is the implementation of MessageStore that deals with Cassandra no SQL DB.
 * It uses Hector for making queries.
 */
public class HectorBasedMessageStoreImpl implements MessageStore {

    private static Log log = LogFactory.getLog(HectorBasedMessageStoreImpl.class);


    private AlreadyProcessedMessageTracker alreadyMovedMessageTracker;


    /**
     * Keyspace object
     */
    private Keyspace keyspace;

    /**
     * HectorConnection object which tracks the Cassandra connection
     */
    private HectorConnection hectorConnection;

    /**
     * {@inheritDoc}
     */
    @Override
    public DurableStoreConnection initializeMessageStore(ConfigurationProperties
                                                                 connectionProperties)
            throws AndesException {
        // create connection object
        //todo remove this if after testing
        if (hectorConnection == null) {
            hectorConnection = new HectorConnection();
        }
        hectorConnection.initialize(connectionProperties);

        // get cassandra cluster and create column families
        initializeCassandraMessageStore(hectorConnection);

        //TODO: The trackingTimeOut (15000000000L) and  trackingMessagesRemovalTaskIntervalInSec
        // (10) should be configurable. Opened JIRA (MB-781)
        alreadyMovedMessageTracker = new AlreadyProcessedMessageTracker("Message-move-tracker",
                15000000000L, 10);

        return hectorConnection;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeMessagePart(List<AndesMessagePart> partList) throws AndesException {
        try {

            Mutator<String> mutator = HFactory.createMutator(keyspace,
                    CassandraConstants.stringSerializer);

            for (AndesMessagePart part : partList) {
                final String rowKey = AndesConstants.MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX
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
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessageParts(List<Long> messageIdList) throws AndesException {
        try {

            Mutator<String> mutator = HFactory.createMutator(keyspace,
                    CassandraConstants.stringSerializer);

            List<String> rows2Remove = new ArrayList<String>();
            for (long messageId : messageIdList) {
                rows2Remove.add(AndesConstants.MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX +
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
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesMessagePart getContent(long messageId, int offsetValue) throws AndesException {
        try {
            String rowKey = AndesConstants.MESSAGE_CONTENT_CASSANDRA_ROW_NAME_PREFIX + messageId;
            return HectorDataAccessHelper.getMessageContent(rowKey,
                    CassandraConstants.MESSAGE_CONTENT_COLUMN_FAMILY, keyspace, messageId,
                    offsetValue);

        } catch (Exception e) {
            throw new AndesException(
                    "Error while reading content messageID=" + messageId + " offset=" +
                            offsetValue, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetaData(List<AndesMessageMetadata> metadataList) throws AndesException {
        try {

            Mutator<String> mutator = HFactory.createMutator(keyspace,
                    CassandraConstants.stringSerializer);

            for (AndesMessageMetadata metadata : metadataList) {
                HectorDataAccessHelper.addMessageToQueue(
                        CassandraConstants.META_DATA_COLUMN_FAMILY,
                        metadata.getDestination(),
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
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetaData(AndesMessageMetadata metadata) throws AndesException {
        String destination = metadata.getDestination();
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace,
                    CassandraConstants.stringSerializer);

            HectorDataAccessHelper.addMessageToQueue(
                    CassandraConstants
                            .META_DATA_COLUMN_FAMILY,
                    destination,
                    metadata.getMessageID(),
                    metadata.getMetadata(), mutator, true);

        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while writing incoming message to cassandra.", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetaDataToQueue(String queueName, AndesMessageMetadata metadata)
            throws AndesException {
        String destination;

        if (queueName == null) {
            destination = metadata.getDestination();
        } else {
            destination = queueName;
        }

        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace,
                    CassandraConstants.stringSerializer);

            HectorDataAccessHelper.addMessageToQueue(CassandraConstants
                    .META_DATA_COLUMN_FAMILY,
                    destination,
                    metadata.getMessageID(),
                    metadata.getMetadata(), mutator, true);

        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while writing incoming message to cassandra.", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetadataToQueue(String queueName, List<AndesMessageMetadata> metadataList)
            throws AndesException {
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
        try {
            Mutator<String> insertMutator = HFactory.createMutator(keyspace,
                    CassandraConstants.stringSerializer);
            Mutator<String> deleteMutator = HFactory.createMutator(keyspace,
                    CassandraConstants.stringSerializer);

            // Step 1 - Insert the new meta data
            for (AndesMessageMetadata metadata : metadataList) {
                HectorDataAccessHelper.addMessageToQueue(CassandraConstants
                        .META_DATA_COLUMN_FAMILY,
                        metadata.getDestination(),
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
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesMessageMetadata getMetaData(long messageId) throws AndesException {
        try {

            byte[] value = HectorDataAccessHelper
                    .getMessageMetaDataOfMessage(CassandraConstants.META_DATA_COLUMN_FAMILY,
                            keyspace, messageId);
            return new AndesMessageMetadata(messageId, value, true);

        } catch (Exception e) {
            throw new AndesException("Error while getting meta data for messageID " + messageId,
                    e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getMetaDataList(String queueName, long firstMsgId,
                                                      long lastMsgID) throws AndesException {
        try {
            return HectorDataAccessHelper.getMessagesFromQueue
                    (queueName, CassandraConstants.META_DATA_COLUMN_FAMILY, keyspace, firstMsgId,
                            lastMsgID, 1000, true);
        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while reading meta data list for message IDs " +
                    "from " + firstMsgId + " to " + lastMsgID, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(String queueName,
                                                                       long firstMsgId, int count)
            throws AndesException {
        try {
            return HectorDataAccessHelper
                    .getMessagesFromQueue(queueName,
                            CassandraConstants.META_DATA_COLUMN_FAMILY,
                            keyspace, firstMsgId + 1, Long.MAX_VALUE,
                            Integer.MAX_VALUE, true);

        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while reading meta data list for message IDs " +
                    "from " + firstMsgId + " to " + firstMsgId, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessageMetadataFromQueue(String queueName, List<AndesRemovableMetadata>
            messagesToRemove) throws AndesException {
        try {
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
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesRemovableMetadata> getExpiredMessages(int limit) throws AndesException {
        //todo: implement
        return null;
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
                                        boolean isMessageForTopic, String destination)
            throws AndesException {
        //TODO implement
    }

    @Override
    public void deleteAllMessageMetadata(String storageQueueName) throws AndesException {
        //TODO implement. If we decide to use hector instead of cql ,
        // these methods must be implemented.
    }

    @Override
    public void resetMessageCounterForQueue(String storageQueueName) throws AndesException {
        //TODO implement. If we decide to use hector instead of cql ,
        // these methods must be implemented.
    }

    @Override
    public int deleteAllMessageMetadataFromDLC(String storageQueueName, String DLCQueueName) throws AndesException {
        return 0;  //TODO implement. If we decide to use hector instead of cql ,
        // these methods must be implemented.
    }

    @Override
    public List<Long> getMessageIDsAddressedToQueue(String storageQueueName) throws AndesException {
        return null;  //TODO implement. If we decide to use hector instead of cql ,
        // these methods must be implemented.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        alreadyMovedMessageTracker.shutDownMessageTracker();
        hectorConnection.close();
    }

    /**
     * Initialize HectorBasedMessageStoreImpl
     *
     * @param hectorConnection hector based connection to Cassandra
     * @throws AndesException
     */
    private void initializeCassandraMessageStore(HectorConnection hectorConnection) throws
            AndesException {
        try {
            keyspace = hectorConnection.getKeySpace();
            createColumnFamilies(hectorConnection, hectorConnection.getCluster());
        } catch (CassandraDataAccessException e) {
            throw new AndesException("Error while initializing cassandra message store", e);
        }
    }

    /**
     * Create a cassandra column families for andes usage
     *
     * @throws CassandraDataAccessException
     */
    private void createColumnFamilies(HectorConnection connection, Cluster cluster) throws
            CassandraDataAccessException {
        int gcGraceSeconds = connection.getGcGraceSeconds();
        HectorDataAccessHelper.createColumnFamily(CassandraConstants.MESSAGE_CONTENT_COLUMN_FAMILY,
                CassandraConstants.KEYSPACE, cluster,
                CassandraConstants.INTEGER_TYPE,
                gcGraceSeconds);
        HectorDataAccessHelper.createColumnFamily(CassandraConstants.META_DATA_COLUMN_FAMILY,
                CassandraConstants.KEYSPACE, cluster,
                CassandraConstants.LONG_TYPE,
                gcGraceSeconds);
        HectorDataAccessHelper
                .createCounterColumnFamily(CassandraConstants.MESSAGE_COUNTERS_COLUMN_FAMILY,
                        CassandraConstants.KEYSPACE, cluster,
                        gcGraceSeconds);
        HectorDataAccessHelper.createMessageExpiryColumnFamily(
                CassandraConstants.MESSAGES_FOR_EXPIRY_COLUMN_FAMILY, CassandraConstants.KEYSPACE,
                cluster, CassandraConstants.UTF8_TYPE, gcGraceSeconds);
    }
}
