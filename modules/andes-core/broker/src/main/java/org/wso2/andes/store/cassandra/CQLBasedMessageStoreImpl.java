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

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.AndesRemovableMetadata;
import org.wso2.andes.kernel.DurableStoreConnection;
import org.wso2.andes.kernel.MessageStore;
import org.wso2.andes.matrics.DataAccessMetricsManager;
import org.wso2.andes.matrics.MetricsConstants;
import org.wso2.andes.store.AndesStoreUnavailableException;
import org.wso2.carbon.metrics.manager.Timer.Context;

/**
 * CQL 3 based Cassandra MessageStore implementation. This is intended to support Cassandra 2.xx series upwards.
 */
public class CQLBasedMessageStoreImpl implements MessageStore {

  
    private static final Logger log = Logger.getLogger(CQLBasedMessageStoreImpl.class);
   

    private CassandraConfig config;
    private CQLConnection cqlConnection;
    
    private AndesContextStore contextStore;

    private CQLUtils cqlUtils;
    
    /**
     * CQL prepared statement to insert message content to DB
     * Params to bind
     * - MESSAGE_ID
     * - MESSAGE_OFFSET
     * - MESSAGE_CONTENT
     */
    private static final String PS_INSERT_MESSAGE_PART =
            "INSERT INTO " + CQLConstants.CONTENT_TABLE + " ( " +
                    CQLConstants.MESSAGE_ID + ", " + CQLConstants.MESSAGE_OFFSET + ", " +
                    CQLConstants.MESSAGE_CONTENT + ") " +
                    "VALUES (?,?,?)";

    /**
     * CQL prepared statement to delete message content of a given message ID
     * Params to bind
     * - MESSAGE_ID
     */
    private static final String PS_DELETE_MESSAGE_CONTENT =
            "DELETE FROM " + CQLConstants.CONTENT_TABLE +
                    " WHERE " + CQLConstants.MESSAGE_ID + " =?";

    /**
     * CQL prepared statement to insert message metadata to DB
     * Params to bind
     * - QUEUE_NAME
     * - MESSAGE_ID
     * - METADATA
     */
    private static final String PS_INSERT_METADATA =
            "INSERT INTO " + CQLConstants.METADATA_TABLE + " ( " +
                    CQLConstants.QUEUE_NAME + "," + CQLConstants.MESSAGE_ID + "," + CQLConstants.METADATA + ") " +
                    " VALUES (?,?,?);";

    /**
     * CQL prepared statement to delete metadata from DB
     * Params to bind:
     * - MESSAGE_ID
     */
    private static final String PS_DELETE_METADATA =
            "DELETE FROM " + CQLConstants.METADATA_TABLE +
                    " WHERE " + CQLConstants.QUEUE_NAME + "=? AND " +
                    CQLConstants.MESSAGE_ID + "=?";


    /**
     * CQL prepared statement to update retain metadata
     */
    private static final String PS_UPDATE_RETAIN_METADATA =
            "UPDATE " + CQLConstants.RETAINED_METADATA_TABLE + " SET " + CQLConstants.MESSAGE_ID +
            "=?, " + CQLConstants.METADATA + "=?" + " WHERE " + CQLConstants.TOPIC_ID + "=? AND " +
            CQLConstants.TOPIC_NAME + "=?";

    /**
     * CQL prepared statement to delete retain content part
     */
    private static final String PS_DELETE_RETAIN_MESSAGE_PART =
            "DELETE FROM "+ CQLConstants.RETAINED_CONTENT_TABLE + " WHERE " +
            CQLConstants.MESSAGE_ID + "=?";

    /**
     * CQL prepared statement to insert retain content part
     */
    private static final String PS_INSERT_RETAIN_MESSAGE_PART =
            "INSERT INTO " + CQLConstants.RETAINED_CONTENT_TABLE +
            " ( " + CQLConstants.MESSAGE_ID + "," + CQLConstants.MESSAGE_OFFSET + "," +
            CQLConstants.MESSAGE_CONTENT + ") " + " VALUES (?,?,?);";

    // Prepared Statements bound to session
    private PreparedStatement psInsertMessagePart;
    private PreparedStatement psDeleteMessagePart;
    private PreparedStatement psInsertMetadata;
    private PreparedStatement psDeleteMetadata;
    private PreparedStatement psUpdateRetainMetadata;
    private PreparedStatement psDeleteRetainMessagePart;
    private PreparedStatement psInsertRetainMessagePart;

    public CQLBasedMessageStoreImpl() {
        config = new CassandraConfig();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DurableStoreConnection initializeMessageStore(AndesContextStore contextStore,
                                                         ConfigurationProperties connectionProperties) throws AndesException {

        cqlConnection = new CQLConnection();
        cqlUtils = new CQLUtils();
        cqlConnection.initialize(connectionProperties);

        this.contextStore = contextStore;
        config.parse(connectionProperties);
        createSchema(cqlConnection, cqlUtils);

        Session session = cqlConnection.getSession();

        // CQL we can keep the prepared statements for a given session.
        // creating prepared statements per method request is not efficient
        psInsertMessagePart = session.prepare(PS_INSERT_MESSAGE_PART);
        psDeleteMessagePart = session.prepare(PS_DELETE_MESSAGE_CONTENT);
        psInsertMetadata = session.prepare(PS_INSERT_METADATA);
        psDeleteMetadata = session.prepare(PS_DELETE_METADATA);
        psUpdateRetainMetadata    = session.prepare(PS_UPDATE_RETAIN_METADATA);
        psDeleteRetainMessagePart = session.prepare(PS_DELETE_RETAIN_MESSAGE_PART);
        psInsertRetainMessagePart = session.prepare(PS_INSERT_RETAIN_MESSAGE_PART);

        return cqlConnection;
    }

    /**
     * Uses the connection and connection properties to create a new key space if the current KeySpace doesn't exist
     *
     * @param connection CQLConnection
     */
    private void createSchema(CQLConnection connection, CQLUtils cqlUtils) {

        Session session = connection.getSession();

        Statement statement = SchemaBuilder.createTable(config.getKeyspace(), CQLConstants.CONTENT_TABLE).ifNotExists().
                addPartitionKey(CQLConstants.MESSAGE_ID, DataType.bigint()).
                addClusteringColumn(CQLConstants.MESSAGE_OFFSET, DataType.cint()).
                addColumn(CQLConstants.MESSAGE_CONTENT, DataType.blob()).
                withOptions().gcGraceSeconds(config.getGcGraceSeconds()).
                setConsistencyLevel(config.getWriteConsistencyLevel());
        session.execute(statement);

        statement = SchemaBuilder.createTable(config.getKeyspace(), CQLConstants.METADATA_TABLE).ifNotExists().
                addPartitionKey(CQLConstants.QUEUE_NAME, DataType.text()).
                addClusteringColumn(CQLConstants.MESSAGE_ID, DataType.bigint()).
                addColumn(CQLConstants.METADATA, DataType.blob()).
                withOptions().clusteringOrder(CQLConstants.MESSAGE_ID, SchemaBuilder.Direction.ASC).
                gcGraceSeconds(config.getGcGraceSeconds()).
                setConsistencyLevel(config.getWriteConsistencyLevel());
        session.execute(statement);

        statement = SchemaBuilder.createTable(config.getKeyspace(), CQLConstants.RETAINED_CONTENT_TABLE).ifNotExists().
                addPartitionKey(CQLConstants.MESSAGE_ID, DataType.bigint()).
                addClusteringColumn(CQLConstants.MESSAGE_OFFSET, DataType.cint()).
                addColumn(CQLConstants.MESSAGE_CONTENT, DataType.blob()).
                withOptions().gcGraceSeconds(config.getGcGraceSeconds()).
                setConsistencyLevel(config.getWriteConsistencyLevel());
        session.execute(statement);

        statement = SchemaBuilder.createTable(config.getKeyspace(), CQLConstants.RETAINED_METADATA_TABLE).ifNotExists().
                addPartitionKey(CQLConstants.TOPIC_NAME, DataType.text()).
                addClusteringColumn(CQLConstants.TOPIC_ID, DataType.bigint()).
                addColumn(CQLConstants.MESSAGE_ID, DataType.bigint()).
                addColumn(CQLConstants.METADATA, DataType.blob()).
                withOptions().clusteringOrder(CQLConstants.TOPIC_ID, SchemaBuilder.Direction.ASC).
                gcGraceSeconds(config.getGcGraceSeconds()).
                setConsistencyLevel(config.getWriteConsistencyLevel());
        session.execute(statement);


        cqlUtils.createSchema(connection, config);        

        // Message expiration feature moved to MB 3.1.0
/*
        statement = SchemaBuilder.createTable(config.getKeyspace(), EXPIRATION_TABLE).ifNotExists().
                addPartitionKey(EXPIRATION_TIME_RANGE, DataType.bigint()).
                addClusteringColumn(EXPIRATION_TIME, DataType.bigint()).
                addColumn(MESSAGE_ID, DataType.bigint()).
                addColumn(DESTINATION_QUEUE, DataType.bigint()).
                withOptions().gcGraceSeconds(config.getGcGraceSeconds()).
                setConsistencyLevel(config.getWriteConsistencyLevel());
        session.execute(statement);
*/
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeMessagePart(List<AndesMessagePart> partList) throws AndesException {

        Context context = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.ADD_MESSAGE_PART, this.getClass()).start();

        try {
            BatchStatement batchStatement = new BatchStatement();
            batchStatement.setConsistencyLevel(config.getWriteConsistencyLevel());

            for (AndesMessagePart andesMessagePart : partList) {
                batchStatement.add(psInsertMessagePart.bind(
                                andesMessagePart.getMessageID(),
                                andesMessagePart.getOffSet(),
                                ByteBuffer.wrap(andesMessagePart.getData()))
                );
            }

            execute(batchStatement, "adding message parts list. List size " + partList.size());
        } finally {
            context.stop();
        }
    }

    /**
     * Add the given content list to the {@link com.datastax.driver.core.BatchStatement}
     * @param batchStatement {@link com.datastax.driver.core.BatchStatement } the content insertion query
     *                       should added to
     * @param partList Content list
     */
    private void addContentToBatch(BatchStatement batchStatement, List<AndesMessagePart> partList) {
        for (AndesMessagePart andesMessagePart : partList) {
            batchStatement.add(psInsertMessagePart.bind(
                            andesMessagePart.getMessageID(),
                            andesMessagePart.getOffSet(),
                            ByteBuffer.wrap(andesMessagePart.getData()))
            );
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessageParts(Collection<Long> messageIdList) throws AndesException {

        Context context = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.DELETE_MESSAGE_PART, this.getClass()).start();

        try {
            BatchStatement batchStatement = new BatchStatement();
            batchStatement.setConsistencyLevel(config.getWriteConsistencyLevel());
            Iterator<Long> messages = messageIdList.iterator();
            //We need to maintain a pointer to ensure that the batch size limitation would not be exceeded
            int messageIDPointer = 0;

            while (messages.hasNext()) {
                Long messageID = messages.next();
                batchStatement.add(psDeleteMessagePart.bind(messageID));
                messageIDPointer = messageIDPointer + 1;

                //If the maximum batch limit has exceeded we need to execute the current batch first
                if(messageIDPointer == CQLConstants.MAX_MESSAGE_BATCH_SIZE){
                    execute(batchStatement, "deleting message part list. List size " + messageIdList.size());
                    //Will clear the batch to add on the rest of the elements
                    batchStatement.clear();
                    //Will reset the pointer back to its origin
                    messageIDPointer = 0;
                }

            }
            //We need to execute if the batch statement has elements in it
            if(batchStatement.size() > 0) {
                execute(batchStatement, "deleting message part list. List size " + messageIdList.size());
            }

        } finally {
            context.stop();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AndesMessagePart getContent(long messageId, int offsetValue) throws AndesException {

        Context context = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.GET_CONTENT, this.getClass()).start();

        try {
            Statement statement = QueryBuilder.select().all().
                    from(config.getKeyspace(), CQLConstants.CONTENT_TABLE).
                    where(eq(CQLConstants.MESSAGE_ID, messageId)).and(eq(CQLConstants.MESSAGE_OFFSET, offsetValue)).
                    setConsistencyLevel(config.getReadConsistencyLevel());

            ResultSet resultSet = execute(statement, "retrieving message part for msg id " + messageId +
                    " offset " + offsetValue);

            AndesMessagePart messagePart = null;
            Row row = resultSet.one();
            if (null != row) {
                ByteBuffer buffer = row.getBytes(CQLConstants.MESSAGE_CONTENT);
                byte[] content = new byte[buffer.remaining()];
                buffer.get(content);

                messagePart = new AndesMessagePart();
                messagePart.setMessageID(row.getLong(CQLConstants.MESSAGE_ID));
                messagePart.setOffSet(row.getInt(CQLConstants.MESSAGE_OFFSET));
                messagePart.setData(content);
                messagePart.setDataLength(content.length);
            }
            return messagePart;
        } finally {
            context.stop();

        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<Long, List<AndesMessagePart>> getContent(List<Long> messageIdList) throws AndesException {
        Context context = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.GET_CONTENT_BATCH, this.getClass()).start();

        try {

            //The content batch would hold the message id as the key and the content chunks as the value
            Map<Long, List<AndesMessagePart>> messageContentBatch = new HashMap<Long, List<AndesMessagePart>>
                    (messageIdList.size());

            //First we need to convert the list into a Long [] since collections are not supported by CQL
            //More information could be found in https://datastax-oss.atlassian.net/browse/JAVA-110
            Object[] messageIds = messageIdList.toArray();

            //SELECT * FROM MB_KEYSPACE.MB_CONTENT WHERE MESSAGE_ID IN (messageIDs...);
            Statement statement = QueryBuilder.select().all().
                    from(config.getKeyspace(), CQLConstants.CONTENT_TABLE).
                    where(in(CQLConstants.MESSAGE_ID, messageIds)).
                    setConsistencyLevel(config.getReadConsistencyLevel());

            //The list of messages retrieved from the database
            ResultSet listOfMessages = execute(statement, "retrieving message part for the provided list of ids");

            //Will iterate through each message and will create
            for (Row row : listOfMessages) {
                long messageID = row.getLong(CQLConstants.MESSAGE_ID);
                int offset = row.getInt(CQLConstants.MESSAGE_OFFSET);
                ByteBuffer buffer = row.getBytes(CQLConstants.MESSAGE_CONTENT);
                byte[] content = new byte[buffer.remaining()];
                buffer.get(content);

                List<AndesMessagePart> partList = messageContentBatch.get(messageID);

                //If the message has not being added
                if (null == partList) {
                    partList = new ArrayList<AndesMessagePart>();
                    messageContentBatch.put(messageID, partList);
                }

                //Will create a message part
                AndesMessagePart messagePart = new AndesMessagePart();
                messagePart.setMessageID(messageID);
                messagePart.setOffSet(offset);
                messagePart.setData(content);
                messagePart.setDataLength(content.length);

                partList.add(messagePart);

            }
            return messageContentBatch;
        } finally {
            context.stop();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetaData(List<AndesMessageMetadata> metadataList) throws AndesException {

        Context context = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.GET_META_DATA_LIST, this.getClass()).start();

        try {
            BatchStatement batchStatement = new BatchStatement();
            batchStatement.setConsistencyLevel(config.getWriteConsistencyLevel());

            for (AndesMessageMetadata metadata : metadataList) {
                addMetadataToBatch(batchStatement, metadata, metadata.getStorageQueueName());
            }

            execute(batchStatement, " adding metadata list. list size " + metadataList.size());
        } finally {
            context.stop();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetaData(AndesMessageMetadata metadata) throws AndesException {

        Context context = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.ADD_META_DATA, this.getClass()).start();

        try {
            BatchStatement batchStatement = new BatchStatement();
            batchStatement.setConsistencyLevel(config.getWriteConsistencyLevel());
            addMetadataToBatch(batchStatement, metadata, metadata.getStorageQueueName());

            execute(batchStatement, "adding metadata with msg id " + metadata.getMessageID() + " storage queue "
                    + metadata.getStorageQueueName());
        } finally {
            context.stop();
        }
    }

    /**
     * Helper method to avoid code duplication for binding andes message to prepared statement.
     * In an event of change in metadata insertion query only need to chang the logic in this method
     *
     * @param batchStatement BatchStatement
     * @param metadata       AndesMessageMetadata to be added to DB
     * @param queueName      storage queue name
     */
    private void addMetadataToBatch(BatchStatement batchStatement,
                                    AndesMessageMetadata metadata,
                                    String queueName) {

        batchStatement.add(psInsertMetadata.bind(
                        queueName,
                        metadata.getMessageID(),
                        ByteBuffer.wrap(metadata.getMetadata()))
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetaDataToQueue(String queueName,
                                   AndesMessageMetadata metadata) throws AndesException {

        Context context = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.ADD_META_DATA_TO_QUEUE, this.getClass()).start();

        try {
            BatchStatement batchStatement = new BatchStatement();
            batchStatement.setConsistencyLevel(config.getWriteConsistencyLevel());
            addMetadataToBatch(batchStatement, metadata, queueName);

            execute(batchStatement, "adding metadata to queue " + queueName +
                    " with msg id " + metadata.getMessageID());
        } finally {
            context.stop();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMetadataToQueue(String queueName,
                                   List<AndesMessageMetadata> metadataList) throws AndesException {

        Context context = DataAccessMetricsManager.
                addAndGetTimer(MetricsConstants.ADD_META_DATA_TO_QUEUE_LIST, this.getClass()).start();

        try {
            BatchStatement batchStatement = new BatchStatement();
            batchStatement.setConsistencyLevel(config.getWriteConsistencyLevel());

            for (AndesMessageMetadata metadata : metadataList) {
                addMetadataToBatch(batchStatement, metadata, queueName);
            }

            execute(batchStatement, "adding metadata list to queue ");
        } finally {
            context.stop();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void moveMetaDataToQueue(long messageId,
                                    String currentQueueName,
                                    String targetQueueName) throws AndesException {
        List<AndesMessageMetadata> metadataList = getNextNMessageMetadataFromQueue(currentQueueName, messageId, 1);

        if (metadataList.isEmpty()) {
            throw new AndesException("Message MetaData not found to move the message to Dead Letter Channel");
        }

        List<AndesRemovableMetadata> removableMetadataList = new ArrayList<AndesRemovableMetadata>(1);
        removableMetadataList.add(new AndesRemovableMetadata(messageId, currentQueueName, currentQueueName));

        addMetaDataToQueue(targetQueueName, metadataList.get(0));
        deleteMessageMetadataFromQueue(currentQueueName, removableMetadataList);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateMetaDataInformation(String currentQueueName,
                                          List<AndesMessageMetadata> metadataList) throws AndesException {

        Context context = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.UPDATE_META_DATA_INFORMATION,
                this.getClass()).start();

        try {
            // Step 1 add metadata to the new queue
            addMetaData(metadataList);

            // Step 2 - Delete the old meta data when inserting new meta is complete to avoid
            // losing messages
            List<AndesRemovableMetadata> removableMetadataList = new ArrayList<AndesRemovableMetadata>(metadataList.size());
            for (AndesMessageMetadata metadata : metadataList) {
                removableMetadataList.add(new AndesRemovableMetadata(metadata.getMessageID(),
                                metadata.getDestination(),
                                currentQueueName)
                );
            }
            deleteMessageMetadataFromQueue(currentQueueName, removableMetadataList);
        } finally {
            context.stop();
        }
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This method is very expensive in Cassandra. The partition key of table is queue name. Therefore to get the
     * metadata with given message id query needs to go through different queues (rows in table) stored in different
     * nodes in Cassandra cluster.
     * <p/>
     * USE THIS WITH CAUTION! DON'T USE IN CRITICAL PATH
     */
    @Override
    public AndesMessageMetadata getMetaData(long messageId) throws AndesException {
        Context context = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.GET_META_DATA, this.getClass())
                .start();

        try {
            AndesMessageMetadata metadata = null;

            Statement statement = QueryBuilder.select().column(CQLConstants.METADATA).
                    from(config.getKeyspace(), CQLConstants.METADATA_TABLE).
                    where(eq(CQLConstants.MESSAGE_ID, messageId)).
                    limit(1).
                    allowFiltering().
                    setConsistencyLevel(config.getReadConsistencyLevel());

            ResultSet resultSet = execute(statement, "retrieving metadata for msg id " + messageId);
            Row row = resultSet.one();

            if (null != row) {
                metadata = getMetadataFromRow(row, messageId);
            }
            return metadata;
        } finally {
            context.stop();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getMetaDataList(String queueName,
                                                      long firstMsgId,
                                                      long lastMsgID) throws AndesException {

        Context context = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.GET_META_DATA_LIST, this.getClass()).start();

        try {
            Statement statement = QueryBuilder.select().column(CQLConstants.MESSAGE_ID).column(CQLConstants.METADATA).
                    from(config.getKeyspace(), CQLConstants.METADATA_TABLE).
                    where(eq(CQLConstants.QUEUE_NAME, queueName)).
                    and(gte(CQLConstants.MESSAGE_ID, firstMsgId)).
                    and(lte(CQLConstants.MESSAGE_ID, lastMsgID)).
                    setConsistencyLevel(config.getReadConsistencyLevel());

            ResultSet resultSet = execute(statement, "retrieving metadata list from queue " + queueName +
                    "between msg id " + firstMsgId + " and " + lastMsgID);
            List<AndesMessageMetadata> metadataList =
                    new ArrayList<AndesMessageMetadata>(resultSet.getAvailableWithoutFetching());

            for (Row row : resultSet) {
                metadataList.add(getMetadataFromRow(row, row.getLong(CQLConstants.MESSAGE_ID)));
            }
            return metadataList;

        } finally {
            context.stop();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageMetadata> getNextNMessageMetadataFromQueue(String storageQueueName,
                                                                       long firstMsgId,
                                                                       int count) throws AndesException {

        Context context = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.
                GET_NEXT_MESSAGE_METADATA_FROM_QUEUE, this.getClass()).start();

        try {
            Statement statement = QueryBuilder.select().column(CQLConstants.METADATA).column(CQLConstants.MESSAGE_ID).
                    from(config.getKeyspace(), CQLConstants.METADATA_TABLE).
                    where(eq(CQLConstants.QUEUE_NAME, storageQueueName)).
                    and(gte(CQLConstants.MESSAGE_ID, firstMsgId)).
                    limit(count).
                    setConsistencyLevel(config.getReadConsistencyLevel());

            ResultSet resultSet = execute(statement, "retrieving metadata list from " + storageQueueName +
                    " with starting msg id " + firstMsgId + " and limit " + count);
            List<AndesMessageMetadata> metadataList =
                    new ArrayList<AndesMessageMetadata>(resultSet.getAvailableWithoutFetching());

            for (Row row : resultSet) {
                metadataList.add(getMetadataFromRow(row, row.getLong(CQLConstants.MESSAGE_ID)));
            }
            return metadataList;
        } finally {
            context.stop();
        }
    }

    /**
     * Retrieves AndesMessageMetadata from a given row. (ResultSet row)
     * @param row Row
     * @param messageID messageID of metadata
     * @return AndesMessageMetadata
     */
    private AndesMessageMetadata getMetadataFromRow(Row row, long messageID) {
        ByteBuffer buffer = row.getBytes(CQLConstants.METADATA);
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new AndesMessageMetadata(messageID, bytes, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessageMetadataFromQueue(String storageQueueName,
                                               List<AndesRemovableMetadata> messagesToRemove) throws AndesException {

        Context context = DataAccessMetricsManager.addAndGetTimer(MetricsConstants.DELETE_MESSAGE_META_DATA_FROM_QUEUE,
                this.getClass()).start();

        try {
            BatchStatement batchStatement = new BatchStatement();

            for (AndesRemovableMetadata metadata : messagesToRemove) {
                batchStatement.add(psDeleteMetadata.bind(
                        storageQueueName,
                        metadata.getMessageID()
                ));
            }

            execute(batchStatement, "deleting metadata list from " + storageQueueName +
                    " list size " + messagesToRemove.size());
        } finally {
            context.stop();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesRemovableMetadata> getExpiredMessages(int limit) throws AndesException {
        // Message expiration feature moved to MB 3.1.0
        return new ArrayList<AndesRemovableMetadata>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessagesFromExpiryQueue(List<Long> messagesToRemove) throws AndesException {
        // Message expiration feature moved to MB 3.1.0
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMessageToExpiryQueue(Long messageId,
                                        Long expirationTime,
                                        boolean isMessageForTopic,
                                        String destination) throws AndesException {
        // Message expiration feature moved to MB 3.1.0
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteAllMessageMetadata(String storageQueueName) throws AndesException {

        Statement statement = QueryBuilder.delete().from(config.getKeyspace(), CQLConstants.METADATA_TABLE).
                where(eq(CQLConstants.QUEUE_NAME, storageQueueName)).
                setConsistencyLevel(config.getWriteConsistencyLevel());

        execute(statement, "deleting all metadata from " + storageQueueName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int deleteAllMessageMetadataFromDLC(String storageQueueName,
                                               String DLCQueueName) throws AndesException {

        Statement query = QueryBuilder.select().column(CQLConstants.MESSAGE_ID).column(CQLConstants.METADATA).
                from(config.getKeyspace(), CQLConstants.METADATA_TABLE).
                where(eq(CQLConstants.QUEUE_NAME, DLCQueueName)).
                setConsistencyLevel(config.getReadConsistencyLevel());

        ResultSet resultSet = execute(query, "retrieving metadata from DLC " + DLCQueueName);

        // Internally CQL retrieves results as pages. Doesn't load all the result at once to client side
        // using the same fetch size to iterate through the messages and delete from DLC
        int fetchSize = query.getFetchSize();
        int removedMessageCount = 0;
        List<AndesRemovableMetadata> removableMetadataList = new ArrayList<AndesRemovableMetadata>(fetchSize);
        AndesMessageMetadata metadata;

        for (Row row : resultSet) {
            metadata = getMetadataFromRow(row, row.getLong(CQLConstants.MESSAGE_ID));

            if (metadata.getStorageQueueName().equals(storageQueueName)) {

                AndesRemovableMetadata removableMetadata = new AndesRemovableMetadata(
                        metadata.getMessageID(),
                        metadata.getDestination(),
                        metadata.getStorageQueueName()
                );
                removableMetadataList.add(removableMetadata);
            }

            // When the end of current fetched results is reached we delete that found removable messages from DLC
            if (resultSet.getAvailableWithoutFetching() == 0 && !removableMetadataList.isEmpty()) {
                deleteMessageMetadataFromQueue(DLCQueueName, removableMetadataList);
                removedMessageCount = removedMessageCount + removableMetadataList.size();
                removableMetadataList.clear();
            }
        }
        return removedMessageCount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Long> getMessageIDsAddressedToQueue(String storageQueueName, Long startMessageID) throws AndesException {

        Statement statement = QueryBuilder.select().column(CQLConstants.MESSAGE_ID).
                from(config.getKeyspace(), CQLConstants.METADATA_TABLE).
                where(eq(CQLConstants.QUEUE_NAME, storageQueueName)).and(gte(CQLConstants.MESSAGE_ID,startMessageID)).
                setConsistencyLevel(config.getReadConsistencyLevel());

        ResultSet resultSet = execute(statement, "retrieving message ids addressed to " + storageQueueName);
        List<Long> msgIDList = new ArrayList<Long>(resultSet.getAvailableWithoutFetching());

        for (Row row : resultSet) {
            msgIDList.add(row.getLong(CQLConstants.MESSAGE_ID));
        }
        return msgIDList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addQueue(String destinationQueueName) throws AndesException {
        contextStore.addMessageCounterForQueue(destinationQueueName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getMessageCountForQueue(String destinationQueueName) throws AndesException {
        return contextStore.getMessageCountForQueue(destinationQueueName);
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
    public void removeQueue(String destinationQueueName) throws AndesException {
        contextStore.removeMessageCounterForQueue(destinationQueueName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void incrementMessageCountForQueue(String destinationQueueName, long incrementBy) throws AndesException {
        contextStore.incrementMessageCountForQueue(destinationQueueName, incrementBy);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void decrementMessageCountForQueue(String destinationQueueName, long decrementBy) throws AndesException {
        contextStore.decrementMessageCountForQueue(destinationQueueName, decrementBy);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        cqlConnection.close();
    }

    @Override
    public AndesTransaction newTransaction() throws AndesException {
        return new CQLBasedTransactionImpl();
    }

    /**
     * Executes the statement using the given session and returns the resultSet.
     * Additionally this catches any NoHostAvailableException or QueryExecutionException thrown by CQL driver and
     * rethrows an AndesException
     *
     * @param statement statement to be executed
     * @param task description of the task that is done by the statement
     * @return ResultSet
     * @throws AndesException
     */
    private ResultSet execute(Statement statement, String task) throws AndesException {
        try {
            return cqlConnection.getSession().execute(statement);
        } catch (NoHostAvailableException e) {
            
            log.error("Unable to connect to cassandra cluster for " + task, e);
            
            Map<InetSocketAddress,Throwable> errors = e.getErrors();
            for ( Entry<InetSocketAddress, Throwable> err: errors.entrySet()){
                log.error("Error occured while connecting to cassandra server: " + err.getKey() + " error: ", err.getValue());
            }
            
            throw new AndesStoreUnavailableException("error occured while trying to connect to cassandra server(s) for " + task, e);
            
        } catch (QueryExecutionException e) {
            throw new AndesStoreUnavailableException("Error occurred while " + task, e);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    public boolean isOperational(String testString, long testTime) {

        /* Order of tests done is important here */
        return cqlUtils.isReachable(cqlConnection) &&
               cqlUtils.testInsert(cqlConnection, config, testString, testTime) &&
               cqlUtils.testRead(cqlConnection, config, testString, testTime) &&
               cqlUtils.testDelete(cqlConnection, config, testString, testTime);  
        
    }    
    
   
    /**
     * Store retain message list in database
     *
     * {@inheritDoc}
     */
    @Override
    public void storeRetainedMessages(Map<String,AndesMessage> retainMap) throws AndesException {


        for (AndesMessage message : retainMap.values()) {

            AndesMessageMetadata metadata = message.getMetadata();
            String destination = metadata.getDestination();
            RetainedItemData retainedItemData = getRetainedTopicID(destination);

            if (null != retainedItemData) {

                addRetainedMessageToUpdateBatch(message, metadata, retainedItemData);
                retainedItemData.messageID = metadata.getMessageID();
            } else {
                createRetainedEntry(message);
            }

        }

    }

    /**
     * Update already existing retain topic message
     *
     * @param message
     * @param metadata
     * @param retainedItemData
     * @throws AndesException
     */
    private void addRetainedMessageToUpdateBatch(AndesMessage message,
                                                 AndesMessageMetadata metadata,
                                                 RetainedItemData retainedItemData)
            throws AndesException {

        BatchStatement batchStatement = new BatchStatement();

        // update metadata
        batchStatement.add(psUpdateRetainMetadata.bind(
                metadata.getMessageID(),
                ByteBuffer.wrap(metadata.getMetadata()),
                retainedItemData.topicID,
                metadata.getDestination()
        ));

        // update content
        batchStatement.add(psDeleteRetainMessagePart.bind(
                retainedItemData.messageID
        ));
        for (AndesMessagePart messagePart : message.getContentChunkList()) {
            batchStatement.add(psInsertRetainMessagePart.bind(
                    metadata.getMessageID(),
                    messagePart.getOffSet(),
                    ByteBuffer.wrap(messagePart.getData())
            ));
        }

        execute(batchStatement, "update retain message content and metadata for topic " +
                                metadata.getDestination());

    }

    /**
     * Create new retain entry for given topic message
     *
     * @param message
     * @throws AndesException
     */
    private void createRetainedEntry(AndesMessage message) throws AndesException {

        AndesMessageMetadata metadata = message.getMetadata();
        String destination = metadata.getDestination();
        long topicID   = destination.hashCode();
        long messageID = metadata.getMessageID();

        // create metadata entry
        Statement statement = QueryBuilder.insertInto(config.getKeyspace(),
                                                      CQLConstants.RETAINED_METADATA_TABLE).
                                                      ifNotExists().
                value(CQLConstants.TOPIC_NAME, destination).
                value(CQLConstants.MESSAGE_ID, messageID).
                value(CQLConstants.TOPIC_ID, topicID).
                value(CQLConstants.METADATA, ByteBuffer.wrap(metadata.getMetadata())).
                setConsistencyLevel(config.getWriteConsistencyLevel());

        execute(statement, "storing retain message metadata for topic " + destination);

        // create content entries
        for (AndesMessagePart messagePart : message.getContentChunkList()) {
            statement = QueryBuilder.insertInto(config.getKeyspace(),
                                                CQLConstants.RETAINED_CONTENT_TABLE).
                                                ifNotExists().
                    value(CQLConstants.MESSAGE_ID, messageID).
                    value(CQLConstants.MESSAGE_OFFSET, messagePart.getOffSet()).
                    value(CQLConstants.MESSAGE_CONTENT, ByteBuffer.wrap(messagePart.getData())).
                    setConsistencyLevel(config.getWriteConsistencyLevel());

            execute(statement, "storing retain message content offset " + messagePart.getOffSet() +
                               " for topic " + destination);

        }

    }

    /**
     * Get retain topic name list
     *
     * {@inheritDoc}
     */
    @Override
    public List<String> getAllRetainedTopics() throws AndesException {

        List<String> topicList = new ArrayList<>();
        ResultSet resultSet;

        Statement statement = QueryBuilder.select().column(CQLConstants.TOPIC_NAME).
                from(config.getKeyspace(), CQLConstants.RETAINED_METADATA_TABLE).
                setConsistencyLevel(config.getReadConsistencyLevel());

        resultSet = execute(statement, "retrieving retained topic name list.");

        for(Row result : resultSet.all()) {
            topicList.add(result.getString(CQLConstants.TOPIC_NAME));
        }

        return topicList;
    }

    /**
     * Get retain metadata for given topic destination
     *
     * {@inheritDoc}
     */
    @Override
    public AndesMessageMetadata getRetainedMetaData(String destination) throws AndesException {

        AndesMessageMetadata metadata = null;

        RetainedItemData retainedItemData = getRetainedTopicID(destination);

        Statement statement = QueryBuilder.select().column(CQLConstants.MESSAGE_ID).column(CQLConstants.METADATA).
                from(config.getKeyspace(), CQLConstants.RETAINED_METADATA_TABLE).
                where(eq(CQLConstants.TOPIC_ID, retainedItemData.topicID)).
                limit(1).
                allowFiltering().
                setConsistencyLevel(config.getReadConsistencyLevel());

        ResultSet results = execute(statement, "retrieving metadata for given destination " + destination);

        for (Row result : results.all()) {
            ByteBuffer buffer = result.getBytes(CQLConstants.METADATA);
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            long messageId = result.getLong(CQLConstants.MESSAGE_ID);
            metadata = new AndesMessageMetadata(messageId, bytes, true);
        }

        return metadata;


    }



    /**
     * Get retained topic ID for destination
     *
     * @param destination
     *         destination name
     * @return Retained item data
     */
    private RetainedItemData getRetainedTopicID(String destination)
            throws AndesException {

        RetainedItemData itemData = null;
        ResultSet results;

        Statement statement = QueryBuilder.select().column(CQLConstants.MESSAGE_ID).
                              column(CQLConstants.TOPIC_ID).from(config.getKeyspace(),
                                                            CQLConstants.RETAINED_METADATA_TABLE).
                                                            where(eq(CQLConstants.TOPIC_NAME, destination)).
                              setConsistencyLevel(config.getReadConsistencyLevel());

        results = execute(statement, "retrieving retained topic metadata for destination " + destination);

        for (Row result : results.all()) {

            long topicID = result.getLong(CQLConstants.TOPIC_ID);
            long messageID = result.getLong(CQLConstants.MESSAGE_ID);

            itemData = new RetainedItemData(topicID,messageID);

        }

        return itemData;
    }



    /**
     * Get retained content parts for given message id
     *
     * {@inheritDoc}
     */
    @Override
    public Map<Integer, AndesMessagePart> getRetainedContentParts(long messageID) throws AndesException {

        Map<Integer, AndesMessagePart> retainContentPartMap = new HashMap<Integer, AndesMessagePart>();
        ResultSet results;

        Statement statement =
                QueryBuilder.select().column(CQLConstants.MESSAGE_OFFSET).column(CQLConstants.MESSAGE_CONTENT).
                             from(config.getKeyspace(), CQLConstants.RETAINED_CONTENT_TABLE).
                             where(eq(CQLConstants.MESSAGE_ID, messageID)).
                setConsistencyLevel(config.getReadConsistencyLevel());

        results = execute(statement, "retrieving retained topic message content for message id " + messageID);

        for (Row result : results.all()) {
            ByteBuffer buffer = result.getBytes(CQLConstants.MESSAGE_CONTENT);
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);

            int offset = result.getInt(CQLConstants.MESSAGE_OFFSET);

            AndesMessagePart messagePart = new AndesMessagePart();

            messagePart.setMessageID(messageID);
            messagePart.setData(bytes);
            messagePart.setDataLength(bytes.length);
            messagePart.setOffSet(offset);
            retainContentPartMap.put(offset, messagePart);
        }




        return retainContentPartMap;
    }

    /**
     * CQL based message store transaction implementation
     */
    public class CQLBasedTransactionImpl implements AndesTransaction {

        /**
         * Messages to be committed is added to this message list
         */
        private final List<AndesMessage> messageList;

        /**
         * Following the commit in this class Andes state needs to be updated using
         * {@link org.wso2.andes.kernel.distruptor.inbound.StateEventHandler}. But if a commit fails at state event
         * handler we need to be able to rollback. For this purpose we need a list of last successful commit from
         * this class.
         */
        private final List<AndesMessage> rollbackList;

        /**
         * Creates a CQL based transaction object
         */
        CQLBasedTransactionImpl() {
            messageList = new ArrayList<AndesMessage>();
            rollbackList = new ArrayList<AndesMessage>();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void enqueue(AndesMessage message) throws AndesException {
            messageList.add(message);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void commit() throws AndesException {

            rollbackList.addAll(messageList);
            BatchStatement batchStatement = new BatchStatement();

            for(AndesMessage message: messageList) {
                addContentToBatch(batchStatement, message.getContentChunkList());
                addMetadataToBatch(batchStatement, message.getMetadata(), message.getMetadata().getStorageQueueName());
            }
            execute(batchStatement, "Storing metadata for transaction. Batch size" + messageList.size());
            messageList.clear();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void rollback() throws AndesException {
            messageList.clear();
            if (rollbackList.isEmpty()) {
                return;
            }

            List<AndesRemovableMetadata> removableMetadataList = new ArrayList<AndesRemovableMetadata>();
            List<Long> idList = new ArrayList<Long>();
            for (AndesMessage message : rollbackList) {
                AndesRemovableMetadata andesRemovableMetadata =
                        new AndesRemovableMetadata(message.getMetadata().getMessageID(),
                                message.getMetadata().getStorageQueueName(),
                                message.getMetadata().getStorageQueueName());

                removableMetadataList.add(andesRemovableMetadata);
                idList.add(message.getMetadata().getMessageID());
            }

            deleteMessageMetadataFromQueue(rollbackList.get(0).getMetadata().getStorageQueueName(), removableMetadataList);
            deleteMessageParts(idList);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() throws AndesException {
            messageList.clear();
            rollbackList.clear();
        }
    }


    /**
     * Used to store details about a retained item entry
     */
    private static class RetainedItemData {
        /**
         * Topic id of the DB entry
         */
        public long topicID;

        /**
         * Retained message ID for the topic
         */
        public long messageID;

        /**
         * Retained item entry
         *
         * @param topicID
         * @param messageID
         */
        private RetainedItemData(long topicID, long messageID) {
            this.topicID = topicID;
            this.messageID = messageID;
        }
    }

}
